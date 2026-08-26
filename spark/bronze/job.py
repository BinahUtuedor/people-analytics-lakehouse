"""Portable spark-submit entry point for Raw-to-Bronze processing."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession

from config.datasets import SUPPORTED_DATASETS
from config.logger import logger
from config.settings import settings
from spark.bronze.reader import read_raw_table
from spark.bronze.transform import transform_to_bronze
from spark.bronze.validate import BronzeValidationResult, validate_bronze
from spark.bronze.writer import (
    bronze_output_exists,
    build_bronze_output_path,
    write_bronze,
)
from spark.utilities import build_spark_session, stop_spark, validate_s3a_available


@dataclass(frozen=True)
class BronzeJobResult:
    """Summary of one successfully published Bronze batch."""

    table_name: str
    batch_id: str
    input_path: str
    output_path: str
    raw_count: int
    bronze_count: int
    publication_status: str = "published"


def run_bronze_job(
    spark: SparkSession,
    *,
    table_name: str,
    batch_id: str,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    bronze_prefix: str | None = None,
    verify_existing: bool = False,
) -> BronzeJobResult:
    """Read, transform, validate, reconcile, and publish one Raw batch."""

    raw_dataframe, batch = read_raw_table(
        spark,
        table_name,
        batch_id,
        bucket=bucket,
        raw_prefix=raw_prefix,
    )
    output_path = build_bronze_output_path(
        batch,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
    )
    if verify_existing and bronze_output_exists(spark, output_path):
        logger.info(
            "Verifying existing Bronze batch | "
            f"table={table_name} | batch_id={batch_id} | "
            f"output={output_path}"
        )
        bronze_dataframe = spark.read.parquet(output_path)
        validation = validate_bronze(
            raw_dataframe,
            bronze_dataframe,
            batch,
        )
        return BronzeJobResult(
            table_name=table_name,
            batch_id=batch_id,
            input_path=batch.s3a_batch_path,
            output_path=output_path,
            raw_count=validation.raw_count,
            bronze_count=validation.bronze_count,
            publication_status="existing_verified",
        )

    bronze_dataframe = transform_to_bronze(raw_dataframe, batch)
    validation: BronzeValidationResult = validate_bronze(
        raw_dataframe,
        bronze_dataframe,
        batch,
    )

    logger.info(
        "Bronze validation passed | "
        f"table={table_name} | batch_id={batch_id} | "
        f"input={batch.s3a_batch_path} | output={output_path} | "
        f"raw_count={validation.raw_count:,} | "
        f"bronze_count={validation.bronze_count:,}"
    )
    write_bronze(bronze_dataframe, output_path)

    return BronzeJobResult(
        table_name=table_name,
        batch_id=batch_id,
        input_path=batch.s3a_batch_path,
        output_path=output_path,
        raw_count=validation.raw_count,
        bronze_count=validation.bronze_count,
    )


def run_bronze_batch(
    spark: SparkSession,
    *,
    table_names: tuple[str, ...],
    batch_id: str,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    bronze_prefix: str | None = None,
    verify_existing: bool = False,
) -> tuple[BronzeJobResult, ...]:
    """Process an explicit shared Raw batch for each supported dataset."""

    results: list[BronzeJobResult] = []
    for table_name in table_names:
        logger.info(
            "Starting Bronze dataset | "
            f"table={table_name} | batch_id={batch_id}"
        )
        results.append(
            run_bronze_job(
                spark,
                table_name=table_name,
                batch_id=batch_id,
                bucket=bucket,
                raw_prefix=raw_prefix,
                bronze_prefix=bronze_prefix,
                verify_existing=verify_existing,
            )
        )
    return tuple(results)


def parse_args() -> argparse.Namespace:
    """Parse portable Bronze job arguments."""

    parser = argparse.ArgumentParser(
        description="Process a validated Amazon S3 Raw batch to Bronze."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--table")
    selection.add_argument(
        "--all-tables",
        action="store_true",
        help="Process every dataset in the shared supported-data registry.",
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bucket")
    parser.add_argument("--raw-prefix")
    parser.add_argument("--bronze-prefix")
    parser.add_argument(
        "--verify-existing",
        action="store_true",
        help=(
            "Revalidate an existing Bronze batch against Raw and continue; "
            "without this flag, existing output fails publication."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Run the Bronze job and stop Spark on success or failure."""

    args = parse_args()
    spark: SparkSession | None = None
    try:
        job_scope = "all-tables" if args.all_tables else args.table
        spark = build_spark_session(
            app_name=f"{settings.SPARK_APP_NAME}-bronze-{job_scope}"
        )
        validate_s3a_available(spark)
        table_names = SUPPORTED_DATASETS if args.all_tables else (args.table,)
        results = run_bronze_batch(
            spark,
            table_names=table_names,
            batch_id=args.batch_id,
            bucket=args.bucket,
            raw_prefix=args.raw_prefix,
            bronze_prefix=args.bronze_prefix,
            verify_existing=args.verify_existing,
        )
        published = sum(
            result.publication_status == "published" for result in results
        )
        verified = len(results) - published
        logger.info(
            "Bronze job completed | "
            f"batch_id={args.batch_id} | datasets={len(results)} | "
            f"published={published} | existing_verified={verified} | "
            f"rows={sum(result.bronze_count for result in results):,}"
        )
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
