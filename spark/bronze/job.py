"""Portable spark-submit entry point for Raw-to-Bronze processing."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession

from config.logger import logger
from config.settings import settings
from spark.bronze.reader import read_raw_table
from spark.bronze.transform import transform_to_bronze
from spark.bronze.validate import BronzeValidationResult, validate_bronze
from spark.bronze.writer import build_bronze_output_path, write_bronze
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


def run_bronze_job(
    spark: SparkSession,
    *,
    table_name: str,
    batch_id: str,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    bronze_prefix: str | None = None,
) -> BronzeJobResult:
    """Read, transform, validate, reconcile, and publish one Raw batch."""

    raw_dataframe, batch = read_raw_table(
        spark,
        table_name,
        batch_id,
        bucket=bucket,
        raw_prefix=raw_prefix,
    )
    bronze_dataframe = transform_to_bronze(raw_dataframe, batch)
    validation: BronzeValidationResult = validate_bronze(
        raw_dataframe,
        bronze_dataframe,
        batch,
    )
    output_path = build_bronze_output_path(
        batch,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
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


def parse_args() -> argparse.Namespace:
    """Parse portable Bronze job arguments."""

    parser = argparse.ArgumentParser(
        description="Process one validated Amazon S3 Raw batch to Bronze."
    )
    parser.add_argument("--table", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bucket")
    parser.add_argument("--raw-prefix")
    parser.add_argument("--bronze-prefix")
    return parser.parse_args()


def main() -> None:
    """Run the Bronze job and stop Spark on success or failure."""

    args = parse_args()
    spark: SparkSession | None = None
    try:
        spark = build_spark_session(
            app_name=f"{settings.SPARK_APP_NAME}-bronze-{args.table}"
        )
        validate_s3a_available(spark)
        result = run_bronze_job(
            spark,
            table_name=args.table,
            batch_id=args.batch_id,
            bucket=args.bucket,
            raw_prefix=args.raw_prefix,
            bronze_prefix=args.bronze_prefix,
        )
        logger.info(
            "Bronze job completed | "
            f"table={result.table_name} | batch_id={result.batch_id} | "
            f"rows={result.bronze_count:,} | output={result.output_path}"
        )
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
