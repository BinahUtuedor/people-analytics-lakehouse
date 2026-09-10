"""Portable Spark entry point for Bronze-to-Silver processing."""

from __future__ import annotations
import argparse
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from config.datasets import SUPPORTED_DATASETS
from config.logger import logger
from config.settings import settings
from spark.silver.reader import read_bronze_table
from spark.silver.transform import transform_to_silver
from spark.silver.validate import (
    SilverValidationResult,
    validate_employee_references,
    validate_silver,
)
from spark.silver.writer import (
    build_silver_output_path,
    silver_output_exists,
    write_silver,
)
from spark.utilities import build_spark_session, stop_spark, validate_s3a_available


@dataclass(frozen=True)
class SilverJobResult:
    table_name: str
    batch_id: str
    input_path: str
    output_path: str
    bronze_count: int
    silver_count: int
    publication_status: str


def run_silver_job(
    spark: SparkSession,
    *,
    table_name: str,
    batch_id: str,
    bucket: str | None = None,
    bronze_prefix: str | None = None,
    silver_prefix: str | None = None,
    verify_existing: bool = False,
    employees: DataFrame | None = None,
) -> SilverJobResult:
    bronze, batch = read_bronze_table(
        spark, table_name, batch_id, bucket=bucket, bronze_prefix=bronze_prefix
    )
    output = build_silver_output_path(batch, bucket=bucket, silver_prefix=silver_prefix)
    if verify_existing and silver_output_exists(spark, output):
        silver = spark.read.parquet(output)
        status = "existing_verified"
    else:
        if silver_output_exists(spark, output):
            raise RuntimeError(
                f"Silver output already exists: {output}. Use --verify-existing."
            )
        silver = transform_to_silver(bronze, batch)
        status = "published"
    result: SilverValidationResult = validate_silver(
        bronze, silver, table_name, batch_id
    )
    if employees is not None:
        validate_employee_references(silver, employees, table_name)
    if status == "published":
        write_silver(silver, output)
    return SilverJobResult(
        table_name,
        batch_id,
        batch.s3a_batch_path,
        output,
        result.bronze_count,
        result.silver_count,
        status,
    )


def run_silver_batch(
    spark: SparkSession, *, table_names: tuple[str, ...], batch_id: str, **options
) -> tuple[SilverJobResult, ...]:
    results: list[SilverJobResult] = []
    employees = None
    for table_name in table_names:
        result = run_silver_job(
            spark,
            table_name=table_name,
            batch_id=batch_id,
            employees=employees,
            **options,
        )
        results.append(result)
        if table_name == "employees":
            employees, _ = read_bronze_table(
                spark,
                "employees",
                batch_id,
                bucket=options.get("bucket"),
                bronze_prefix=options.get("bronze_prefix"),
            )
            employees = transform_to_silver(employees, _)
    return tuple(results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process a validated Bronze batch to Silver."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--table")
    selection.add_argument("--all-tables", action="store_true")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bucket")
    parser.add_argument("--bronze-prefix")
    parser.add_argument("--silver-prefix")
    parser.add_argument("--verify-existing", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark: SparkSession | None = None
    try:
        spark = build_spark_session(
            app_name=f"{settings.SPARK_APP_NAME}-silver-{'all-tables' if args.all_tables else args.table}"
        )
        validate_s3a_available(spark)
        tables = SUPPORTED_DATASETS if args.all_tables else (args.table,)
        results = run_silver_batch(
            spark,
            table_names=tables,
            batch_id=args.batch_id,
            bucket=args.bucket,
            bronze_prefix=args.bronze_prefix,
            silver_prefix=args.silver_prefix,
            verify_existing=args.verify_existing,
        )
        logger.info(
            f"Silver job completed | batch_id={args.batch_id} | datasets={len(results)} | rows={sum(item.silver_count for item in results):,}"
        )
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
