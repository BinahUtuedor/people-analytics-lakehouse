"""
Bronze Raw-layer reader.

Responsibilities:

- discover Raw Parquet objects in Amazon S3;
- identify the most recent extraction batch for a table;
- construct S3A paths for Spark;
- read the selected Raw batch into a Spark DataFrame;
- preserve Raw source columns and partition metadata.

This module performs no Bronze transformations and writes no data.

Run a test from the project root:

    python -m spark.bronze.reader --table business_units
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from config.logger import logger
from config.settings import settings
from spark.utilities import (
    build_spark_session,
    stop_spark,
    validate_s3a_available,
)


class BronzeReadError(RuntimeError):
    """
    Raised when Raw data cannot be discovered or read.
    """


@dataclass(frozen=True)
class RawBatch:
    """
    Metadata describing one Raw extraction batch.

    Attributes:
        table_name:
            Source PostgreSQL table.

        bucket:
            S3 bucket containing the Raw data.

        batch_prefix:
            Common S3 key prefix containing the batch.

        parquet_keys:
            Parquet objects belonging to the batch.

        extraction_date:
            Partition extraction date, when available.

        batch_id:
            Shared batch / extraction identifier, when available.

        last_modified:
            Latest S3 modification timestamp among batch objects.
    """

    table_name: str
    bucket: str
    batch_prefix: str
    parquet_keys: tuple[str, ...]
    extraction_date: str | None
    batch_id: str | None
    last_modified: datetime

    @property
    def s3a_batch_path(
        self,
    ) -> str:
        """Return the S3A URI for the batch directory."""

        return (
            f"s3a://{self.bucket}/"
            f"{self.batch_prefix}"
        )

    @property
    def s3a_table_path(
        self,
    ) -> str:
        """
        Return the S3A table root.

        This is used as Spark's basePath so partition columns can be
        discovered from directory names.
        """

        raw_prefix = (
            settings.AWS_S3_RAW_PREFIX
            .strip("/")
        )

        return (
            f"s3a://{self.bucket}/"
            f"{raw_prefix}/"
            f"{self.table_name}"
        )


def get_s3_client():
    """
    Create the Boto3 S3 client.

    Credentials are resolved by Boto3 from the environment/configured
    AWS credential chain.
    """

    return boto3.client(
        "s3",
        region_name=settings.AWS_REGION,
    )


def normalise_prefix(
    prefix: str,
) -> str:
    """
    Normalise an S3 prefix without introducing a leading slash.
    """

    return prefix.strip("/")


def build_table_prefix(
    table_name: str,
) -> str:
    """
    Build the S3 Raw prefix for one source table.

    Example:

        raw/postgresql/employees/
    """

    if not table_name.strip():
        raise ValueError(
            "table_name must not be empty."
        )

    raw_prefix = normalise_prefix(
        settings.AWS_S3_RAW_PREFIX
    )

    return (
        f"{raw_prefix}/"
        f"{table_name.strip()}/"
    )


def list_raw_parquet_objects(
    table_name: str,
) -> list[dict]:
    """
    List all Raw Parquet objects for a PostgreSQL source table.

    Uses pagination so discovery continues to work when a table prefix
    contains more than 1,000 S3 objects.
    """

    if not settings.AWS_S3_BUCKET:
        raise BronzeReadError(
            "AWS_S3_BUCKET is not configured."
        )

    table_prefix = build_table_prefix(
        table_name
    )

    logger.info(
        "Discovering Raw S3 objects | "
        f"table={table_name} | "
        f"prefix={table_prefix}"
    )

    client = get_s3_client()

    paginator = client.get_paginator(
        "list_objects_v2"
    )

    objects: list[dict] = []

    try:
        pages = paginator.paginate(
            Bucket=settings.AWS_S3_BUCKET,
            Prefix=table_prefix,
        )

        for page in pages:
            for item in page.get(
                "Contents",
                [],
            ):
                key = item[
                    "Key"
                ]

                if key.lower().endswith(
                    ".parquet"
                ):
                    objects.append(
                        item
                    )

    except (
        BotoCoreError,
        ClientError,
    ) as error:
        raise BronzeReadError(
            "Unable to list Raw S3 objects for "
            f"table '{table_name}'."
        ) from error

    if not objects:
        raise BronzeReadError(
            "No Raw Parquet objects found for "
            f"table '{table_name}' under "
            f"s3://{settings.AWS_S3_BUCKET}/"
            f"{table_prefix}"
        )

    logger.info(
        "Raw Parquet objects discovered | "
        f"table={table_name} | "
        f"files={len(objects):,}"
    )

    return objects


def parse_partition_metadata(
    key: str,
) -> tuple[
    str | None,
    str | None,
]:
    """
    Extract extraction date and batch ID from an S3 object key.

    Supported path components include:

        extraction_date=2026-07-27
        batch_id=<uuid>

    and the earlier/raw-compatible form:

        extraction_id=<uuid>

    Supporting both forms keeps the Bronze reader backwards-compatible
    with Raw data generated before the shared batch-ID migration.
    """

    extraction_date: str | None = None
    batch_id: str | None = None

    path = PurePosixPath(
        key
    )

    for component in path.parts:

        if component.startswith(
            "extraction_date="
        ):
            extraction_date = (
                component.split(
                    "=",
                    1,
                )[1]
            )

        elif component.startswith(
            "batch_id="
        ):
            batch_id = (
                component.split(
                    "=",
                    1,
                )[1]
            )

        elif component.startswith(
            "extraction_id="
        ):
            batch_id = (
                component.split(
                    "=",
                    1,
                )[1]
            )

    return (
        extraction_date,
        batch_id,
    )


def get_batch_prefix(
    parquet_key: str,
) -> str:
    """
    Return the parent prefix containing a Parquet file.
    """

    parent = (
        PurePosixPath(
            parquet_key
        )
        .parent
    )

    return (
        f"{parent.as_posix()}/"
    )


def discover_latest_raw_batch(
    table_name: str,
) -> RawBatch:
    """
    Discover the most recently modified Raw extraction batch.

    S3 LastModified is used to determine recency instead of assuming
    that lexical ordering of extraction IDs reflects processing order.
    """

    objects = list_raw_parquet_objects(
        table_name
    )

    grouped: dict[
        str,
        list[dict],
    ] = {}

    for item in objects:
        batch_prefix = (
            get_batch_prefix(
                item["Key"]
            )
        )

        grouped.setdefault(
            batch_prefix,
            [],
        ).append(
            item
        )

    batch_candidates: list[
        RawBatch
    ] = []

    for (
        batch_prefix,
        batch_objects,
    ) in grouped.items():

        latest_modified = max(
            item["LastModified"]
            for item in batch_objects
        )

        sample_key = (
            batch_objects[0][
                "Key"
            ]
        )

        (
            extraction_date,
            batch_id,
        ) = parse_partition_metadata(
            sample_key
        )

        parquet_keys = tuple(
            sorted(
                item["Key"]
                for item
                in batch_objects
            )
        )

        batch_candidates.append(
            RawBatch(
                table_name=table_name,
                bucket=(
                    settings
                    .AWS_S3_BUCKET
                ),
                batch_prefix=(
                    batch_prefix
                ),
                parquet_keys=(
                    parquet_keys
                ),
                extraction_date=(
                    extraction_date
                ),
                batch_id=batch_id,
                last_modified=(
                    latest_modified
                ),
            )
        )

    latest_batch = max(
        batch_candidates,
        key=lambda batch: (
            batch.last_modified
        ),
    )

    logger.info(
        "Latest Raw batch discovered | "
        f"table={table_name} | "
        f"extraction_date="
        f"{latest_batch.extraction_date} | "
        f"batch_id={latest_batch.batch_id} | "
        f"files="
        f"{len(latest_batch.parquet_keys):,}"
    )

    return latest_batch


def read_raw_batch(
    spark: SparkSession,
    batch: RawBatch,
) -> DataFrame:
    """
    Read one Raw batch into a Spark DataFrame.

    Spark's basePath is set to the table root so partition information
    encoded in paths remains available when applicable.
    """

    logger.info(
        "Reading Raw Parquet batch with Spark | "
        f"table={batch.table_name} | "
        f"path={batch.s3a_batch_path}"
    )

    try:
        dataframe = (
            spark.read
            .option(
                "basePath",
                batch.s3a_table_path,
            )
            .parquet(
                batch.s3a_batch_path
            )
        )

    except Exception as error:
        raise BronzeReadError(
            "Spark failed to read Raw Parquet data for "
            f"table '{batch.table_name}' from "
            f"{batch.s3a_batch_path}."
        ) from error

    logger.info(
        "Raw batch loaded into Spark | "
        f"table={batch.table_name} | "
        f"columns={len(dataframe.columns)}"
    )

    return dataframe


def read_latest_raw_table(
    spark: SparkSession,
    table_name: str,
) -> tuple[
    DataFrame,
    RawBatch,
]:
    """
    Discover and read the latest Raw extraction for a table.

    Returns:
        Tuple containing:
        - Spark DataFrame
        - RawBatch metadata
    """

    batch = (
        discover_latest_raw_batch(
            table_name
        )
    )

    dataframe = read_raw_batch(
        spark=spark,
        batch=batch,
    )

    return (
        dataframe,
        batch,
    )


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for standalone reader testing.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Test reading a Raw S3 Parquet "
            "batch using Spark."
        )
    )

    parser.add_argument(
        "--table",
        required=True,
        help=(
            "PostgreSQL source table to read, "
            "for example business_units."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """
    Standalone Bronze-reader test entry point.
    """

    args = parse_args()

    spark: SparkSession | None = None

    try:
        spark = build_spark_session(
            app_name=(
                "people-analytics-"
                "bronze-reader-test"
            )
        )

        validate_s3a_available(
            spark
        )

        dataframe, batch = (
            read_latest_raw_table(
                spark=spark,
                table_name=args.table,
            )
        )

        row_count = (
            dataframe.count()
        )

        logger.info(
            "Bronze reader test successful | "
            f"table={args.table} | "
            f"rows={row_count:,} | "
            f"batch_id={batch.batch_id}"
        )

        dataframe.printSchema()

        dataframe.show(
            10,
            truncate=False,
        )

    finally:
        stop_spark(
            spark
        )


if __name__ == "__main__":
    main()