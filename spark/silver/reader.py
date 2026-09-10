"""Resolve and read exact Bronze partitions for Silver processing."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import DataFrame, SparkSession
from config.settings import settings
from spark.bronze.reader import normalise_prefix, parse_partition_metadata


class SilverReadError(RuntimeError):
    """Raised when an exact Bronze input cannot be discovered or read."""


@dataclass(frozen=True)
class BronzeBatch:
    table_name: str
    bucket: str
    bronze_prefix: str
    batch_prefix: str
    extraction_date: str
    batch_id: str
    source_system: str = "postgresql"

    @property
    def s3a_batch_path(self) -> str:
        return f"s3a://{self.bucket}/{self.batch_prefix}"


def _bucket(bucket: str | None) -> str:
    value = bucket or settings.AWS_S3_BUCKET
    if not value:
        raise SilverReadError("AWS_S3_BUCKET is not configured.")
    return value


def _prefix(prefix: str | None) -> str:
    value = normalise_prefix(prefix or settings.AWS_S3_BRONZE_PREFIX)
    if not value:
        raise SilverReadError("The Bronze S3 prefix must not be empty.")
    return value


def build_bronze_table_prefix(table_name: str, bronze_prefix: str | None = None) -> str:
    if not table_name.strip():
        raise ValueError("table_name must not be empty.")
    return f"{_prefix(bronze_prefix)}/postgresql/{table_name.strip()}/"


def discover_bronze_batch(
    table_name: str,
    batch_id: str,
    *,
    bucket: str | None = None,
    bronze_prefix: str | None = None,
    s3_client: Any | None = None,
) -> BronzeBatch:
    """Discover exactly one partition before reading; never infer a latest batch."""
    if not batch_id.strip():
        raise ValueError("batch_id must not be empty.")
    resolved_bucket, resolved_prefix = _bucket(bucket), _prefix(bronze_prefix)
    client = s3_client or boto3.client("s3", region_name=settings.AWS_REGION)
    try:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=resolved_bucket,
            Prefix=build_bronze_table_prefix(table_name, resolved_prefix),
        )
        keys = [
            item["Key"]
            for page in pages
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet")
        ]
    except (BotoCoreError, ClientError) as error:
        raise SilverReadError(
            f"Unable to list Bronze objects for '{table_name}'."
        ) from error
    candidates = {
        (date, key.rsplit("/", 1)[0] + "/")
        for key in keys
        for date, candidate, _ in [parse_partition_metadata(key)]
        if candidate == batch_id and date
    }
    if not candidates:
        raise SilverReadError(
            f"Bronze batch '{batch_id}' was not found for table '{table_name}'."
        )
    if len(candidates) != 1:
        raise SilverReadError(
            f"Bronze batch '{batch_id}' is ambiguous for table '{table_name}'."
        )
    extraction_date, batch_prefix = candidates.pop()
    return BronzeBatch(
        table_name,
        resolved_bucket,
        resolved_prefix,
        batch_prefix,
        extraction_date,
        batch_id,
    )


def read_bronze_table(
    spark: SparkSession, table_name: str, batch_id: str, **options: Any
) -> tuple[DataFrame, BronzeBatch]:
    batch = discover_bronze_batch(table_name, batch_id, **options)
    try:
        return spark.read.parquet(batch.s3a_batch_path), batch
    except Exception as error:
        raise SilverReadError(
            f"Spark failed to read Bronze batch at {batch.s3a_batch_path}."
        ) from error
