"""Discover and read validated Raw Parquet batches from Amazon S3.

The reader owns physical file lineage. It adds ``_source_file`` with Spark's
``input_file_name()`` while the DataFrame still refers to the Raw files.
Transformation and publication remain separate Bronze responsibilities.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.logger import logger
from config.settings import settings
from spark.utilities import build_spark_session, stop_spark, validate_s3a_available


SOURCE_SYSTEM = "postgresql"
SOURCE_SCHEMA = "public"


class BronzeReadError(RuntimeError):
    """Raised when a Raw batch cannot be resolved or read safely."""


@dataclass(frozen=True)
class RawBatch:
    """Resolved identity and location of one Raw table extraction."""

    table_name: str
    bucket: str
    raw_prefix: str
    batch_prefix: str
    parquet_keys: tuple[str, ...]
    extraction_date: str
    batch_id: str
    extraction_id: str
    last_modified: datetime
    source_system: str = SOURCE_SYSTEM
    source_schema: str = SOURCE_SCHEMA

    @property
    def s3a_batch_path(self) -> str:
        """Return the S3A URI of the extraction directory."""

        return f"s3a://{self.bucket}/{self.batch_prefix}"

    @property
    def s3a_table_path(self) -> str:
        """Return the S3A URI of the Raw table root."""

        return f"s3a://{self.bucket}/{self.raw_prefix}/{self.table_name}"


@dataclass(frozen=True)
class ParquetCompatibility:
    """Spark read contract for Parquet logical types it cannot infer."""

    schema: StructType | None = None
    nanos_timestamp_columns: tuple[str, ...] = ()
    time_columns: tuple[str, ...] = ()


def normalise_prefix(prefix: str) -> str:
    """Normalise an S3 prefix without introducing a leading slash."""

    return str(prefix).strip("/")


def _resolve_bucket(bucket: str | None) -> str:
    resolved = bucket or settings.AWS_S3_BUCKET
    if not resolved:
        raise BronzeReadError("AWS_S3_BUCKET is not configured.")
    return resolved


def _resolve_raw_prefix(raw_prefix: str | None) -> str:
    resolved = normalise_prefix(raw_prefix or settings.AWS_S3_RAW_PREFIX)
    if not resolved:
        raise BronzeReadError("The Raw S3 prefix must not be empty.")
    return resolved


def build_table_prefix(
    table_name: str,
    raw_prefix: str | None = None,
) -> str:
    """Build the S3 Raw prefix for one source table."""

    table = table_name.strip()
    if not table:
        raise ValueError("table_name must not be empty.")
    return f"{_resolve_raw_prefix(raw_prefix)}/{table}/"


def get_s3_client(region: str | None = None):
    """Create an S3 client using the standard AWS credential chain."""

    return boto3.client("s3", region_name=region or settings.AWS_REGION)


def list_raw_parquet_objects(
    table_name: str,
    *,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    s3_client: Any | None = None,
) -> list[dict[str, Any]]:
    """List all Raw Parquet objects for a table using S3 pagination."""

    resolved_bucket = _resolve_bucket(bucket)
    table_prefix = build_table_prefix(table_name, raw_prefix)
    client = s3_client or get_s3_client()
    logger.info(
        "Discovering Raw S3 objects | "
        f"table={table_name} | prefix={table_prefix}"
    )

    objects: list[dict[str, Any]] = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=resolved_bucket,
            Prefix=table_prefix,
        )
        for page in pages:
            for item in page.get("Contents", []):
                if item["Key"].lower().endswith(".parquet"):
                    objects.append(item)
    except (BotoCoreError, ClientError) as error:
        raise BronzeReadError(
            f"Unable to list Raw S3 objects for table '{table_name}'."
        ) from error

    if not objects:
        raise BronzeReadError(
            "No Raw Parquet objects found for "
            f"table '{table_name}' under "
            f"s3://{resolved_bucket}/{table_prefix}"
        )
    return objects


def parse_partition_metadata(
    key: str,
) -> tuple[str | None, str | None, str | None]:
    """Return extraction date, shared batch ID, and extraction ID."""

    extraction_date: str | None = None
    batch_id: str | None = None
    extraction_id: str | None = None
    for component in PurePosixPath(key).parts:
        if component.startswith("extraction_date="):
            extraction_date = component.split("=", 1)[1]
        elif component.startswith("batch_id="):
            batch_id = component.split("=", 1)[1]
        elif component.startswith("extraction_id="):
            extraction_id = component.split("=", 1)[1]
    return extraction_date, batch_id, extraction_id


def get_batch_prefix(parquet_key: str) -> str:
    """Return the extraction directory containing a Parquet object."""

    return f"{PurePosixPath(parquet_key).parent.as_posix()}/"


def _build_batch_candidates(
    table_name: str,
    objects: list[dict[str, Any]],
    *,
    bucket: str,
    raw_prefix: str,
) -> list[RawBatch]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    expected_table_prefix = f"{raw_prefix}/{table_name}/"
    for item in objects:
        key = item.get("Key")
        last_modified = item.get("LastModified")
        if not isinstance(key, str) or not isinstance(last_modified, datetime):
            raise BronzeReadError("Raw S3 object metadata is incomplete.")
        if not key.startswith(expected_table_prefix):
            raise BronzeReadError(
                "Raw object key is outside the requested table prefix: "
                f"{key}"
            )

        extraction_date, batch_id, extraction_id = parse_partition_metadata(key)
        if not extraction_date or not batch_id or not extraction_id:
            raise BronzeReadError(
                f"Raw object key has incomplete partition metadata: {key}"
            )
        identity = (
            extraction_date,
            batch_id,
            extraction_id,
            get_batch_prefix(key),
        )
        grouped.setdefault(identity, []).append(item)

    candidates: list[RawBatch] = []
    for identity, batch_objects in grouped.items():
        extraction_date, batch_id, extraction_id, batch_prefix = identity
        candidates.append(
            RawBatch(
                table_name=table_name,
                bucket=bucket,
                raw_prefix=raw_prefix,
                batch_prefix=batch_prefix,
                parquet_keys=tuple(
                    sorted(item["Key"] for item in batch_objects)
                ),
                extraction_date=extraction_date,
                batch_id=batch_id,
                extraction_id=extraction_id,
                last_modified=max(
                    item["LastModified"] for item in batch_objects
                ),
            )
        )
    return candidates


def discover_raw_batch(
    table_name: str,
    batch_id: str,
    *,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    objects: list[dict[str, Any]] | None = None,
    s3_client: Any | None = None,
) -> RawBatch:
    """Resolve exactly one extraction for a requested shared Raw batch."""

    requested_batch = batch_id.strip()
    if not requested_batch:
        raise ValueError("batch_id must not be empty.")
    resolved_bucket = _resolve_bucket(bucket)
    resolved_prefix = _resolve_raw_prefix(raw_prefix)
    raw_objects = objects
    if raw_objects is None:
        raw_objects = list_raw_parquet_objects(
            table_name,
            bucket=resolved_bucket,
            raw_prefix=resolved_prefix,
            s3_client=s3_client,
        )
    candidates = [
        candidate
        for candidate in _build_batch_candidates(
            table_name,
            raw_objects,
            bucket=resolved_bucket,
            raw_prefix=resolved_prefix,
        )
        if candidate.batch_id == requested_batch
    ]
    if not candidates:
        raise BronzeReadError(
            f"Raw batch '{requested_batch}' was not found for table "
            f"'{table_name}'."
        )
    if len(candidates) > 1:
        extraction_ids = sorted(
            candidate.extraction_id for candidate in candidates
        )
        raise BronzeReadError(
            f"Raw batch '{requested_batch}' is ambiguous for table "
            f"'{table_name}'; extraction IDs: {extraction_ids}"
        )
    return candidates[0]


def discover_latest_raw_batch(
    table_name: str,
    *,
    bucket: str | None = None,
    raw_prefix: str | None = None,
    objects: list[dict[str, Any]] | None = None,
    s3_client: Any | None = None,
) -> RawBatch:
    """Resolve the most recently modified extraction for compatibility."""

    resolved_bucket = _resolve_bucket(bucket)
    resolved_prefix = _resolve_raw_prefix(raw_prefix)
    raw_objects = objects
    if raw_objects is None:
        raw_objects = list_raw_parquet_objects(
            table_name,
            bucket=resolved_bucket,
            raw_prefix=resolved_prefix,
            s3_client=s3_client,
        )
    candidates = _build_batch_candidates(
        table_name,
        raw_objects,
        bucket=resolved_bucket,
        raw_prefix=resolved_prefix,
    )
    if not candidates:
        raise BronzeReadError(f"No Raw batches were found for '{table_name}'.")
    return max(candidates, key=lambda candidate: candidate.last_modified)


def read_parquet_with_lineage(
    spark: SparkSession,
    input_path: str,
    compatibility: ParquetCompatibility | None = None,
) -> DataFrame:
    """Read Parquet and capture physical file provenance immediately."""

    resolved = compatibility or ParquetCompatibility()
    if resolved.nanos_timestamp_columns:
        # Spark 4 rejects Parquet TIMESTAMP(NANOS) during inference. Reading
        # those physical values as longs and converting only footer-confirmed
        # columns retains their timestamp meaning at Spark's microsecond
        # precision without changing immutable Raw objects.
        spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

    reader = spark.read
    if resolved.schema is not None:
        reader = reader.schema(resolved.schema)
    dataframe = (
        reader.parquet(input_path)
        .withColumn("_source_file", input_file_name())
    )
    for column_name in resolved.nanos_timestamp_columns:
        escaped_column = column_name.replace("`", "``")
        dataframe = dataframe.withColumn(
            column_name,
            F.timestamp_micros(
                F.expr(f"`{escaped_column}` DIV 1000")
            ),
        )
    for column_name in resolved.time_columns:
        escaped_column = column_name.replace("`", "``")
        dataframe = dataframe.withColumn(
            column_name,
            F.date_format(
                F.timestamp_micros(F.expr(f"`{escaped_column}`")),
                "HH:mm:ss.SSSSSS",
            ),
        )
    return dataframe


def _spark_field_from_parquet(field) -> StructField:
    """Map a flat Parquet primitive field to its Spark read type."""

    primitive = field.asPrimitiveType()
    physical_type = str(primitive.getPrimitiveTypeName()).upper()
    logical = primitive.getLogicalTypeAnnotation()
    logical_name = str(logical).upper() if logical is not None else ""
    nullable = str(field.getRepetition()).upper() != "REQUIRED"

    if "STRING" in logical_name or "ENUM" in logical_name:
        data_type = StringType()
    elif "TIMESTAMP" in logical_name:
        data_type = LongType() if "NANOS" in logical_name else TimestampType()
    elif "TIME" in logical_name:
        data_type = LongType()
    elif "DATE" in logical_name:
        data_type = DateType()
    elif physical_type == "BOOLEAN":
        data_type = BooleanType()
    elif physical_type == "INT32":
        data_type = IntegerType()
    elif physical_type == "INT64":
        data_type = LongType()
    elif physical_type == "FLOAT":
        data_type = FloatType()
    elif physical_type == "DOUBLE":
        data_type = DoubleType()
    elif physical_type in {"BINARY", "FIXED_LEN_BYTE_ARRAY"}:
        data_type = BinaryType()
    elif physical_type == "INT96":
        data_type = TimestampType()
    else:
        raise BronzeReadError(
            f"Unsupported Parquet physical type for '{field.getName()}': "
            f"{physical_type}."
        )
    return StructField(field.getName(), data_type, nullable)


def get_parquet_compatibility(
    spark: SparkSession,
    batch: RawBatch,
) -> ParquetCompatibility:
    """Inspect Raw footers for unsupported Spark temporal logical types."""

    hadoop_configuration = (
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    jvm = spark.sparkContext._jvm
    schemas: list[tuple[tuple[str, str, str], ...]] = []
    first_fields = None
    try:
        for key in batch.parquet_keys:
            path = jvm.org.apache.hadoop.fs.Path(
                f"s3a://{batch.bucket}/{key}"
            )
            input_file = (
                jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    path,
                    hadoop_configuration,
                )
            )
            parquet_reader = (
                jvm.org.apache.parquet.hadoop.ParquetFileReader.open(input_file)
            )
            try:
                fields = (
                    parquet_reader.getFooter()
                    .getFileMetaData()
                    .getSchema()
                    .getFields()
                )
                if first_fields is None:
                    first_fields = list(fields)
                schemas.append(
                    tuple(
                        (
                            field.getName(),
                            str(field.asPrimitiveType().getPrimitiveTypeName()),
                            str(field.getLogicalTypeAnnotation()),
                        )
                        for field in fields
                    )
                )
            finally:
                parquet_reader.close()
    except Exception as error:
        raise BronzeReadError(
            "Unable to inspect Raw Parquet timestamp metadata for "
            f"table '{batch.table_name}'."
        ) from error

    if schemas and any(schema != schemas[0] for schema in schemas[1:]):
        raise BronzeReadError(
            "Raw Parquet parts disagree on their physical schema for "
            f"table '{batch.table_name}'."
        )

    fields = first_fields or []
    nanos_columns = tuple(
        field.getName()
        for field in fields
        if "TIMESTAMP" in str(field.getLogicalTypeAnnotation()).upper()
        and "NANOS" in str(field.getLogicalTypeAnnotation()).upper()
    )
    time_columns = tuple(
        field.getName()
        for field in fields
        if "TIME" in str(field.getLogicalTypeAnnotation()).upper()
        and "TIMESTAMP" not in str(field.getLogicalTypeAnnotation()).upper()
    )
    schema = (
        StructType([_spark_field_from_parquet(field) for field in fields])
        if time_columns
        else None
    )
    return ParquetCompatibility(
        schema=schema,
        nanos_timestamp_columns=tuple(sorted(nanos_columns)),
        time_columns=tuple(sorted(time_columns)),
    )


def read_raw_batch(spark: SparkSession, batch: RawBatch) -> DataFrame:
    """Read one Raw extraction and capture physical source-file lineage."""

    logger.info(
        "Reading Raw Parquet batch with Spark | "
        f"table={batch.table_name} | path={batch.s3a_batch_path}"
    )
    try:
        compatibility = get_parquet_compatibility(spark, batch)
        if compatibility.nanos_timestamp_columns:
            logger.info(
                "Applying Raw TIMESTAMP(NANOS) compatibility | "
                f"table={batch.table_name} | "
                f"columns={list(compatibility.nanos_timestamp_columns)}"
            )
        if compatibility.time_columns:
            logger.info(
                "Applying Raw TIME compatibility | "
                f"table={batch.table_name} | "
                f"columns={list(compatibility.time_columns)}"
            )
        return read_parquet_with_lineage(
            spark,
            batch.s3a_batch_path,
            compatibility,
        )
    except Exception as error:
        raise BronzeReadError(
            "Spark failed to read Raw Parquet data for "
            f"table '{batch.table_name}' from {batch.s3a_batch_path}."
        ) from error


def read_raw_table(
    spark: SparkSession,
    table_name: str,
    batch_id: str,
    **discovery_options: Any,
) -> tuple[DataFrame, RawBatch]:
    """Resolve and read a requested shared Raw batch."""

    batch = discover_raw_batch(
        table_name,
        batch_id,
        **discovery_options,
    )
    return read_raw_batch(spark, batch), batch


def read_latest_raw_table(
    spark: SparkSession,
    table_name: str,
    **discovery_options: Any,
) -> tuple[DataFrame, RawBatch]:
    """Resolve and read the latest Raw extraction for compatibility."""

    batch = discover_latest_raw_batch(table_name, **discovery_options)
    return read_raw_batch(spark, batch), batch


def parse_args() -> argparse.Namespace:
    """Parse reader smoke-test arguments."""

    parser = argparse.ArgumentParser(
        description="Test reading a Raw S3 Parquet batch using Spark."
    )
    parser.add_argument("--table", required=True)
    parser.add_argument("--batch-id")
    return parser.parse_args()


def main() -> None:
    """Run the standalone reader smoke test."""

    args = parse_args()
    spark: SparkSession | None = None
    try:
        spark = build_spark_session(
            app_name="people-analytics-bronze-reader-test"
        )
        validate_s3a_available(spark)
        if args.batch_id:
            dataframe, batch = read_raw_table(
                spark,
                args.table,
                args.batch_id,
            )
        else:
            dataframe, batch = read_latest_raw_table(spark, args.table)
        logger.info(
            "Bronze reader test successful | "
            f"table={args.table} | rows={dataframe.count():,} | "
            f"batch_id={batch.batch_id} | "
            f"extraction_id={batch.extraction_id}"
        )
        dataframe.printSchema()
        dataframe.show(10, truncate=False)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
