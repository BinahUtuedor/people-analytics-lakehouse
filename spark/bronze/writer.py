"""Duplicate-safe publication of validated Bronze Parquet batches."""

from __future__ import annotations

from pyspark.sql import DataFrame

from config.logger import logger
from config.settings import settings
from spark.bronze.reader import RawBatch, normalise_prefix


class BronzeWriteError(RuntimeError):
    """Raised when a Bronze batch cannot be published safely."""


def build_bronze_output_path(
    batch: RawBatch,
    *,
    bucket: str | None = None,
    bronze_prefix: str | None = None,
) -> str:
    """Build the batch-specific Bronze S3A output path."""

    resolved_bucket = bucket or settings.AWS_S3_BUCKET or batch.bucket
    if not resolved_bucket:
        raise BronzeWriteError("AWS_S3_BUCKET is not configured.")

    resolved_prefix = normalise_prefix(
        bronze_prefix or settings.AWS_S3_BRONZE_PREFIX
    )
    if not resolved_prefix:
        raise BronzeWriteError("The Bronze S3 prefix must not be empty.")

    return (
        f"s3a://{resolved_bucket}/{resolved_prefix}/"
        f"{batch.source_system}/{batch.table_name}/"
        f"extraction_date={batch.extraction_date}/"
        f"batch_id={batch.batch_id}/"
    )


def write_bronze(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """Publish a Bronze batch without append, overwrite, or deletion."""

    logger.info(f"Writing Bronze Parquet | path={output_path}")
    try:
        (
            dataframe.write
            .mode("errorifexists")
            .option("compression", "snappy")
            .parquet(output_path)
        )
    except Exception as error:
        raise BronzeWriteError(
            "Bronze publication failed. Existing output is never appended, "
            f"overwritten, or deleted automatically: {output_path}"
        ) from error
    logger.info(f"Bronze Parquet published | path={output_path}")
