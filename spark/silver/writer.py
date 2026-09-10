"""Duplicate-safe Silver Parquet publication."""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from config.settings import settings
from spark.bronze.reader import normalise_prefix
from spark.silver.reader import BronzeBatch


class SilverWriteError(RuntimeError):
    pass


def build_silver_output_path(
    batch: BronzeBatch, *, bucket: str | None = None, silver_prefix: str | None = None
) -> str:
    target_bucket = bucket or settings.AWS_S3_BUCKET or batch.bucket
    prefix = normalise_prefix(silver_prefix or settings.AWS_S3_SILVER_PREFIX)
    if not target_bucket or not prefix:
        raise SilverWriteError("Silver bucket and prefix must be configured.")
    return f"s3a://{target_bucket}/{prefix}/{batch.source_system}/{batch.table_name}/extraction_date={batch.extraction_date}/batch_id={batch.batch_id}/"


def silver_output_exists(spark: SparkSession, path: str) -> bool:
    try:
        hpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        return bool(
            hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(
                hpath
            )
        )
    except Exception as error:
        raise SilverWriteError(
            f"Unable to inspect Silver output path: {path}"
        ) from error


def write_silver(dataframe: DataFrame, path: str) -> None:
    try:
        dataframe.write.mode("errorifexists").option("compression", "snappy").parquet(
            path
        )
    except Exception as error:
        raise SilverWriteError(
            f"Silver publication failed without overwrite: {path}"
        ) from error
