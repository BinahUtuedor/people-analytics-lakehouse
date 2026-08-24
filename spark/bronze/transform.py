"""Source-conformed Bronze transformations for Raw Spark DataFrames."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.bronze.reader import RawBatch


class BronzeTransformError(RuntimeError):
    """Raised when a Raw DataFrame cannot be transformed safely."""


def get_business_columns(dataframe: DataFrame) -> list[str]:
    """Return lexically ordered source business columns."""

    columns = sorted(
        column
        for column in dataframe.columns
        if not column.startswith("_")
    )
    if not columns:
        raise BronzeTransformError(
            "Bronze record hashing requires at least one business column."
        )
    return columns


def build_record_hash(dataframe: DataFrame) -> Column:
    """Build the documented canonical SHA-256 business-record hash."""

    business_columns = get_business_columns(dataframe)
    canonical_record = F.to_json(
        F.struct(
            *[
                F.col(column).alias(column)
                for column in business_columns
            ]
        ),
        options={"ignoreNullFields": "false"},
    )
    return F.sha2(canonical_record, 256)


def transform_to_bronze(
    raw_dataframe: DataFrame,
    batch: RawBatch,
    *,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """Preserve Raw values and append Bronze technical metadata."""

    if "_source_file" not in raw_dataframe.columns:
        raise BronzeTransformError(
            "Raw DataFrame is missing reader-provided _source_file lineage."
        )

    ingested_at_column = (
        F.lit(ingested_at).cast("timestamp")
        if ingested_at is not None
        else F.current_timestamp()
    )

    return (
        raw_dataframe
        .withColumn("_bronze_ingested_at", ingested_at_column)
        .withColumn(
            "_extraction_date",
            F.to_date(F.lit(batch.extraction_date)),
        )
        .withColumn("_record_hash", build_record_hash(raw_dataframe))
    )
