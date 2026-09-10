"""Deterministic source-conforming Bronze-to-Silver transformations."""

from __future__ import annotations
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType
from spark.silver.reader import BronzeBatch

SILVER_METADATA = {
    "_batch_id",
    "_extraction_date",
    "_source_file",
    "_record_hash",
    "_bronze_ingested_at",
}
MONEY_COLUMNS = {
    "salary",
    "base_salary",
    "old_salary",
    "new_salary",
    "salary_increase_amount",
    "gross_pay",
    "net_pay",
    "tax_amount",
    "overtime_pay",
    "cost",
}
DECIMAL_HINTS = ("score", "percent", "percentage", "rate", "hours", "amount")


def transform_to_silver(dataframe: DataFrame, batch: BronzeBatch) -> DataFrame:
    """Clean values, apply explicit analytical types, deduplicate hash repeats."""
    result = dataframe
    for field in dataframe.schema.fields:
        name = field.name
        if isinstance(field.dataType, StringType) and name not in SILVER_METADATA:
            trimmed = F.trim(F.col(name))
            result = result.withColumn(
                name, F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)
            )
        if name.endswith("_id") and name not in {"_batch_id", "_extraction_id"}:
            result = result.withColumn(name, F.col(name).cast(LongType()))
        elif name.endswith("_date"):
            result = result.withColumn(name, F.to_date(F.col(name)))
        elif name in MONEY_COLUMNS:
            result = result.withColumn(name, F.col(name).cast(DecimalType(18, 2)))
        elif any(hint in name for hint in DECIMAL_HINTS):
            result = result.withColumn(name, F.col(name).cast(DecimalType(18, 4)))
    if "_record_hash" in result.columns:
        order = [
            F.col(c).asc_nulls_last()
            for c in ("_bronze_ingested_at", "_source_file")
            if c in result.columns
        ]
        result = (
            result.withColumn(
                "_silver_row_number",
                F.row_number().over(Window.partitionBy("_record_hash").orderBy(*order)),
            )
            .filter(F.col("_silver_row_number") == 1)
            .drop("_silver_row_number")
        )
    return result.withColumn("_silver_transformed_at", F.current_timestamp())
