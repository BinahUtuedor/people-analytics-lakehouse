"""Structural, lineage, hash, and count validation for Bronze data."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.bronze.reader import RawBatch
from spark.bronze.transform import build_record_hash


RAW_LINEAGE_COLUMNS = {
    "_source_system",
    "_source_schema",
    "_source_table",
    "_batch_id",
    "_extraction_id",
    "_extracted_at_utc",
    "_source_file",
}

BRONZE_METADATA_COLUMNS = {
    "_bronze_ingested_at",
    "_extraction_date",
    "_record_hash",
}

REQUIRED_BRONZE_COLUMNS = RAW_LINEAGE_COLUMNS | BRONZE_METADATA_COLUMNS


class BronzeValidationError(RuntimeError):
    """Raised when Bronze data fails a publication-gate check."""


@dataclass(frozen=True)
class BronzeValidationResult:
    """Successful Raw-to-Bronze reconciliation result."""

    table_name: str
    batch_id: str
    raw_count: int
    bronze_count: int
    passed: bool = True


def _has_invalid_rows(dataframe: DataFrame, condition) -> bool:
    """Check for one invalid row without collecting the dataset."""

    return dataframe.filter(condition).limit(1).count() > 0


def _validate_lineage_values(
    dataframe: DataFrame,
    batch: RawBatch,
) -> list[str]:
    expected_values = {
        "_source_system": batch.source_system,
        "_source_schema": batch.source_schema,
        "_source_table": batch.table_name,
        "_batch_id": batch.batch_id,
        "_extraction_id": batch.extraction_id,
        "_extraction_date": batch.extraction_date,
    }
    errors: list[str] = []
    for column, expected in expected_values.items():
        if column not in dataframe.columns:
            continue
        invalid = F.col(column).isNull() | (
            F.col(column).cast("string") != F.lit(str(expected))
        )
        if _has_invalid_rows(dataframe, invalid):
            errors.append(
                f"{column} contains values inconsistent with the Raw batch."
            )
    return errors


def validate_bronze(
    raw_dataframe: DataFrame,
    bronze_dataframe: DataFrame,
    batch: RawBatch,
) -> BronzeValidationResult:
    """Validate Bronze structure and reconcile Raw and Bronze row counts."""

    errors: list[str] = []
    raw_columns = set(raw_dataframe.columns)
    bronze_columns = set(bronze_dataframe.columns)

    missing_raw_columns = raw_columns - bronze_columns
    if missing_raw_columns:
        errors.append(
            "Bronze is missing Raw columns: "
            f"{sorted(missing_raw_columns)}"
        )

    missing_metadata = REQUIRED_BRONZE_COLUMNS - bronze_columns
    if missing_metadata:
        errors.append(
            "Bronze is missing required metadata columns: "
            f"{sorted(missing_metadata)}"
        )

    raw_types = {
        field.name: field.dataType
        for field in raw_dataframe.schema.fields
    }
    bronze_types = {
        field.name: field.dataType
        for field in bronze_dataframe.schema.fields
    }
    changed_types = sorted(
        column
        for column in raw_columns & bronze_columns
        if raw_types[column] != bronze_types[column]
    )
    if changed_types:
        errors.append(
            "Bronze changed Raw column types: "
            f"{changed_types}"
        )

    raw_count = raw_dataframe.count()
    bronze_count = bronze_dataframe.count()
    if raw_count != bronze_count:
        errors.append(
            "Raw-to-Bronze row-count mismatch: "
            f"Raw={raw_count:,}, Bronze={bronze_count:,}."
        )

    if not missing_metadata:
        errors.extend(_validate_lineage_values(bronze_dataframe, batch))

        required_non_null = REQUIRED_BRONZE_COLUMNS
        for column in sorted(required_non_null):
            if _has_invalid_rows(bronze_dataframe, F.col(column).isNull()):
                errors.append(f"{column} contains null values.")

        invalid_hash = ~F.col("_record_hash").rlike("^[0-9a-f]{64}$")
        if _has_invalid_rows(bronze_dataframe, invalid_hash):
            errors.append("_record_hash contains invalid SHA-256 values.")

        unexpected_hash = F.col("_record_hash") != build_record_hash(
            bronze_dataframe
        )
        if _has_invalid_rows(bronze_dataframe, unexpected_hash):
            errors.append(
                "_record_hash is inconsistent with source business values."
            )

        empty_source_file = F.length(F.trim(F.col("_source_file"))) == 0
        if _has_invalid_rows(bronze_dataframe, empty_source_file):
            errors.append("_source_file contains empty values.")

    if errors:
        raise BronzeValidationError(" | ".join(errors))

    return BronzeValidationResult(
        table_name=batch.table_name,
        batch_id=batch.batch_id,
        raw_count=raw_count,
        bronze_count=bronze_count,
    )
