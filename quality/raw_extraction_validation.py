"""
Validate raw PostgreSQL-to-Parquet extraction batches.

Checks include:

- batch manifest exists and is readable
- every expected table is represented
- Parquet file exists
- Parquet file is readable
- manifest row count matches Parquet
- current PostgreSQL row count matches Parquet
- required technical metadata exists
- _batch_id is correct
- _extraction_id is correct
- _source_system is correct
- _source_schema is correct
- _source_table is correct
- extraction timestamp is populated
- no accidental pandas index columns exist

Run latest batch:

    python -m quality.raw_extraction_validation

Run specific batch:

    python -m quality.raw_extraction_validation \
        --batch-id <uuid>
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from config.logger import logger
from database.connection import engine
from etl.extract import (
    EXTRACT_TABLES,
    MANIFEST_DIRECTORY,
    SOURCE_SCHEMA,
    SOURCE_SYSTEM,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REQUIRED_METADATA_COLUMNS: set[str] = {
    "_source_system",
    "_source_schema",
    "_source_table",
    "_batch_id",
    "_extraction_id",
    "_extracted_at_utc",
}


DISALLOWED_INDEX_COLUMNS: set[str] = {
    "index",
    "__index_level_0__",
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RawExtractionValidationError(
    RuntimeError
):
    """Raised when raw extraction validation fails."""


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

@dataclass
class TableValidationResult:
    """Validation result for one extracted table."""

    table_name: str
    passed: bool = True
    manifest_row_count: int | None = None
    parquet_row_count: int | None = None
    postgres_row_count: int | None = None
    errors: list[str] = field(
        default_factory=list
    )


@dataclass
class BatchValidationResult:
    """Validation result for one extraction batch."""

    batch_id: str
    passed: bool
    tables_checked: int
    tables_passed: int
    tables_failed: int
    table_results: list[
        TableValidationResult
    ]


# ---------------------------------------------------------------------------
# Manifest Discovery
# ---------------------------------------------------------------------------

def get_manifest_path(
    batch_id: str,
) -> Path:
    """Return the manifest path for one batch."""

    return (
        MANIFEST_DIRECTORY
        / f"batch_id={batch_id}.json"
    )


def find_latest_manifest() -> Path:
    """
    Return the most recently modified batch manifest.
    """

    if not MANIFEST_DIRECTORY.exists():
        raise RawExtractionValidationError(
            "Manifest directory does not exist: "
            f"{MANIFEST_DIRECTORY}"
        )

    manifests = list(
        MANIFEST_DIRECTORY.glob(
            "batch_id=*.json"
        )
    )

    if not manifests:
        raise RawExtractionValidationError(
            "No extraction manifests were found."
        )

    return max(
        manifests,
        key=lambda path: path.stat().st_mtime,
    )


def load_manifest(
    batch_id: str | None = None,
) -> dict[str, Any]:
    """
    Load either a requested batch manifest or the latest manifest.
    """

    if batch_id:
        manifest_path = get_manifest_path(
            batch_id
        )

        if not manifest_path.exists():
            raise RawExtractionValidationError(
                f"Manifest does not exist: "
                f"{manifest_path}"
            )

    else:
        manifest_path = find_latest_manifest()

    try:
        with manifest_path.open(
            mode="r",
            encoding="utf-8",
        ) as file:
            manifest = json.load(file)

    except (
        OSError,
        json.JSONDecodeError,
    ) as error:
        raise RawExtractionValidationError(
            f"Unable to read manifest: "
            f"{manifest_path}"
        ) from error

    required_manifest_fields = {
        "batch_id",
        "source_system",
        "source_schema",
        "tables",
    }

    missing_fields = (
        required_manifest_fields
        - manifest.keys()
    )

    if missing_fields:
        raise RawExtractionValidationError(
            "Manifest is missing required fields: "
            f"{sorted(missing_fields)}"
        )

    return manifest


# ---------------------------------------------------------------------------
# PostgreSQL Reconciliation
# ---------------------------------------------------------------------------

def get_postgres_row_count(
    table_name: str,
    schema: str,
) -> int:
    """
    Return the current row count for a PostgreSQL table.

    Table names are restricted to EXTRACT_TABLES before this function
    is called, preventing arbitrary identifiers from being supplied.
    """

    if table_name not in EXTRACT_TABLES:
        raise RawExtractionValidationError(
            f"Unsupported extraction table: "
            f"{table_name}"
        )

    if schema != SOURCE_SCHEMA:
        raise RawExtractionValidationError(
            f"Unexpected PostgreSQL schema: "
            f"{schema}"
        )

    statement = text(
        f'SELECT COUNT(*) '
        f'FROM "{schema}"."{table_name}"'
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(
                statement
            )

            count = result.scalar_one()

    except Exception as error:
        raise RawExtractionValidationError(
            f"Unable to count rows in "
            f"{schema}.{table_name}"
        ) from error

    return int(count)


# ---------------------------------------------------------------------------
# Individual Validation Checks
# ---------------------------------------------------------------------------

def validate_file_exists(
    path: Path,
    result: TableValidationResult,
) -> None:
    """Validate that the Parquet artifact exists."""

    if not path.exists():
        result.errors.append(
            f"Parquet file does not exist: {path}"
        )

    elif not path.is_file():
        result.errors.append(
            f"Parquet path is not a file: {path}"
        )


def read_parquet_file(
    path: Path,
    result: TableValidationResult,
) -> pd.DataFrame | None:
    """Attempt to read a Parquet artifact."""

    try:
        return pd.read_parquet(
            path,
            engine="pyarrow",
        )

    except Exception as error:
        result.errors.append(
            f"Unable to read Parquet file: "
            f"{type(error).__name__}: {error}"
        )

        return None


def validate_required_metadata(
    dataframe: pd.DataFrame,
    result: TableValidationResult,
) -> None:
    """Validate required raw-layer metadata columns."""

    missing_columns = (
        REQUIRED_METADATA_COLUMNS
        - set(dataframe.columns)
    )

    if missing_columns:
        result.errors.append(
            "Missing technical metadata columns: "
            f"{sorted(missing_columns)}"
        )


def validate_no_index_columns(
    dataframe: pd.DataFrame,
    result: TableValidationResult,
) -> None:
    """Detect accidental DataFrame index columns."""

    invalid_columns = (
        DISALLOWED_INDEX_COLUMNS
        & set(dataframe.columns)
    )

    if invalid_columns:
        result.errors.append(
            "Unexpected index columns found: "
            f"{sorted(invalid_columns)}"
        )

    unnamed_columns = [
        column
        for column in dataframe.columns
        if str(column).startswith(
            "Unnamed:"
        )
    ]

    if unnamed_columns:
        result.errors.append(
            "Unexpected unnamed columns found: "
            f"{unnamed_columns}"
        )


def validate_metadata_values(
    dataframe: pd.DataFrame,
    table_manifest: dict[str, Any],
    batch_manifest: dict[str, Any],
    result: TableValidationResult,
) -> None:
    """
    Validate that technical metadata values are internally consistent.
    """

    if dataframe.empty:
        return

    expected_values = {
        "_source_system": (
            batch_manifest[
                "source_system"
            ]
        ),
        "_source_schema": (
            batch_manifest[
                "source_schema"
            ]
        ),
        "_source_table": (
            table_manifest[
                "table_name"
            ]
        ),
        "_batch_id": (
            batch_manifest[
                "batch_id"
            ]
        ),
        "_extraction_id": (
            table_manifest[
                "extraction_id"
            ]
        ),
    }

    for column, expected_value in (
        expected_values.items()
    ):
        if column not in dataframe.columns:
            continue

        actual_values = set(
            dataframe[column]
            .dropna()
            .astype(str)
            .unique()
        )

        if actual_values != {
            str(expected_value)
        }:
            result.errors.append(
                f"{column} expected "
                f"'{expected_value}' but found "
                f"{sorted(actual_values)}"
            )


def validate_metadata_nulls(
    dataframe: pd.DataFrame,
    result: TableValidationResult,
) -> None:
    """Validate metadata columns do not contain nulls."""

    for column in REQUIRED_METADATA_COLUMNS:

        if column not in dataframe.columns:
            continue

        null_count = int(
            dataframe[column]
            .isna()
            .sum()
        )

        if null_count > 0:
            result.errors.append(
                f"{column} contains "
                f"{null_count:,} null values."
            )


def validate_manifest_row_count(
    dataframe: pd.DataFrame,
    table_manifest: dict[str, Any],
    result: TableValidationResult,
) -> None:
    """Reconcile manifest and Parquet row counts."""

    manifest_count = int(
        table_manifest["row_count"]
    )

    parquet_count = len(dataframe)

    result.manifest_row_count = (
        manifest_count
    )

    result.parquet_row_count = (
        parquet_count
    )

    if manifest_count != parquet_count:
        result.errors.append(
            "Row-count mismatch between "
            f"manifest ({manifest_count:,}) and "
            f"Parquet ({parquet_count:,})."
        )


def validate_postgres_row_count(
    dataframe: pd.DataFrame,
    table_name: str,
    schema: str,
    result: TableValidationResult,
) -> None:
    """
    Reconcile current PostgreSQL and Parquet counts.
    """

    postgres_count = get_postgres_row_count(
        table_name=table_name,
        schema=schema,
    )

    parquet_count = len(dataframe)

    result.postgres_row_count = (
        postgres_count
    )

    if postgres_count != parquet_count:
        result.errors.append(
            "Row-count mismatch between "
            f"PostgreSQL ({postgres_count:,}) and "
            f"Parquet ({parquet_count:,})."
        )


# ---------------------------------------------------------------------------
# Table Validation
# ---------------------------------------------------------------------------

def validate_table_extract(
    table_manifest: dict[str, Any],
    batch_manifest: dict[str, Any],
) -> TableValidationResult:
    """Validate one table from an extraction batch."""

    table_name = table_manifest[
        "table_name"
    ]

    result = TableValidationResult(
        table_name=table_name
    )

    output_path = Path(
        table_manifest["output_path"]
    )

    validate_file_exists(
        path=output_path,
        result=result,
    )

    if result.errors:
        result.passed = False
        return result

    dataframe = read_parquet_file(
        path=output_path,
        result=result,
    )

    if dataframe is None:
        result.passed = False
        return result

    validate_required_metadata(
        dataframe=dataframe,
        result=result,
    )

    validate_no_index_columns(
        dataframe=dataframe,
        result=result,
    )

    validate_manifest_row_count(
        dataframe=dataframe,
        table_manifest=table_manifest,
        result=result,
    )

    validate_metadata_values(
        dataframe=dataframe,
        table_manifest=table_manifest,
        batch_manifest=batch_manifest,
        result=result,
    )

    validate_metadata_nulls(
        dataframe=dataframe,
        result=result,
    )

    validate_postgres_row_count(
        dataframe=dataframe,
        table_name=table_name,
        schema=batch_manifest[
            "source_schema"
        ],
        result=result,
    )

    result.passed = not result.errors

    return result


# ---------------------------------------------------------------------------
# Batch Validation
# ---------------------------------------------------------------------------

def validate_expected_tables(
    manifest: dict[str, Any],
) -> None:
    """Ensure the batch contains all configured extraction tables."""

    manifest_tables = {
        item["table_name"]
        for item in manifest["tables"]
    }

    expected_tables = set(
        EXTRACT_TABLES
    )

    missing_tables = (
        expected_tables
        - manifest_tables
    )

    unexpected_tables = (
        manifest_tables
        - expected_tables
    )

    errors: list[str] = []

    if missing_tables:
        errors.append(
            "Missing expected tables: "
            f"{sorted(missing_tables)}"
        )

    if unexpected_tables:
        errors.append(
            "Unexpected tables present: "
            f"{sorted(unexpected_tables)}"
        )

    if errors:
        raise RawExtractionValidationError(
            " | ".join(errors)
        )


def validate_batch(
    batch_id: str | None = None,
) -> BatchValidationResult:
    """
    Validate a full extraction batch.
    """

    manifest = load_manifest(
        batch_id=batch_id
    )

    resolved_batch_id = manifest[
        "batch_id"
    ]

    logger.info(
        f"Starting raw extraction validation | "
        f"batch_id={resolved_batch_id}"
    )

    if (
        manifest["source_system"]
        != SOURCE_SYSTEM
    ):
        raise RawExtractionValidationError(
            "Unexpected source system in manifest: "
            f"{manifest['source_system']}"
        )

    if (
        manifest["source_schema"]
        != SOURCE_SCHEMA
    ):
        raise RawExtractionValidationError(
            "Unexpected source schema in manifest: "
            f"{manifest['source_schema']}"
        )

    if manifest.get("failed_tables"):
        raise RawExtractionValidationError(
            "Cannot validate incomplete batch. "
            f"Failed tables: "
            f"{manifest['failed_tables']}"
        )

    validate_expected_tables(
        manifest
    )

    table_results: list[
        TableValidationResult
    ] = []

    for table_manifest in manifest[
        "tables"
    ]:
        result = validate_table_extract(
            table_manifest=table_manifest,
            batch_manifest=manifest,
        )

        table_results.append(
            result
        )

        if result.passed:
            logger.info(
                f"PASS | "
                f"{result.table_name} | "
                f"PostgreSQL="
                f"{result.postgres_row_count:,} | "
                f"Parquet="
                f"{result.parquet_row_count:,}"
            )

        else:
            logger.error(
                f"FAIL | "
                f"{result.table_name}"
            )

            for error in result.errors:
                logger.error(
                    f"  - {error}"
                )

    tables_passed = sum(
        result.passed
        for result in table_results
    )

    tables_failed = (
        len(table_results)
        - tables_passed
    )

    passed = (
        tables_failed == 0
    )

    validation_result = BatchValidationResult(
        batch_id=resolved_batch_id,
        passed=passed,
        tables_checked=len(
            table_results
        ),
        tables_passed=tables_passed,
        tables_failed=tables_failed,
        table_results=table_results,
    )

    logger.info(
        f"Raw extraction validation complete | "
        f"batch_id={resolved_batch_id} | "
        f"Passed={tables_passed} | "
        f"Failed={tables_failed}"
    )

    if not passed:
        failed_tables = [
            result.table_name
            for result in table_results
            if not result.passed
        ]

        raise RawExtractionValidationError(
            "Raw extraction validation failed "
            "for tables: "
            + ", ".join(failed_tables)
        )

    return validation_result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Validate PostgreSQL raw "
            "Parquet extraction batches."
        )
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help=(
            "Specific extraction batch ID. "
            "If omitted, the latest manifest is used."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Command-line entry point."""

    args = parse_arguments()

    result = validate_batch(
        batch_id=args.batch_id
    )

    logger.info(
        f"VALIDATION PASSED | "
        f"batch_id={result.batch_id} | "
        f"{result.tables_passed}/"
        f"{result.tables_checked} tables"
    )


if __name__ == "__main__":
    main()