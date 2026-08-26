"""
Extract PostgreSQL operational tables to local Parquet files.

Each full extraction receives one shared batch_id.

Each individual table extraction also receives its own extraction_id.

Example output:

    data/raw/postgres/
        employees/
            extraction_date=2026-07-26/
                batch_id=<batch-uuid>/
                    extraction_id=<table-uuid>/
                        part-00000.parquet

A batch manifest is also written to:

    data/raw/postgres/_manifests/
        batch_id=<batch-uuid>.json

Run all configured tables:

    python -m etl.extract

Run one table:

    python -m etl.extract --table business_units
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from config.datasets import SUPPORTED_DATASETS
from config.logger import logger
from database.connection import engine


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "postgresql"
SOURCE_SCHEMA = "public"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "postgres"
)

MANIFEST_DIRECTORY = (
    RAW_DATA_DIRECTORY
    / "_manifests"
)

PARQUET_COMPRESSION = "snappy"


# Backward-compatible name retained for existing extraction callers.
EXTRACT_TABLES = SUPPORTED_DATASETS


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ExtractionError(RuntimeError):
    """Raised when PostgreSQL extraction fails."""


# ---------------------------------------------------------------------------
# Result Models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExtractionResult:
    """Summary of one completed table extraction."""

    table_name: str
    row_count: int
    column_count: int
    batch_id: str
    extraction_id: str
    extracted_at_utc: datetime
    output_path: Path


@dataclass(frozen=True)
class BatchResult:
    """Summary of one complete extraction batch."""

    batch_id: str
    batch_started_at_utc: datetime
    batch_completed_at_utc: datetime
    successful_tables: int
    failed_tables: int
    results: list[ExtractionResult]
    manifest_path: Path


# ---------------------------------------------------------------------------
# Database Inspection
# ---------------------------------------------------------------------------

def get_available_tables(
    schema: str = SOURCE_SCHEMA,
) -> set[str]:
    """
    Return physical PostgreSQL tables available in a schema.
    """

    inspector = inspect(engine)

    try:
        return set(
            inspector.get_table_names(
                schema=schema,
            )
        )

    except SQLAlchemyError as error:
        raise ExtractionError(
            f"Unable to inspect PostgreSQL schema '{schema}'."
        ) from error


def validate_table_exists(
    table_name: str,
    schema: str = SOURCE_SCHEMA,
) -> None:
    """Confirm that a requested table exists."""

    available_tables = get_available_tables(
        schema=schema,
    )

    if table_name not in available_tables:
        raise ExtractionError(
            f"Table '{schema}.{table_name}' does not exist. "
            f"Available tables: {sorted(available_tables)}"
        )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_table(
    table_name: str,
    batch_id: str,
    schema: str = SOURCE_SCHEMA,
    extraction_id: str | None = None,
    extracted_at_utc: datetime | None = None,
) -> pd.DataFrame:
    """
    Extract one PostgreSQL table into a DataFrame.

    Technical lineage metadata is appended to every row.
    """

    validate_table_exists(
        table_name=table_name,
        schema=schema,
    )

    extraction_id = (
        extraction_id
        or str(uuid4())
    )

    extracted_at_utc = (
        extracted_at_utc
        or datetime.now(timezone.utc)
    )

    logger.info(
        f"Extracting PostgreSQL table: "
        f"{schema}.{table_name} | "
        f"batch_id={batch_id}"
    )

    try:
        dataframe = pd.read_sql_table(
            table_name=table_name,
            con=engine,
            schema=schema,
        )

    except Exception as error:
        raise ExtractionError(
            f"Failed to extract '{schema}.{table_name}'."
        ) from error

    # ------------------------------------------------------------------
    # Technical lineage metadata
    # ------------------------------------------------------------------

    dataframe["_source_system"] = SOURCE_SYSTEM
    dataframe["_source_schema"] = schema
    dataframe["_source_table"] = table_name
    dataframe["_batch_id"] = batch_id
    dataframe["_extraction_id"] = extraction_id
    dataframe["_extracted_at_utc"] = extracted_at_utc

    logger.info(
        f"Extracted {len(dataframe):,} rows "
        f"from {schema}.{table_name}"
    )

    return dataframe


# ---------------------------------------------------------------------------
# Output Paths
# ---------------------------------------------------------------------------

def build_output_directory(
    table_name: str,
    batch_id: str,
    extraction_id: str,
    extracted_at_utc: datetime,
) -> Path:
    """
    Construct a partitioned raw output directory.
    """

    extraction_date = (
        extracted_at_utc
        .astimezone(timezone.utc)
        .date()
        .isoformat()
    )

    return (
        RAW_DATA_DIRECTORY
        / table_name
        / f"extraction_date={extraction_date}"
        / f"batch_id={batch_id}"
        / f"extraction_id={extraction_id}"
    )


# ---------------------------------------------------------------------------
# Parquet Writing
# ---------------------------------------------------------------------------

def write_table_to_parquet(
    dataframe: pd.DataFrame,
    table_name: str,
    batch_id: str,
    extraction_id: str,
    extracted_at_utc: datetime,
) -> Path:
    """Write one extracted DataFrame to Parquet."""

    output_directory = build_output_directory(
        table_name=table_name,
        batch_id=batch_id,
        extraction_id=extraction_id,
        extracted_at_utc=extracted_at_utc,
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / "part-00000.parquet"
    )

    try:
        dataframe.to_parquet(
            output_path,
            engine="pyarrow",
            compression=PARQUET_COMPRESSION,
            index=False,
        )

    except Exception as error:
        raise ExtractionError(
            f"Failed to write Parquet for "
            f"table '{table_name}' to '{output_path}'."
        ) from error

    logger.info(
        f"Parquet created: {output_path}"
    )

    return output_path


# ---------------------------------------------------------------------------
# Single Table Extraction
# ---------------------------------------------------------------------------

def extract_and_write_table(
    table_name: str,
    batch_id: str,
    schema: str = SOURCE_SCHEMA,
) -> ExtractionResult:
    """Extract one table and write it to Parquet."""

    extraction_id = str(uuid4())

    extracted_at_utc = datetime.now(
        timezone.utc
    )

    dataframe = extract_table(
        table_name=table_name,
        batch_id=batch_id,
        schema=schema,
        extraction_id=extraction_id,
        extracted_at_utc=extracted_at_utc,
    )

    output_path = write_table_to_parquet(
        dataframe=dataframe,
        table_name=table_name,
        batch_id=batch_id,
        extraction_id=extraction_id,
        extracted_at_utc=extracted_at_utc,
    )

    result = ExtractionResult(
        table_name=table_name,
        row_count=len(dataframe),
        column_count=len(dataframe.columns),
        batch_id=batch_id,
        extraction_id=extraction_id,
        extracted_at_utc=extracted_at_utc,
        output_path=output_path,
    )

    logger.info(
        f"Extraction complete: "
        f"{table_name} | "
        f"{result.row_count:,} rows | "
        f"{result.column_count} columns | "
        f"batch_id={batch_id}"
    )

    return result


# ---------------------------------------------------------------------------
# Batch Manifest
# ---------------------------------------------------------------------------

def write_batch_manifest(
    batch_id: str,
    batch_started_at_utc: datetime,
    batch_completed_at_utc: datetime,
    results: list[ExtractionResult],
    failed_tables: list[str],
    schema: str,
) -> Path:
    """
    Write a JSON manifest describing one extraction batch.
    """

    MANIFEST_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_path = (
        MANIFEST_DIRECTORY
        / f"batch_id={batch_id}.json"
    )

    manifest = {
        "batch_id": batch_id,
        "source_system": SOURCE_SYSTEM,
        "source_schema": schema,
        "batch_started_at_utc": (
            batch_started_at_utc.isoformat()
        ),
        "batch_completed_at_utc": (
            batch_completed_at_utc.isoformat()
        ),
        "successful_tables": len(results),
        "failed_tables": failed_tables,
        "tables": [
            {
                "table_name": result.table_name,
                "row_count": result.row_count,
                "column_count": result.column_count,
                "batch_id": result.batch_id,
                "extraction_id": result.extraction_id,
                "extracted_at_utc": (
                    result.extracted_at_utc.isoformat()
                ),
                "output_path": str(
                    result.output_path.resolve()
                ),
            }
            for result in results
        ],
    }

    try:
        with manifest_path.open(
            mode="w",
            encoding="utf-8",
        ) as file:
            json.dump(
                manifest,
                file,
                indent=2,
            )

    except OSError as error:
        raise ExtractionError(
            f"Unable to write batch manifest: "
            f"{manifest_path}"
        ) from error

    logger.info(
        f"Batch manifest created: {manifest_path}"
    )

    return manifest_path


# ---------------------------------------------------------------------------
# Full Batch Extraction
# ---------------------------------------------------------------------------

def run_full_extract(
    tables: Iterable[str] = EXTRACT_TABLES,
    schema: str = SOURCE_SCHEMA,
) -> BatchResult:
    """
    Extract all configured PostgreSQL tables using one shared batch_id.
    """

    batch_id = str(uuid4())

    batch_started_at_utc = datetime.now(
        timezone.utc
    )

    table_list = list(tables)

    logger.info(
        f"Starting PostgreSQL extraction for "
        f"{len(table_list)} tables | "
        f"batch_id={batch_id}"
    )

    available_tables = get_available_tables(
        schema=schema,
    )

    results: list[ExtractionResult] = []

    failures: list[
        tuple[str, Exception]
    ] = []

    for table_name in table_list:

        if table_name not in available_tables:
            error = ExtractionError(
                f"Configured table does not exist: "
                f"{schema}.{table_name}"
            )

            logger.error(str(error))

            failures.append(
                (
                    table_name,
                    error,
                )
            )

            continue

        try:
            result = extract_and_write_table(
                table_name=table_name,
                batch_id=batch_id,
                schema=schema,
            )

            results.append(result)

        except Exception as error:
            logger.exception(
                f"Extraction failed for "
                f"{schema}.{table_name}"
            )

            failures.append(
                (
                    table_name,
                    error,
                )
            )

    batch_completed_at_utc = datetime.now(
        timezone.utc
    )

    failed_table_names = [
        table_name
        for table_name, _ in failures
    ]

    manifest_path = write_batch_manifest(
        batch_id=batch_id,
        batch_started_at_utc=batch_started_at_utc,
        batch_completed_at_utc=batch_completed_at_utc,
        results=results,
        failed_tables=failed_table_names,
        schema=schema,
    )

    logger.info(
        f"PostgreSQL extraction finished | "
        f"batch_id={batch_id} | "
        f"Successful: {len(results)} | "
        f"Failed: {len(failures)}"
    )

    if failures:
        raise ExtractionError(
            "One or more PostgreSQL table "
            "extractions failed: "
            + ", ".join(failed_table_names)
        )

    return BatchResult(
        batch_id=batch_id,
        batch_started_at_utc=batch_started_at_utc,
        batch_completed_at_utc=batch_completed_at_utc,
        successful_tables=len(results),
        failed_tables=len(failures),
        results=results,
        manifest_path=manifest_path,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Extract PostgreSQL operational "
            "tables to local Parquet."
        )
    )

    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help=(
            "Extract one table only. "
            "If omitted, all configured tables are extracted."
        ),
    )

    parser.add_argument(
        "--schema",
        type=str,
        default=SOURCE_SCHEMA,
        help=(
            "PostgreSQL source schema. "
            f"Default: {SOURCE_SCHEMA}"
        ),
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help=(
            "Optional batch ID when extracting "
            "one table."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Command-line entry point."""

    args = parse_arguments()

    try:

        if args.table:
            batch_id = (
                args.batch_id
                or str(uuid4())
            )

            result = extract_and_write_table(
                table_name=args.table,
                batch_id=batch_id,
                schema=args.schema,
            )

            logger.info(
                f"Output: {result.output_path}"
            )

            logger.info(
                f"Batch ID: {result.batch_id}"
            )

        else:
            batch_result = run_full_extract(
                schema=args.schema,
            )

            logger.info(
                f"Batch ID: "
                f"{batch_result.batch_id}"
            )

            logger.info(
                f"Manifest: "
                f"{batch_result.manifest_path}"
            )

            for result in batch_result.results:
                logger.info(
                    f"{result.table_name}: "
                    f"{result.row_count:,} rows "
                    f"→ {result.output_path}"
                )

    except ExtractionError as error:
        logger.error(
            f"Extraction process failed: {error}"
        )

        raise


if __name__ == "__main__":
    main()
