"""
People Analytics Lakehouse Platform CLI.

This is the root command-line entry point for the platform.

It does not contain simulation, validation or ETL business logic.
Instead, it delegates execution to the existing project modules.

Examples:

    python main.py simulate

    python main.py simulate --full-refresh

    python main.py validate

    python main.py extract

    python main.py validate-raw

    python main.py upload-s3

    python main.py raw-pipeline
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Sequence


# ---------------------------------------------------------------------------
# Command Runner
# ---------------------------------------------------------------------------

def run_command(
    command: Sequence[str],
) -> None:
    """
    Run a child Python/module command and fail immediately if it fails.

    Using sys.executable ensures the same Python interpreter / virtual
    environment currently running main.py is also used for all delegated
    project commands.
    """

    printable_command = " ".join(
        command
    )

    print(
        f"\n>>> Running: "
        f"{printable_command}\n"
    )

    subprocess.run(
        list(command),
        check=True,
    )


def run_python_module(
    module_name: str,
    *arguments: str,
) -> None:
    """
    Execute an existing project module using:

        python -m <module_name>

    Additional CLI arguments are appended unchanged.
    """

    run_command(
        [
            sys.executable,
            "-m",
            module_name,
            *arguments,
        ]
    )


# ---------------------------------------------------------------------------
# Platform Commands
# ---------------------------------------------------------------------------

def run_simulation(
    full_refresh: bool = False,
) -> None:
    """
    Run the synthetic people analytics simulator.
    """

    arguments: list[str] = []

    if full_refresh:
        arguments.append(
            "--full-refresh"
        )

    run_python_module(
        "simulator.simulator",
        *arguments,
    )


def run_validation() -> None:
    """
    Run PostgreSQL operational data-quality validation.
    """

    run_python_module(
        "quality.validation"
    )


def run_extraction() -> None:
    """
    Extract PostgreSQL source tables to local raw Parquet.
    """

    run_python_module(
        "etl.extract"
    )


def run_raw_validation() -> None:
    """
    Validate raw Parquet extracts against PostgreSQL.
    """

    run_python_module(
        "quality.raw_extraction_validation"
    )


def run_s3_upload() -> None:
    """
    Upload validated raw Parquet extracts to Amazon S3.
    """

    run_python_module(
        "etl.export_s3"
    )


def run_raw_pipeline() -> None:
    """
    Run the complete currently implemented raw-data pipeline.

    Pipeline:

        PostgreSQL
            ↓
        Local raw Parquet
            ↓
        Raw extraction validation
            ↓
        Amazon S3 raw zone
    """

    run_extraction()

    run_raw_validation()

    run_s3_upload()


def run_full_refresh_pipeline() -> None:
    """
    Rebuild synthetic operational data and run the complete raw pipeline.

    Pipeline:

        Database reference data
            ↓
        Full simulation refresh
            ↓
        Operational data-quality validation
            ↓
        Raw Parquet extraction
            ↓
        Raw extraction validation
            ↓
        Amazon S3 raw zone

    Reference-data seeding is intentionally not run automatically here.
    It remains an explicit database-management operation.
    """

    run_simulation(
        full_refresh=True
    )

    run_validation()

    run_raw_pipeline()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """
    Build the platform command-line parser.
    """

    parser = argparse.ArgumentParser(
        prog="people-analytics",
        description=(
            "People Analytics Lakehouse "
            "Platform command-line interface."
        ),
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    simulate_parser = (
        subparsers.add_parser(
            "simulate",
            help=(
                "Generate synthetic people "
                "analytics data."
            ),
        )
    )

    simulate_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help=(
            "Clear generated operational data "
            "before rebuilding the simulation."
        ),
    )

    # ------------------------------------------------------------------
    # Quality
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "validate",
        help=(
            "Run operational PostgreSQL "
            "data-quality validation."
        ),
    )

    # ------------------------------------------------------------------
    # Raw ETL
    # ------------------------------------------------------------------

    subparsers.add_parser(
        "extract",
        help=(
            "Extract PostgreSQL tables "
            "to raw Parquet."
        ),
    )

    subparsers.add_parser(
        "validate-raw",
        help=(
            "Validate raw Parquet extracts "
            "against PostgreSQL."
        ),
    )

    subparsers.add_parser(
        "upload-s3",
        help=(
            "Upload validated raw Parquet "
            "extracts to Amazon S3."
        ),
    )

    subparsers.add_parser(
        "raw-pipeline",
        help=(
            "Run extraction, raw validation "
            "and S3 upload."
        ),
    )

    subparsers.add_parser(
        "full-refresh",
        help=(
            "Rebuild simulation, validate it "
            "and run the complete raw pipeline."
        ),
    )

    return parser


def main() -> None:
    """
    Application entry point.
    """

    parser = build_parser()

    args = parser.parse_args()

    if args.command == "simulate":
        run_simulation(
            full_refresh=(
                args.full_refresh
            )
        )

    elif args.command == "validate":
        run_validation()

    elif args.command == "extract":
        run_extraction()

    elif args.command == "validate-raw":
        run_raw_validation()

    elif args.command == "upload-s3":
        run_s3_upload()

    elif args.command == "raw-pipeline":
        run_raw_pipeline()

    elif args.command == "full-refresh":
        run_full_refresh_pipeline()

    else:
        parser.error(
            f"Unknown command: "
            f"{args.command}"
        )


if __name__ == "__main__":
    main()
