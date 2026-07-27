"""
Reference-data YAML loader.

This module reads static reference datasets from the reference_data
directory and returns them as Python dictionaries.

It does not interact with PostgreSQL or create ORM objects.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REFERENCE_DATA_DIRECTORY = Path(__file__).resolve().parent


REFERENCE_DATA_FILES: dict[str, str] = {
    "business_units": "business_units.yml",
    "departments": "departments.yml",
    "locations": "locations.yml",
    "job_roles": "job_roles.yml",
    "attendance_statuses": "attendance_statuses.yml",
    "genders": "genders.yml",
    "leave_types": "leave_types.yml",
    "employment_types": "employment_types.yml",
    "exit_reasons": "exit_reasons.yml",
    "training_categories": "training_categories.yml",
    "public_holidays": "public_holidays.yml",
    "absence_reasons": "absence_reasons.yml",
}


class ReferenceDataLoadError(Exception):
    """Raised when a reference-data file cannot be loaded."""


def _resolve_reference_file(dataset_name: str) -> Path:
    """
    Resolve and validate the path for a named reference dataset.

    Args:
        dataset_name: Logical name of the reference dataset.

    Returns:
        Absolute path to the YAML file.

    Raises:
        ReferenceDataLoadError: If the dataset is unknown or missing.
    """

    filename = REFERENCE_DATA_FILES.get(dataset_name)

    if filename is None:
        available_datasets = ", ".join(
            sorted(REFERENCE_DATA_FILES)
        )

        raise ReferenceDataLoadError(
            f"Unknown reference dataset: {dataset_name}. "
            f"Available datasets: {available_datasets}"
        )

    path = REFERENCE_DATA_DIRECTORY / filename

    if not path.exists():
        raise ReferenceDataLoadError(
            f"Reference-data file does not exist: {path}"
        )

    if not path.is_file():
        raise ReferenceDataLoadError(
            f"Reference-data path is not a file: {path}"
        )

    if path.suffix.lower() not in {".yml", ".yaml"}:
        raise ReferenceDataLoadError(
            f"Reference-data file must be YAML: {path}"
        )

    return path


def load_yaml(dataset_name: str) -> Any:
    """
    Load one named YAML reference dataset.

    Args:
        dataset_name: Logical name configured in REFERENCE_DATA_FILES.

    Returns:
        Parsed YAML content.

    Raises:
        ReferenceDataLoadError: If the file cannot be read or parsed.
    """

    path = _resolve_reference_file(dataset_name)

    try:
        with path.open(
            mode="r",
            encoding="utf-8",
        ) as file:
            data = yaml.safe_load(file)

    except yaml.YAMLError as error:
        raise ReferenceDataLoadError(
            f"Invalid YAML in {path}: {error}"
        ) from error

    except OSError as error:
        raise ReferenceDataLoadError(
            f"Unable to read {path}: {error}"
        ) from error

    if data is None:
        raise ReferenceDataLoadError(
            f"Reference-data file is empty: {path}"
        )

    return data


def load_reference_records(
    dataset_name: str,
) -> list[dict[str, Any]]:
    """
    Load a reference dataset as a list of dictionaries.

    Args:
        dataset_name: Logical reference dataset name.

    Returns:
        List of reference-data records.

    Raises:
        ReferenceDataLoadError: If the top-level YAML value is not a list
        or if an individual record is not a dictionary.
    """

    data = load_yaml(dataset_name)

    if not isinstance(data, list):
        raise ReferenceDataLoadError(
            f"Reference dataset '{dataset_name}' must contain "
            "a top-level YAML list."
        )

    records: list[dict[str, Any]] = []

    for index, record in enumerate(
        data,
        start=1,
    ):
        if not isinstance(record, dict):
            raise ReferenceDataLoadError(
                f"Record {index} in '{dataset_name}' must be "
                "a YAML mapping."
            )

        records.append(record)

    return records


def load_all_reference_data(
) -> dict[str, list[dict[str, Any]]]:
    """
    Load all configured reference datasets.

    Returns:
        Dictionary keyed by logical dataset name.
    """

    return {
        dataset_name: load_reference_records(
            dataset_name
        )
        for dataset_name in REFERENCE_DATA_FILES
    }