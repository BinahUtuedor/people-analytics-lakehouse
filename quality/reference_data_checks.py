"""
Reference-data validation framework.

Validates YAML-backed reference datasets before they are seeded
into PostgreSQL.

Current reference datasets:

    business_units
    departments
    locations
    job_roles
    attendance_statuses
    genders
    leave_types
    employment_types
    exit_reasons
    training_categories
    public_holidays
    absence_reasons

Run indirectly through:

    python -m database.seed

or directly:

    python -c "
    from reference_data.loader import load_all_reference_data
    from quality.reference_data_checks import validate_reference_data

    data = load_all_reference_data()
    validate_reference_data(data)

    print('ALL REFERENCE DATA PASSED')
    "
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from numbers import Number
from typing import Any


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ReferenceDataValidationError(ValueError):
    """
    Raised when one or more reference-data validation rules fail.
    """


# ---------------------------------------------------------------------------
# Required Fields
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: dict[str, set[str]] = {
    "business_units": {
        "unit_name",
        "description",
    },

    "departments": {
        "department_name",
        "cost_center",
        "business_unit",
    },

    "locations": {
        "country",
        "city",
        "office_name",
        "timezone",
    },

    "job_roles": {
        "role_name",
        "grade",
        "salary_band_min",
        "salary_band_max",
    },

    "attendance_statuses": {
        "attendance_status_code",
        "attendance_status_name",
        "weight",
        "requires_clock_times",
        "absence_reason_required",
    },

    "genders": {
        "gender_code",
        "gender_name",
    },

    "leave_types": {
        "leave_type_code",
        "leave_type_name",
        "description",
        "paid",
        "requires_approval",
    },

    "employment_types": {
        "employment_type_code",
        "employment_type_name",
        "description",
    },

    "exit_reasons": {
    "exit_reason_code",
    "exit_reason_name",
    "voluntary",
    "weight",
    "reasons",
},

    "training_categories": {
        "training_category_code",
        "training_category_name",
        "description",
    },

    "public_holidays": {
        "holiday_code",
        "holiday_name",
        "month_day",
        "country_code",
        "active",
    },

    "absence_reasons": {
        "absence_reason_code",
        "absence_reason_name",
        "description",
    },
}


# ---------------------------------------------------------------------------
# Unique Fields
# ---------------------------------------------------------------------------

UNIQUE_FIELDS: dict[str, tuple[str, ...]] = {
    "business_units": (
        "unit_name",
    ),

    "departments": (
        "department_name",
        "cost_center",
    ),

    "locations": (
        "office_name",
    ),

    "job_roles": (
        "role_name",
    ),

    "attendance_statuses": (
        "attendance_status_code",
        "attendance_status_name",
    ),

    "genders": (
        "gender_code",
        "gender_name",
    ),

    "leave_types": (
        "leave_type_code",
        "leave_type_name",
    ),

    "employment_types": (
        "employment_type_code",
        "employment_type_name",
    ),

    "exit_reasons": (
        "exit_reason_code",
        "exit_reason_name",
    ),

    "training_categories": (
        "training_category_code",
        "training_category_name",
    ),

    "public_holidays": (
        "holiday_code",
        "holiday_name",
        "month_day",
    ),

    "absence_reasons": (
        "absence_reason_code",
        "absence_reason_name",
    ),
}


# ---------------------------------------------------------------------------
# Generic Validation Helpers
# ---------------------------------------------------------------------------

def _validate_dataset_exists(
    reference_data: dict[str, list[dict[str, Any]]],
    dataset_name: str,
) -> list[dict[str, Any]]:
    """
    Confirm that a required dataset exists and contains records.
    """

    if dataset_name not in reference_data:
        raise ReferenceDataValidationError(
            f"Required reference dataset is missing: {dataset_name}"
        )

    records = reference_data[dataset_name]

    if not isinstance(records, list):
        raise ReferenceDataValidationError(
            f"Reference dataset '{dataset_name}' must be a list."
        )

    if not records:
        raise ReferenceDataValidationError(
            f"Reference dataset '{dataset_name}' must not be empty."
        )

    return records


def _validate_required_fields(
    dataset_name: str,
    records: list[dict[str, Any]],
) -> None:
    """
    Confirm that every record contains every required field
    and that required string values are not blank.
    """

    required_fields = REQUIRED_FIELDS[
        dataset_name
    ]

    errors: list[str] = []

    for index, record in enumerate(
        records,
        start=1,
    ):
        if not isinstance(record, dict):
            errors.append(
                f"{dataset_name} record {index} "
                "must be a mapping."
            )
            continue

        missing_fields = (
            required_fields
            - record.keys()
        )

        if missing_fields:
            errors.append(
                f"{dataset_name} record {index} "
                "is missing required fields: "
                f"{sorted(missing_fields)}"
            )

        empty_fields = [
            field_name
            for field_name in required_fields
            if (
                field_name in record
                and (
                    record[field_name] is None
                    or (
                        isinstance(
                            record[field_name],
                            str,
                        )
                        and not record[
                            field_name
                        ].strip()
                    )
                )
            )
        ]

        if empty_fields:
            errors.append(
                f"{dataset_name} record {index} "
                "contains empty required fields: "
                f"{sorted(empty_fields)}"
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


def _validate_unique_field(
    dataset_name: str,
    records: list[dict[str, Any]],
    field_name: str,
) -> None:
    """
    Confirm that values for one field are unique.
    """

    values = [
        record[field_name]
        for record in records
    ]

    counts = Counter(
        values
    )

    duplicates = sorted(
        str(value)
        for value, count in counts.items()
        if count > 1
    )

    if duplicates:
        raise ReferenceDataValidationError(
            f"Duplicate values found in "
            f"{dataset_name}.{field_name}: "
            f"{duplicates}"
        )


def _validate_unique_fields(
    dataset_name: str,
    records: list[dict[str, Any]],
) -> None:
    """
    Run all uniqueness checks configured for a dataset.
    """

    for field_name in UNIQUE_FIELDS.get(
        dataset_name,
        (),
    ):
        _validate_unique_field(
            dataset_name=dataset_name,
            records=records,
            field_name=field_name,
        )


def _validate_boolean_field(
    dataset_name: str,
    records: list[dict[str, Any]],
    field_name: str,
) -> None:
    """
    Confirm that a field contains actual Boolean values.
    """

    errors: list[str] = []

    for index, record in enumerate(
        records,
        start=1,
    ):
        value = record.get(
            field_name
        )

        if not isinstance(
            value,
            bool,
        ):
            errors.append(
                f"{dataset_name} record {index} "
                f"field '{field_name}' must be Boolean, "
                f"got {type(value).__name__}: {value}"
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


def _validate_code_format(
    dataset_name: str,
    records: list[dict[str, Any]],
    field_name: str,
) -> None:
    """
    Confirm that reference codes use a stable uppercase convention.

    Allowed characters:
    - A-Z
    - 0-9
    - underscore
    """

    errors: list[str] = []

    for index, record in enumerate(
        records,
        start=1,
    ):
        value = record[
            field_name
        ]

        if not isinstance(
            value,
            str,
        ):
            errors.append(
                f"{dataset_name} record {index} "
                f"field '{field_name}' must be a string."
            )
            continue

        cleaned = value.replace(
            "_",
            "",
        )

        if (
            not value
            or value != value.upper()
            or not cleaned.isalnum()
        ):
            errors.append(
                f"{dataset_name} record {index} "
                f"has invalid code '{value}' "
                f"in '{field_name}'. "
                "Use uppercase letters, numbers and underscores only."
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Business Unit / Department Validation
# ---------------------------------------------------------------------------

def _validate_department_business_units(
    departments: list[dict[str, Any]],
    business_units: list[dict[str, Any]],
) -> None:
    """
    Confirm every department references a valid business unit.
    """

    valid_business_units = {
        item["unit_name"]
        for item in business_units
    }

    errors: list[str] = []

    for department in departments:
        department_name = (
            department[
                "department_name"
            ]
        )

        business_unit_name = (
            department[
                "business_unit"
            ]
        )

        if (
            business_unit_name
            not in valid_business_units
        ):
            errors.append(
                f"Department '{department_name}' "
                f"references unknown business unit "
                f"'{business_unit_name}'."
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Job Role Validation
# ---------------------------------------------------------------------------

def _validate_job_role_salary_bands(
    job_roles: list[dict[str, Any]],
) -> None:
    """
    Validate minimum and maximum salary bands.
    """

    errors: list[str] = []

    for role in job_roles:
        role_name = (
            role["role_name"]
        )

        salary_min = (
            role["salary_band_min"]
        )

        salary_max = (
            role["salary_band_max"]
        )

        if not isinstance(
            salary_min,
            Number,
        ):
            errors.append(
                f"Job role '{role_name}' "
                "has non-numeric salary_band_min."
            )

        if not isinstance(
            salary_max,
            Number,
        ):
            errors.append(
                f"Job role '{role_name}' "
                "has non-numeric salary_band_max."
            )

        if (
            isinstance(
                salary_min,
                Number,
            )
            and salary_min < 0
        ):
            errors.append(
                f"Job role '{role_name}' "
                "has a negative salary_band_min."
            )

        if (
            isinstance(
                salary_max,
                Number,
            )
            and salary_max < 0
        ):
            errors.append(
                f"Job role '{role_name}' "
                "has a negative salary_band_max."
            )

        if (
            isinstance(
                salary_min,
                Number,
            )
            and isinstance(
                salary_max,
                Number,
            )
            and salary_min > salary_max
        ):
            errors.append(
                f"Job role '{role_name}' "
                "has salary_band_min greater than "
                "salary_band_max."
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Attendance Status Validation
# ---------------------------------------------------------------------------

def _validate_attendance_statuses(
    statuses: list[dict[str, Any]],
) -> None:
    """
    Validate attendance-status weights and behavioural flags.
    """

    errors: list[str] = []

    for status in statuses:
        status_name = (
            status[
                "attendance_status_name"
            ]
        )

        weight = (
            status[
                "weight"
            ]
        )

        if (
            not isinstance(
                weight,
                int,
            )
            or isinstance(
                weight,
                bool,
            )
        ):
            errors.append(
                f"Attendance status '{status_name}' "
                "must have an integer weight."
            )

        elif weight <= 0:
            errors.append(
                f"Attendance status '{status_name}' "
                "must have weight greater than zero."
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


def _validate_absent_status_rules(
    statuses: list[dict[str, Any]],
) -> None:
    """
    Validate the semantic rules for the Absent attendance status.
    """

    absent_statuses = [
        status
        for status in statuses
        if (
            status[
                "attendance_status_name"
            ]
            == "Absent"
        )
    ]

    if len(absent_statuses) != 1:
        raise ReferenceDataValidationError(
            "attendance_statuses must contain "
            "exactly one 'Absent' status."
        )

    absent = absent_statuses[
        0
    ]

    if absent[
        "requires_clock_times"
    ]:
        raise ReferenceDataValidationError(
            "Attendance status 'Absent' "
            "must have requires_clock_times=false."
        )

    if not absent[
        "absence_reason_required"
    ]:
        raise ReferenceDataValidationError(
            "Attendance status 'Absent' "
            "must have absence_reason_required=true."
        )


# ---------------------------------------------------------------------------
# Leave Validation
# ---------------------------------------------------------------------------

def _validate_leave_types(
    leave_types: list[dict[str, Any]],
) -> None:
    """
    Validate leave-type Boolean configuration.
    """

    _validate_boolean_field(
        dataset_name="leave_types",
        records=leave_types,
        field_name="paid",
    )

    _validate_boolean_field(
        dataset_name="leave_types",
        records=leave_types,
        field_name="requires_approval",
    )


# ---------------------------------------------------------------------------
# Exit Reason Validation
# ---------------------------------------------------------------------------

def _validate_exit_reasons(
    exit_reasons: list[dict[str, Any]],
) -> None:
    """
    Validate exit reasons, voluntary flags and detailed reason lists.
    """

    errors: list[str] = []

    for record in exit_reasons:
        exit_reason_name = (
            record[
                "exit_reason_name"
            ]
        )

        voluntary = (
            record[
                "voluntary"
            ]
        )

        reasons = (
            record[
                "reasons"
            ]
        )

        if not isinstance(
            voluntary,
            bool,
        ):
            errors.append(
                f"Exit reason '{exit_reason_name}' "
                "must have a Boolean voluntary value."
            )

        if not isinstance(
            reasons,
            list,
        ):
            errors.append(
                f"Exit reason '{exit_reason_name}' "
                "must contain a list of detailed reasons."
            )
            continue

        if not reasons:
            errors.append(
                f"Exit reason '{exit_reason_name}' "
                "must contain at least one detailed reason."
            )
            continue

        invalid_reasons = [
            reason
            for reason in reasons
            if (
                not isinstance(
                    reason,
                    str,
                )
                or not reason.strip()
            )
        ]

        if invalid_reasons:
            errors.append(
                f"Exit reason '{exit_reason_name}' "
                "contains blank or invalid detailed reasons."
            )

        duplicate_reasons = [
            reason
            for reason, count
            in Counter(
                reasons
            ).items()
            if count > 1
        ]

        if duplicate_reasons:
            errors.append(
                f"Exit reason '{exit_reason_name}' "
                "contains duplicate detailed reasons: "
                f"{duplicate_reasons}"
            )

        # Optional future field.
        if "weight" in record:
            weight = record[
                "weight"
            ]

            if (
                not isinstance(
                    weight,
                    int,
                )
                or isinstance(
                    weight,
                    bool,
                )
                or weight <= 0
            ):
                errors.append(
                    f"Exit reason '{exit_reason_name}' "
                    "must have a positive integer weight."
                )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Public Holiday Validation
# ---------------------------------------------------------------------------

def _validate_public_holidays(
    holidays: list[dict[str, Any]],
) -> None:
    """
    Validate simplified recurring public-holiday definitions.

    Current format:

        month_day: MM-DD
    """

    errors: list[str] = []

    for holiday in holidays:
        holiday_name = (
            holiday[
                "holiday_name"
            ]
        )

        month_day = (
            holiday[
                "month_day"
            ]
        )

        country_code = (
            holiday[
                "country_code"
            ]
        )

        active = (
            holiday[
                "active"
            ]
        )

        if not isinstance(
            month_day,
            str,
        ):
            errors.append(
                f"Public holiday '{holiday_name}' "
                "must have month_day as a string."
            )

        else:
            try:
                datetime.strptime(
                    month_day,
                    "%m-%d",
                )

            except ValueError:
                errors.append(
                    f"Public holiday '{holiday_name}' "
                    f"has invalid month_day '{month_day}'. "
                    "Expected format MM-DD."
                )

        if (
            not isinstance(
                country_code,
                str,
            )
            or len(
                country_code.strip()
            ) != 2
        ):
            errors.append(
                f"Public holiday '{holiday_name}' "
                "must use a two-character country_code."
            )

        elif (
            country_code
            != country_code.upper()
        ):
            errors.append(
                f"Public holiday '{holiday_name}' "
                "country_code must be uppercase."
            )

        if not isinstance(
            active,
            bool,
        ):
            errors.append(
                f"Public holiday '{holiday_name}' "
                "must have a Boolean active field."
            )

    if errors:
        raise ReferenceDataValidationError(
            "\n".join(errors)
        )


# ---------------------------------------------------------------------------
# Code Field Validation
# ---------------------------------------------------------------------------

def _validate_reference_codes(
    validated_datasets: dict[
        str,
        list[dict[str, Any]],
    ],
) -> None:
    """
    Validate stable business-code fields across reference datasets.
    """

    code_fields: dict[
        str,
        str,
    ] = {
        "attendance_statuses": (
            "attendance_status_code"
        ),
        "genders": (
            "gender_code"
        ),
        "leave_types": (
            "leave_type_code"
        ),
        "employment_types": (
            "employment_type_code"
        ),
        "exit_reasons": (
            "exit_reason_code"
        ),
        "training_categories": (
            "training_category_code"
        ),
        "public_holidays": (
            "holiday_code"
        ),
        "absence_reasons": (
            "absence_reason_code"
        ),
    }

    for (
        dataset_name,
        field_name,
    ) in code_fields.items():

        _validate_code_format(
            dataset_name=dataset_name,
            records=(
                validated_datasets[
                    dataset_name
                ]
            ),
            field_name=field_name,
        )


# ---------------------------------------------------------------------------
# Main Validation Entry Point
# ---------------------------------------------------------------------------

def validate_reference_data(
    reference_data: dict[
        str,
        list[dict[str, Any]],
    ],
) -> None:
    """
    Validate all configured reference datasets.

    Args:
        reference_data:
            Dictionary returned by
            reference_data.loader.load_all_reference_data().

    Raises:
        ReferenceDataValidationError:
            If any validation rule fails.
    """

    validated_datasets: dict[
        str,
        list[dict[str, Any]],
    ] = {}

    # ------------------------------------------------------------------
    # Generic checks
    # ------------------------------------------------------------------

    for dataset_name in REQUIRED_FIELDS:

        records = (
            _validate_dataset_exists(
                reference_data=reference_data,
                dataset_name=dataset_name,
            )
        )

        _validate_required_fields(
            dataset_name=dataset_name,
            records=records,
        )

        _validate_unique_fields(
            dataset_name=dataset_name,
            records=records,
        )

        validated_datasets[
            dataset_name
        ] = records

    # ------------------------------------------------------------------
    # Cross-reference checks
    # ------------------------------------------------------------------

    _validate_department_business_units(
        departments=(
            validated_datasets[
                "departments"
            ]
        ),
        business_units=(
            validated_datasets[
                "business_units"
            ]
        ),
    )

    # ------------------------------------------------------------------
    # Dataset-specific checks
    # ------------------------------------------------------------------

    _validate_job_role_salary_bands(
        validated_datasets[
            "job_roles"
        ]
    )

    _validate_attendance_statuses(
        validated_datasets[
            "attendance_statuses"
        ]
    )

    _validate_boolean_field(
        dataset_name="attendance_statuses",
        records=validated_datasets[
            "attendance_statuses"
        ],
        field_name="requires_clock_times",
    )

    _validate_boolean_field(
        dataset_name="attendance_statuses",
        records=validated_datasets[
            "attendance_statuses"
        ],
        field_name="absence_reason_required",
    )

    _validate_absent_status_rules(
        validated_datasets[
            "attendance_statuses"
        ]
    )

    _validate_leave_types(
        validated_datasets[
            "leave_types"
        ]
    )

    _validate_exit_reasons(
        validated_datasets[
            "exit_reasons"
        ]
    )

    _validate_public_holidays(
        validated_datasets[
            "public_holidays"
        ]
    )

    # ------------------------------------------------------------------
    # Stable-code checks
    # ------------------------------------------------------------------

    _validate_reference_codes(
        validated_datasets
    )
