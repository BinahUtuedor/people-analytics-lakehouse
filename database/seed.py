"""
Seed database lookup/reference tables.

This script:

1. Loads all YAML-backed reference datasets.
2. Validates them.
3. Inserts or updates PostgreSQL lookup tables.
4. Preserves dependency order.
5. Is safe to rerun without creating duplicates.

Run from the project root:

    python -m database.seed
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.logger import logger
from database.connection import get_session
from database.models import (
    AbsenceReason,
    AttendanceStatus,
    BusinessUnit,
    Department,
    EmploymentType,
    ExitReason,
    Gender,
    JobRole,
    LeaveType,
    Location,
    PublicHoliday,
    TrainingCategory,
)
from quality.reference_data_checks import (
    ReferenceDataValidationError,
    validate_reference_data,
)
from reference_data.loader import (
    ReferenceDataLoadError,
    load_all_reference_data,
)


# ---------------------------------------------------------------------------
# Generic Upsert Helper
# ---------------------------------------------------------------------------

def get_or_create(
    session: Session,
    model,
    defaults: dict[str, Any] | None = None,
    **lookup: Any,
):
    """
    Return an existing record or create it if missing.

    If the record already exists, fields supplied through `defaults`
    are compared and updated when values have changed.

    Returns:
        tuple:
            instance
            created
            updated
    """

    instance = (
        session.query(model)
        .filter_by(**lookup)
        .one_or_none()
    )

    if instance is not None:
        updated = False

        if defaults:
            for field_name, expected_value in defaults.items():

                current_value = getattr(
                    instance,
                    field_name,
                )

                if current_value != expected_value:
                    setattr(
                        instance,
                        field_name,
                        expected_value,
                    )

                    updated = True

        return (
            instance,
            False,
            updated,
        )

    params = dict(
        lookup
    )

    if defaults:
        params.update(
            defaults
        )

    instance = model(
        **params
    )

    session.add(
        instance
    )

    return (
        instance,
        True,
        False,
    )


def log_seed_result(
    entity_name: str,
    display_value: str,
    created: bool,
    updated: bool,
) -> None:
    """
    Log the result of one seeded reference record.
    """

    if created:
        logger.info(
            f"Created {entity_name}: "
            f"{display_value}"
        )

    elif updated:
        logger.info(
            f"Updated {entity_name}: "
            f"{display_value}"
        )

    else:
        logger.info(
            f"{entity_name.capitalize()} "
            f"already current: "
            f"{display_value}"
        )


# ---------------------------------------------------------------------------
# Existing Reference Tables
# ---------------------------------------------------------------------------

def seed_business_units(
    session: Session,
    records: list[dict[str, Any]],
) -> dict[str, BusinessUnit]:
    """
    Seed business units.

    Returns:
        Mapping of business-unit name to ORM object.
    """

    business_units: dict[
        str,
        BusinessUnit,
    ] = {}

    for item in records:

        unit, created, updated = (
            get_or_create(
                session,
                BusinessUnit,
                unit_name=item[
                    "unit_name"
                ],
                defaults={
                    "description": (
                        item[
                            "description"
                        ]
                    ),
                },
            )
        )

        business_units[
            unit.unit_name
        ] = unit

        log_seed_result(
            entity_name="business unit",
            display_value=unit.unit_name,
            created=created,
            updated=updated,
        )

    session.flush()

    return business_units


def seed_locations(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed office locations.
    """

    for item in records:

        location, created, updated = (
            get_or_create(
                session,
                Location,
                office_name=item[
                    "office_name"
                ],
                defaults={
                    "country": (
                        item[
                            "country"
                        ]
                    ),
                    "city": (
                        item[
                            "city"
                        ]
                    ),
                    "timezone": (
                        item[
                            "timezone"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="location",
            display_value=location.office_name,
            created=created,
            updated=updated,
        )

    session.flush()


def seed_job_roles(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed job roles.
    """

    for item in records:

        role, created, updated = (
            get_or_create(
                session,
                JobRole,
                role_name=item[
                    "role_name"
                ],
                defaults={
                    "grade": (
                        item[
                            "grade"
                        ]
                    ),
                    "salary_band_min": (
                        item[
                            "salary_band_min"
                        ]
                    ),
                    "salary_band_max": (
                        item[
                            "salary_band_max"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="job role",
            display_value=role.role_name,
            created=created,
            updated=updated,
        )

    session.flush()


def seed_departments(
    session: Session,
    records: list[dict[str, Any]],
    business_units: dict[str, BusinessUnit],
) -> None:
    """
    Seed departments and resolve business-unit relationships.
    """

    for item in records:

        unit_name = item[
            "business_unit"
        ]

        business_unit = (
            business_units.get(
                unit_name
            )
        )

        if business_unit is None:
            raise ValueError(
                "Business unit not found "
                f"for department "
                f"'{item['department_name']}': "
                f"{unit_name}"
            )

        department, created, updated = (
            get_or_create(
                session,
                Department,
                department_name=item[
                    "department_name"
                ],
                defaults={
                    "cost_center": (
                        item[
                            "cost_center"
                        ]
                    ),
                    "business_unit_id": (
                        business_unit
                        .business_unit_id
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="department",
            display_value=(
                department
                .department_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Employment Reference Tables
# ---------------------------------------------------------------------------

def seed_employment_types(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed employment types.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                EmploymentType,
                employment_type_code=(
                    item[
                        "employment_type_code"
                    ]
                ),
                defaults={
                    "employment_type_name": (
                        item[
                            "employment_type_name"
                        ]
                    ),
                    "description": (
                        item[
                            "description"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="employment type",
            display_value=(
                record
                .employment_type_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


def seed_genders(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed gender reference values.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                Gender,
                gender_code=item[
                    "gender_code"
                ],
                defaults={
                    "gender_name": (
                        item[
                            "gender_name"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="gender",
            display_value=(
                record.gender_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Attendance Reference Tables
# ---------------------------------------------------------------------------

def seed_attendance_statuses(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed attendance statuses.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                AttendanceStatus,
                attendance_status_code=(
                    item[
                        "attendance_status_code"
                    ]
                ),
                defaults={
                    "attendance_status_name": (
                        item[
                            "attendance_status_name"
                        ]
                    ),
                    "weight": (
                        item[
                            "weight"
                        ]
                    ),
                    "requires_clock_times": (
                        item[
                            "requires_clock_times"
                        ]
                    ),
                    "absence_reason_required": (
                        item[
                            "absence_reason_required"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="attendance status",
            display_value=(
                record
                .attendance_status_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


def seed_absence_reasons(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed attendance absence reasons.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                AbsenceReason,
                absence_reason_code=(
                    item[
                        "absence_reason_code"
                    ]
                ),
                defaults={
                    "absence_reason_name": (
                        item[
                            "absence_reason_name"
                        ]
                    ),
                    "description": (
                        item[
                            "description"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="absence reason",
            display_value=(
                record
                .absence_reason_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


def seed_public_holidays(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed recurring public-holiday reference data.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                PublicHoliday,
                holiday_code=item[
                    "holiday_code"
                ],
                defaults={
                    "holiday_name": (
                        item[
                            "holiday_name"
                        ]
                    ),
                    "month_day": (
                        item[
                            "month_day"
                        ]
                    ),
                    "country_code": (
                        item[
                            "country_code"
                        ]
                    ),
                    "active": (
                        item[
                            "active"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="public holiday",
            display_value=(
                record.holiday_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Leave Reference Tables
# ---------------------------------------------------------------------------

def seed_leave_types(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed leave types.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                LeaveType,
                leave_type_code=item[
                    "leave_type_code"
                ],
                defaults={
                    "leave_type_name": (
                        item[
                            "leave_type_name"
                        ]
                    ),
                    "description": (
                        item[
                            "description"
                        ]
                    ),
                    "paid": (
                        item[
                            "paid"
                        ]
                    ),
                    "requires_approval": (
                        item[
                            "requires_approval"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="leave type",
            display_value=(
                record.leave_type_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Training Reference Tables
# ---------------------------------------------------------------------------

def seed_training_categories(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed training categories.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                TrainingCategory,
                training_category_code=(
                    item[
                        "training_category_code"
                    ]
                ),
                defaults={
                    "training_category_name": (
                        item[
                            "training_category_name"
                        ]
                    ),
                    "description": (
                        item[
                            "description"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="training category",
            display_value=(
                record
                .training_category_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Exit Reference Tables
# ---------------------------------------------------------------------------

def seed_exit_reasons(
    session: Session,
    records: list[dict[str, Any]],
) -> None:
    """
    Seed employee exit reasons.

    Detailed reasons are stored in the JSON column on ExitReason.
    """

    for item in records:

        record, created, updated = (
            get_or_create(
                session,
                ExitReason,
                exit_reason_code=(
                    item[
                        "exit_reason_code"
                    ]
                ),
                defaults={
                    "exit_reason_name": (
                        item[
                            "exit_reason_name"
                        ]
                    ),
                    "voluntary": (
                        item[
                            "voluntary"
                        ]
                    ),
                    "weight": (
                        item[
                            "weight"
                        ]
                    ),
                    "reasons": (
                        item[
                            "reasons"
                        ]
                    ),
                },
            )
        )

        log_seed_result(
            entity_name="exit reason",
            display_value=(
                record
                .exit_reason_name
            ),
            created=created,
            updated=updated,
        )

    session.flush()


# ---------------------------------------------------------------------------
# Main Seed Runner
# ---------------------------------------------------------------------------

def seed_database() -> None:
    """
    Load, validate and seed all reference datasets.

    Dependency order:

        Business Units
            ↓
        Departments

    All remaining reference datasets are currently independent lookup
    tables and can be seeded after the organisational hierarchy.
    """

    logger.info(
        "=" * 70
    )

    logger.info(
        "DATABASE REFERENCE-DATA SEED"
    )

    logger.info(
        "=" * 70
    )

    session = get_session()

    try:
        # ---------------------------------------------------------------
        # Load YAML reference data
        # ---------------------------------------------------------------

        reference_data = (
            load_all_reference_data()
        )

        logger.info(
            "Reference-data YAML files "
            f"loaded successfully | "
            f"Datasets={len(reference_data)}"
        )

        # ---------------------------------------------------------------
        # Validate YAML reference data
        # ---------------------------------------------------------------

        validate_reference_data(
            reference_data
        )

        logger.info(
            "Reference-data validation "
            "completed successfully."
        )

        # ---------------------------------------------------------------
        # Organisation
        # ---------------------------------------------------------------

        business_units = (
            seed_business_units(
                session=session,
                records=reference_data[
                    "business_units"
                ],
            )
        )

        seed_locations(
            session=session,
            records=reference_data[
                "locations"
            ],
        )

        seed_job_roles(
            session=session,
            records=reference_data[
                "job_roles"
            ],
        )

        seed_departments(
            session=session,
            records=reference_data[
                "departments"
            ],
            business_units=business_units,
        )

        # ---------------------------------------------------------------
        # Employment
        # ---------------------------------------------------------------

        seed_employment_types(
            session=session,
            records=reference_data[
                "employment_types"
            ],
        )

        seed_genders(
            session=session,
            records=reference_data[
                "genders"
            ],
        )

        # ---------------------------------------------------------------
        # Attendance
        # ---------------------------------------------------------------

        seed_attendance_statuses(
            session=session,
            records=reference_data[
                "attendance_statuses"
            ],
        )

        seed_absence_reasons(
            session=session,
            records=reference_data[
                "absence_reasons"
            ],
        )

        seed_public_holidays(
            session=session,
            records=reference_data[
                "public_holidays"
            ],
        )

        # ---------------------------------------------------------------
        # Leave
        # ---------------------------------------------------------------

        seed_leave_types(
            session=session,
            records=reference_data[
                "leave_types"
            ],
        )

        # ---------------------------------------------------------------
        # Learning / Training
        # ---------------------------------------------------------------

        seed_training_categories(
            session=session,
            records=reference_data[
                "training_categories"
            ],
        )

        # ---------------------------------------------------------------
        # Workforce Exit
        # ---------------------------------------------------------------

        seed_exit_reasons(
            session=session,
            records=reference_data[
                "exit_reasons"
            ],
        )

        # ---------------------------------------------------------------
        # Commit
        # ---------------------------------------------------------------

        session.commit()

        logger.info(
            "=" * 70
        )

        logger.info(
            "DATABASE REFERENCE-DATA "
            "SEED COMPLETED SUCCESSFULLY"
        )

        logger.info(
            "=" * 70
        )

    except (
        SQLAlchemyError,
        ValueError,
        ReferenceDataLoadError,
        ReferenceDataValidationError,
    ) as error:

        session.rollback()

        logger.error(
            "Database reference-data "
            "seed failed."
        )

        logger.exception(
            error
        )

        raise

    except Exception as error:

        session.rollback()

        logger.error(
            "Unexpected database "
            "reference-data seed failure."
        )

        logger.exception(
            error
        )

        raise

    finally:
        session.close()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    seed_database()
