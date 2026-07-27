"""
Employee exit simulation module.

Generates actual employee exit events and updates Employee current state.

Exit types, probability weights, voluntary flags and detailed reasons
are supplied from PostgreSQL reference tables seeded from YAML.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from config.constants import DEFAULT_RANDOM_SEED
from database.models import (
    Employee,
    EmployeeExit,
    ExitReason,
)


random.seed(DEFAULT_RANDOM_SEED)

EXIT_PROBABILITY = 0.03


def determine_exit_reason(
    exit_reasons: list[ExitReason],
) -> ExitReason:
    """
    Select an exit-reason reference record using configured weights.
    """

    if not exit_reasons:
        raise ValueError(
            "Exit simulation requires exit-reason "
            "reference data."
        )

    return random.choices(
        population=exit_reasons,
        weights=[
            exit_reason.weight
            for exit_reason
            in exit_reasons
        ],
        k=1,
    )[0]


def calculate_exit_date(
    employee: Employee,
) -> date:
    """
    Generate an exit date at least 180 days after hire and not in future.
    """

    today = date.today()

    earliest_exit_date = (
        employee.hire_date
        + timedelta(
            days=180
        )
    )

    available_days = (
        today
        - earliest_exit_date
    ).days

    return (
        earliest_exit_date
        + timedelta(
            days=random.randint(
                0,
                available_days,
            )
        )
    )


def generate_employee_exits(
    employees: list[Employee],
    exit_reasons: list[ExitReason],
) -> list[EmployeeExit]:
    """
    Generate EmployeeExit events and update Employee current-state fields.

    Exit types, weights, voluntary flags and detailed reasons come from
    centrally governed PostgreSQL reference data.

    Managers are excluded in Phase 1 to avoid breaking the existing
    manager hierarchy before direct-report reassignment is implemented.
    """

    if not exit_reasons:
        raise ValueError(
            "Exit simulation requires exit-reason "
            "reference data. "
            "Run python -m database.seed first."
        )

    records: list[EmployeeExit] = []

    today = date.today()

    eligible = [
        employee
        for employee in employees
        if (
            employee.is_active
            and not employee.is_manager
            and employee.hire_date
            <= today - timedelta(
                days=180
            )
            and random.random()
            < EXIT_PROBABILITY
        )
    ]

    for employee in eligible:

        exit_reason_config = (
            determine_exit_reason(
                exit_reasons
            )
        )

        exit_type = (
            exit_reason_config
            .exit_reason_name
        )

        exit_date = (
            calculate_exit_date(
                employee
            )
        )

        detailed_reasons = (
            exit_reason_config
            .reasons
        )

        if not detailed_reasons:
            raise ValueError(
                f"Exit reason '{exit_type}' "
                "does not contain detailed reasons."
            )

        exit_reason = random.choice(
            detailed_reasons
        )

        voluntary_flag = bool(
            exit_reason_config
            .voluntary
        )

        regrettable_flag = (
            voluntary_flag
            and exit_type == "Resignation"
            and random.random() < 0.35
        )

        # Keep Employee as the latest/current workforce state.
        employee.termination_date = (
            exit_date
        )

        employee.employment_status = (
            "Terminated"
        )

        employee.is_active = False

        records.append(
            EmployeeExit(
                employee=employee,
                exit_date=exit_date,

                # Existing EmployeeExit schema remains unchanged.
                exit_type=exit_type,
                exit_reason=exit_reason,

                voluntary_flag=(
                    voluntary_flag
                ),

                regrettable_flag=(
                    regrettable_flag
                ),
            )
        )

    return records
