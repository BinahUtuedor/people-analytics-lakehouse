"""
Transfer simulation module.

Generates employee transfers and updates Employee current organisational
state while preserving the old/new values in Transfer history.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from config.constants import DEFAULT_RANDOM_SEED
from database.models import (
    Department,
    Employee,
    Location,
    Transfer,
)


random.seed(DEFAULT_RANDOM_SEED)


DEPARTMENT_TRANSFER_REASONS = [
    "Career development opportunity in another department.",
    "Skills aligned more closely with another business area.",
    "Internal mobility following organisational restructuring.",
    "Transfer to support a priority project in another department.",
    "Employee requested broader cross-functional experience.",
    "Role realignment following changes in business requirements.",
    "Transfer to address resourcing requirements in another team.",
]

LOCATION_TRANSFER_REASONS = [
    "Employee requested relocation to another office.",
    "Relocation to support regional business requirements.",
    "Transfer following changes in operational coverage.",
    "Move to an office with greater team presence.",
    "Location change to support a new assignment.",
    "Relocation following changes in workforce capacity.",
    "Transfer to support business expansion in another location.",
]

GENERAL_TRANSFER_REASONS = [
    "Business need and career development.",
    "Internal mobility opportunity aligned with employee development.",
    "Organisational restructuring and workforce realignment.",
    "Employee development and changing operational requirements.",
    "Transfer to support evolving business priorities.",
    "Cross-functional development opportunity.",
    "Workforce reallocation to meet changing business demand.",
]


def choose_different_department(
    employee: Employee,
    departments: list[Department],
) -> Department | None:
    """Choose a department different from the current department."""

    alternatives = [
        department
        for department in departments
        if department.department_id
        != employee.department_id
    ]

    return (
        random.choice(alternatives)
        if alternatives
        else None
    )


def choose_different_location(
    employee: Employee,
    locations: list[Location],
) -> Location | None:
    """Choose a location different from the current location."""

    alternatives = [
        location
        for location in locations
        if location.location_id
        != employee.location_id
    ]

    return (
        random.choice(alternatives)
        if alternatives
        else None
    )


def choose_different_manager(
    employee: Employee,
    managers: list[Employee],
) -> Employee | None:
    """Choose an active manager different from the current manager."""

    current_manager_id = (
        employee.manager.employee_id
        if employee.manager is not None
        else None
    )

    alternatives = [
        manager
        for manager in managers
        if (
            manager.is_active
            and manager.employee_id
            != current_manager_id
            and manager.employee_id
            != employee.employee_id
        )
    ]

    return (
        random.choice(alternatives)
        if alternatives
        else None
    )


def determine_transfer_type() -> str:
    """Preserve the project's existing transfer-type categories."""

    return random.choice(
        [
            "Internal Transfer",
            "Department Move",
            "Location Move",
        ]
    )


def generate_transfer_reason(
    transfer_type: str,
) -> str:
    """Generate a varied transfer reason aligned with transfer type."""

    if transfer_type == "Department Move":
        return random.choice(
            DEPARTMENT_TRANSFER_REASONS
        )

    if transfer_type == "Location Move":
        return random.choice(
            LOCATION_TRANSFER_REASONS
        )

    return random.choice(
        GENERAL_TRANSFER_REASONS
    )


def generate_transfers(
    employees: list[Employee],
    departments: list[Department],
    locations: list[Location],
) -> list[Transfer]:
    """
    Generate transfer events and update Employee current state.

    The old department/location/manager values are captured first.
    After the Transfer event is created, Employee is updated to the new
    department, location and manager.
    """

    records: list[Transfer] = []

    today = date.today()

    eligible = [
        employee
        for employee in employees
        if (
            employee.is_active
            and not employee.is_manager
            and random.random() < 0.05
        )
    ]

    managers = [
        employee
        for employee in employees
        if (
            employee.is_manager
            and employee.is_active
        )
    ]

    for employee in eligible:
        new_department = (
            choose_different_department(
                employee,
                departments,
            )
        )

        new_location = (
            choose_different_location(
                employee,
                locations,
            )
        )

        new_manager = (
            choose_different_manager(
                employee,
                managers,
            )
        )

        if (
            new_department is None
            or new_location is None
            or new_manager is None
        ):
            continue

        earliest_date = (
            employee.hire_date
            + timedelta(days=90)
        )

        if earliest_date > today:
            continue

        old_department_id = (
            employee.department_id
        )

        old_location_id = (
            employee.location_id
        )

        old_manager = (
            employee.manager
        )

        transfer_date = (
            employee.hire_date
            + timedelta(
                days=random.randint(
                    90,
                    min(
                        900,
                        (
                            today
                            - employee.hire_date
                        ).days,
                    ),
                )
            )
        )

        transfer_type = (
            determine_transfer_type()
        )

        transfer = Transfer(
            employee=employee,
            old_department_id=(
                old_department_id
            ),
            new_department=(
                new_department
            ),
            old_location_id=(
                old_location_id
            ),
            new_location=(
                new_location
            ),
            old_manager=(
                old_manager
            ),
            new_manager=(
                new_manager
            ),
            transfer_date=(
                transfer_date
            ),
            transfer_type=(
                transfer_type
            ),
            transfer_reason=(
                generate_transfer_reason(
                    transfer_type
                )
            ),
        )

        records.append(
            transfer
        )

        # ---------------------------------------------------------------
        # Update current Employee organisational state only after the old
        # state has been preserved in the Transfer event.
        # ---------------------------------------------------------------

        employee.department = (
            new_department
        )

        employee.location = (
            new_location
        )

        employee.manager = (
            new_manager
        )

    return records