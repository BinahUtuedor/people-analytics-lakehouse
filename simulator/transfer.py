"""
Transfer simulation module.

This module generates realistic employee transfer records.

For every generated transfer:

- the new department must differ from the employee's current department;
- the new location must differ from the employee's current location;
- the new manager must differ from the employee's current manager;
- the employee cannot become their own manager;
- transfer reasons are varied and aligned with the transfer context.

"""

from __future__ import annotations

import random
from datetime import timedelta

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Department, Employee, Location, Transfer


# -------------------------------------------------------------------
# Random seed
#
# Preserves reproducibility across simulation runs.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Transfer reasons
#
# Reasons are grouped according to the type of organisational change
# taking place. This creates more realistic synthetic transfer data
# for downstream workforce and mobility analysis.
# -------------------------------------------------------------------

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

MANAGER_TRANSFER_REASONS = [
    "Reporting-line change following team restructuring.",
    "Transfer to a new manager for specialist development.",
    "Management reassignment following organisational changes.",
    "Move to a team with different leadership responsibilities.",
    "Reporting-line change to support career development.",
    "Manager reassignment following changes in team structure.",
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
    """
    Select a department different from the employee's current department.

    Args:
        employee:
            Employee being transferred.

        departments:
            Available Department ORM objects.

    Returns:
        Department | None:
            A different department, or None when no alternative exists.
    """

    alternatives = [
        department
        for department in departments
        if department.department_id != employee.department_id
    ]

    if not alternatives:
        return None

    return random.choice(alternatives)


def choose_different_location(
    employee: Employee,
    locations: list[Location],
) -> Location | None:
    """
    Select a location different from the employee's current location.

    Args:
        employee:
            Employee being transferred.

        locations:
            Available Location ORM objects.

    Returns:
        Location | None:
            A different location, or None when no alternative exists.
    """

    alternatives = [
        location
        for location in locations
        if location.location_id != employee.location_id
    ]

    if not alternatives:
        return None

    return random.choice(alternatives)


def choose_different_manager(
    employee: Employee,
    managers: list[Employee],
) -> Employee | None:
    """
    Select a manager different from the employee's current manager.

    The employee is also explicitly excluded from the candidate list.

    Args:
        employee:
            Employee being transferred.

        managers:
            Employees identified as managers.

    Returns:
        Employee | None:
            A different manager, or None when no alternative exists.
    """

    current_manager_id = (
        employee.manager.employee_id
        if employee.manager is not None
        else None
    )

    alternatives = [
        manager
        for manager in managers
        if manager.employee_id != current_manager_id
        and manager.employee_id != employee.employee_id
    ]

    if not alternatives:
        return None

    return random.choice(alternatives)


def determine_transfer_type() -> str:
    """
    Select the type of employee transfer.

    """

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
    """
    Generate a transfer reason appropriate to the transfer type.

    Args:
        transfer_type:
            Generated transfer category.

    Returns:
        str:
            Contextually appropriate transfer reason.
    """

    if transfer_type == "Department Move":
        return random.choice(
            DEPARTMENT_TRANSFER_REASONS
        )

    if transfer_type == "Location Move":
        return random.choice(
            LOCATION_TRANSFER_REASONS
        )

    # Internal transfers can be driven by a wider range of organisational
    # or employee-development factors.
    return random.choice(
        GENERAL_TRANSFER_REASONS
        + MANAGER_TRANSFER_REASONS
    )


def generate_transfers(
    employees: list[Employee],
    departments: list[Department],
    locations: list[Location],
) -> list[Transfer]:
    """
    Generate employee transfer records.

    Approximately 5% of non-manager employees remain eligible for
    transfer.

    Every generated transfer changes:

    - department;
    - location;
    - manager.

    Args:
        employees:
            Available Employee ORM objects.

        departments:
            Available Department ORM objects.

        locations:
            Available Location ORM objects.

    Returns:
        list[Transfer]:
            Generated transfer records.
    """

    records = []

    # -------------------------------------------------------------------
    # Uses 5% transfer probability.
    # Managers are excluded from the transfer population.
    # -------------------------------------------------------------------

    eligible = [
        employee
        for employee in employees
        if not employee.is_manager
        and random.random() < 0.05
    ]

    # -------------------------------------------------------------------
    # Build the available manager population once rather than repeatedly
    # scanning the complete employee list.
    # -------------------------------------------------------------------

    managers = [
        employee
        for employee in employees
        if employee.is_manager
    ]

    for employee in eligible:

        # ---------------------------------------------------------------
        # Select values that are guaranteed to differ from the employee's
        # current organisational assignments.
        # ---------------------------------------------------------------

        new_department = choose_different_department(
            employee,
            departments,
        )

        new_location = choose_different_location(
            employee,
            locations,
        )

        new_manager = choose_different_manager(
            employee,
            managers,
        )

        # ---------------------------------------------------------------
        # A valid transfer requires all three replacement values.
        #
        # With the current project this should normally never trigger
        # because the project has multiple departments, locations and managers.
        # It nevertheless protects the simulator from producing an
        # invalid record if the reference data changes later.
        # ---------------------------------------------------------------

        if (
            new_department is None
            or new_location is None
            or new_manager is None
        ):
            continue

        # ---------------------------------------------------------------
        # Generate transfer classification before the reason so that
        # the reason can reflect the transfer context.
        # ---------------------------------------------------------------

        transfer_type = determine_transfer_type()

        transfer_reason = generate_transfer_reason(
            transfer_type
        )

        # ---------------------------------------------------------------
        # Create the Transfer ORM record.
        # ---------------------------------------------------------------

        records.append(
            Transfer(
                employee=employee,

                old_department_id=employee.department_id,
                new_department=new_department,

                old_location_id=employee.location_id,
                new_location=new_location,

                old_manager=employee.manager,
                new_manager=new_manager,

                transfer_date=(
                    employee.hire_date
                    + timedelta(
                        days=random.randint(
                            90,
                            900,
                        )
                    )
                ),

                transfer_type=transfer_type,

                transfer_reason=transfer_reason,
            )
        )

    return records
