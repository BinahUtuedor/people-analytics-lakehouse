"""
Employee exit simulation module.

Generates actual employee exit events and updates Employee current state.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, EmployeeExit


random.seed(DEFAULT_RANDOM_SEED)

EXIT_PROBABILITY = 0.03

EXIT_REASONS = {
    "Resignation": {
        "voluntary": True,
        "reasons": [
            "Accepted a new external career opportunity.",
            "Career progression opportunities were perceived as limited.",
            "Seeking improved work-life balance.",
            "Relocating for personal reasons.",
            "Seeking a role with broader responsibilities.",
            "Compensation expectations were not fully aligned.",
            "Pursuing a career change into another area.",
        ],
    },
    "Retirement": {
        "voluntary": True,
        "reasons": [
            "Retired following completion of planned career service.",
            "Chose early retirement for personal reasons.",
            "Retired after reaching planned retirement age.",
        ],
    },
    "End of Contract": {
        "voluntary": False,
        "reasons": [
            "Fixed-term contract reached its scheduled end date.",
            "Temporary assignment concluded as planned.",
            "Project-based employment ended following project completion.",
        ],
    },
    "Redundancy": {
        "voluntary": False,
        "reasons": [
            "Role removed following organisational restructuring.",
            "Position became redundant following operating model changes.",
            "Workforce reduction following changing business requirements.",
        ],
    },
    "Dismissal": {
        "voluntary": False,
        "reasons": [
            "Employment ended following sustained performance concerns.",
            "Employment terminated following a formal conduct process.",
            "Employment ended after required performance improvement was not achieved.",
        ],
    },
}


def determine_exit_type() -> str:
    """Select a realistic exit type using weighted probabilities."""

    return random.choices(
        population=[
            "Resignation",
            "Retirement",
            "End of Contract",
            "Redundancy",
            "Dismissal",
        ],
        weights=[60, 8, 15, 12, 5],
        k=1,
    )[0]


def calculate_exit_date(employee: Employee) -> date:
    """
    Generate an exit date at least 180 days after hire and not in future.
    """

    today = date.today()
    earliest_exit_date = employee.hire_date + timedelta(days=180)
    available_days = (today - earliest_exit_date).days

    return earliest_exit_date + timedelta(
        days=random.randint(0, available_days)
    )


def generate_employee_exits(
    employees: list[Employee],
) -> list[EmployeeExit]:
    """
    Generate EmployeeExit events and update Employee current-state fields.

    Managers are excluded in Phase 1 to avoid breaking the existing
    manager hierarchy before direct-report reassignment is implemented.
    """

    records: list[EmployeeExit] = []
    today = date.today()

    eligible = [
        employee
        for employee in employees
        if (
            employee.is_active
            and not employee.is_manager
            and employee.hire_date <= today - timedelta(days=180)
            and random.random() < EXIT_PROBABILITY
        )
    ]

    for employee in eligible:
        exit_type = determine_exit_type()
        config = EXIT_REASONS[exit_type]

        exit_date = calculate_exit_date(employee)
        exit_reason = random.choice(config["reasons"])
        voluntary_flag = bool(config["voluntary"])

        regrettable_flag = (
            voluntary_flag
            and exit_type == "Resignation"
            and random.random() < 0.35
        )

        # Keep Employee as the latest/current workforce state.
        employee.termination_date = exit_date
        employee.employment_status = "Terminated"
        employee.is_active = False

        records.append(
            EmployeeExit(
                employee=employee,
                exit_date=exit_date,
                exit_type=exit_type,
                exit_reason=exit_reason,
                voluntary_flag=voluntary_flag,
                regrettable_flag=regrettable_flag,
            )
        )

    return records