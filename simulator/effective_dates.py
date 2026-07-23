"""
Shared effective-date utilities for simulator modules.

These helpers are used BEFORE ORM objects are created so generators
never create employee facts outside the employee's employment window.
"""

from __future__ import annotations

from datetime import date

from database.models import Employee


def employment_end_date(employee: Employee) -> date:
    """
    Return the last date on which employee facts may be generated.

    Active employee:
        today

    Terminated employee:
        termination_date
    """

    return (
        employee.termination_date
        if employee.termination_date is not None
        else date.today()
    )


def date_within_employment_window(
    employee: Employee,
    event_date: date,
) -> bool:
    """Return True when event_date falls within active employment."""

    return (
        employee.hire_date
        <= event_date
        <= employment_end_date(employee)
    )


def date_range_within_employment_window(
    employee: Employee,
    start_date: date,
    end_date: date,
) -> bool:
    """Return True when the complete range falls within employment."""

    return (
        employee.hire_date
        <= start_date
        <= end_date
        <= employment_end_date(employee)
    )