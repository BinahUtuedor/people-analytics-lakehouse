"""
Effective-date quality checks.

Every employee-level fact must fall inside the employee's employment
window from hire_date through termination_date (or today for active
employees).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from database.models import (
    Attendance,
    Employee,
    EmployeeSurvey,
    LeaveRequest,
    ManagerFeedback,
    Payroll,
    PerformanceReview,
    Promotion,
    Training,
    Transfer,
)


def _employment_end(
    employee: Employee,
) -> date:
    return (
        employee.termination_date
        if employee.termination_date is not None
        else date.today()
    )


def _outside(
    employee: Employee,
    start_date: date,
    end_date: date | None = None,
) -> bool:
    end_value = (
        end_date
        if end_date is not None
        else start_date
    )

    return not (
        employee.hire_date
        <= start_date
        <= end_value
        <= _employment_end(employee)
    )


def _single_date_failures(
    records: list[Any],
    date_field: str,
) -> list[Any]:
    failures = []

    for record in records:
        employee = record.employee
        event_date = getattr(
            record,
            date_field,
        )

        if _outside(
            employee,
            event_date,
        ):
            failures.append(
                record
            )

    return failures


def find_attendance_outside_employment(session):
    return _single_date_failures(
        session.query(Attendance).all(),
        "work_date",
    )


def find_payroll_outside_employment(session):
    failures = []

    for record in session.query(Payroll).all():
        if _outside(
            record.employee,
            record.pay_period_start,
            record.pay_period_end,
        ):
            failures.append(
                record
            )

    return failures


def find_leave_outside_employment(session):
    failures = []

    for record in session.query(LeaveRequest).all():
        start_date = getattr(
            record,
            "start_date",
        )

        end_date = getattr(
            record,
            "end_date",
            start_date,
        )

        if _outside(
            record.employee,
            start_date,
            end_date,
        ):
            failures.append(
                record
            )

    return failures


def find_training_outside_employment(session):
    """
    Support the common Training date fields used by this project family.
    """

    failures = []

    candidates = (
        ("start_date", "completion_date"),
        ("start_date", "end_date"),
        ("training_date", None),
        ("completion_date", None),
    )

    for record in session.query(Training).all():
        resolved = None

        for (
            start_field,
            end_field,
        ) in candidates:
            if not hasattr(
                record,
                start_field,
            ):
                continue

            start_value = getattr(
                record,
                start_field,
            )

            if start_value is None:
                continue

            end_value = (
                getattr(
                    record,
                    end_field,
                    None,
                )
                if end_field
                else start_value
            )

            resolved = (
                start_value,
                end_value
                if end_value is not None
                else start_value,
            )

            break

        if resolved is None:
            continue

        if _outside(
            record.employee,
            resolved[0],
            resolved[1],
        ):
            failures.append(
                record
            )

    return failures


def find_performance_reviews_outside_employment(session):
    return _single_date_failures(
        session.query(PerformanceReview).all(),
        "review_date",
    )


def find_surveys_outside_employment(session):
    return _single_date_failures(
        session.query(EmployeeSurvey).all(),
        "survey_date",
    )


def find_manager_feedback_outside_employment(session):
    return _single_date_failures(
        session.query(ManagerFeedback).all(),
        "feedback_date",
    )


def find_promotions_outside_employment(session):
    return _single_date_failures(
        session.query(Promotion).all(),
        "promotion_date",
    )


def find_transfers_outside_employment(session):
    return _single_date_failures(
        session.query(Transfer).all(),
        "transfer_date",
    )