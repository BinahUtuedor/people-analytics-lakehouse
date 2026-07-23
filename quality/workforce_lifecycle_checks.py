"""
Workforce lifecycle validation checks.

These checks validate consistency between recruitment, promotions,
transfers and the current Employee master state.
"""

from __future__ import annotations

from decimal import Decimal

from config.constants import INITIAL_EMPLOYEE_COUNT
from database.models import (
    Employee,
    Promotion,
    Recruitment,
    Transfer,
)


def find_filled_recruitments_without_employee(session):
    """Return filled recruitment records not linked to a successful employee."""

    return [
        recruitment
        for recruitment in session.query(Recruitment).all()
        if (
            recruitment.recruitment_status == "Filled"
            and recruitment.successful_employee is None
        )
    ]


def find_open_recruitments_with_employee(session):
    """Return open recruitment records incorrectly linked to an employee."""

    return [
        recruitment
        for recruitment in session.query(Recruitment).all()
        if (
            recruitment.recruitment_status == "Open"
            and recruitment.successful_employee is not None
        )
    ]


def find_recruitment_hire_date_mismatches(session):
    """
    Verify that a successful employee's hire date matches the recruitment
    event that created the employee.

    Department and role are deliberately not compared to current Employee
    state because a later Transfer or Promotion may validly change them.
    """

    failures = []

    for recruitment in session.query(Recruitment).all():
        employee = recruitment.successful_employee

        if employee is None:
            continue

        if employee.hire_date != recruitment.hire_date:
            failures.append(
                recruitment
            )

    return failures


def find_recruitment_headcount_mismatch(session):
    """
    Validate that successful recruitment actually increased employee count.

    Expected employee records:

        INITIAL_EMPLOYEE_COUNT + number of Filled recruitment campaigns

    EmployeeExit does not delete employees, so exits do not reduce the
    total number of Employee master records.
    """

    filled_count = (
        session.query(Recruitment)
        .filter(
            Recruitment.recruitment_status
            == "Filled"
        )
        .count()
    )

    actual_employee_count = (
        session.query(Employee)
        .count()
    )

    expected_employee_count = (
        INITIAL_EMPLOYEE_COUNT
        + filled_count
    )

    # The validation framework expects a list of offending records.
    # Returning a single marker makes the rule fail with record_count=1
    # while keeping the common validate_empty_result interface.
    if (
        actual_employee_count
        != expected_employee_count
    ):
        return [
            {
                "expected_employee_count": (
                    expected_employee_count
                ),
                "actual_employee_count": (
                    actual_employee_count
                ),
                "filled_recruitment_count": (
                    filled_count
                ),
            }
        ]

    return []


def find_employee_promotion_state_mismatches(session):
    """
    Verify that the latest promotion for each employee matches the
    Employee master role and salary.
    """

    failures = []

    promoted_employee_ids = {
        promotion.employee_id
        for promotion in session.query(Promotion).all()
    }

    for employee_id in promoted_employee_ids:
        latest = (
            session.query(Promotion)
            .filter(
                Promotion.employee_id
                == employee_id
            )
            .order_by(
                Promotion.promotion_date.desc()
            )
            .first()
        )

        employee = session.get(
            Employee,
            employee_id,
        )

        if (
            latest is None
            or employee is None
        ):
            continue

        employee_salary = Decimal(
            str(employee.annual_salary)
        ).quantize(
            Decimal("0.01")
        )

        promoted_salary = Decimal(
            str(latest.new_salary)
        ).quantize(
            Decimal("0.01")
        )

        if (
            employee.role_id
            != latest.new_role_id
            or employee_salary
            != promoted_salary
        ):
            failures.append(
                latest
            )

    return failures


def find_employee_transfer_state_mismatches(session):
    """
    Verify that the latest transfer matches Employee department,
    location and manager.
    """

    failures = []

    transferred_employee_ids = {
        transfer.employee_id
        for transfer in session.query(Transfer).all()
    }

    for employee_id in transferred_employee_ids:
        latest = (
            session.query(Transfer)
            .filter(
                Transfer.employee_id
                == employee_id
            )
            .order_by(
                Transfer.transfer_date.desc()
            )
            .first()
        )

        employee = session.get(
            Employee,
            employee_id,
        )

        if (
            latest is None
            or employee is None
        ):
            continue

        if (
            employee.department_id
            != latest.new_department_id
            or employee.location_id
            != latest.new_location_id
            or employee.manager_id
            != latest.new_manager_id
        ):
            failures.append(
                latest
            )

    return failures