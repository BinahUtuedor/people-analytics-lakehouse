"""
Leave simulation module.

Generates employee leave requests only inside the employee's employment
window.

Business rules:
- leave cannot start before hire_date;
- leave cannot end after termination_date;
- active employees use today's date as the upper boundary;
- approved/pending behaviour is preserved;
- pending requests retain approved_date=None;
- request and approval dates are also prevented from preceding hire_date.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Employee, LeaveRequest
from simulator.effective_dates import employment_end_date


random.seed(DEFAULT_RANDOM_SEED)


LEAVE_TYPES = [
    "Annual Leave",
    "Sick Leave",
    "Parental Leave",
    "Compassionate Leave",
    "Study Leave",
]


def simulation_history_start() -> date:
    """Return the earliest date used by the leave simulator."""

    return date.today() - timedelta(
        days=365 * HISTORICAL_YEARS
    )


def random_date_between(
    start_date: date,
    end_date: date,
) -> date:
    """Return a random date inside an inclusive valid date range."""

    if start_date > end_date:
        raise ValueError(
            "Cannot generate a random date because "
            f"{start_date} is after {end_date}."
        )

    return start_date + timedelta(
        days=random.randint(
            0,
            (
                end_date
                - start_date
            ).days,
        )
    )


def generate_leave_requests(
    employees: list[Employee],
) -> list[LeaveRequest]:
    """
    Generate simulated leave requests inside each employee's employment
    window.

    Employees with no overlap between their employment period and the
    configured historical simulation period receive no leave records.
    """

    records: list[LeaveRequest] = []

    managers = [
        employee
        for employee in employees
        if employee.is_manager
    ]

    if not managers:
        raise ValueError(
            "Leave simulation requires at least one manager."
        )

    historical_start = (
        simulation_history_start()
    )

    for employee in employees:

        # ---------------------------------------------------------------
        # Determine valid leave-generation window.
        # ---------------------------------------------------------------

        valid_start = max(
            employee.hire_date,
            historical_start,
        )

        valid_end = min(
            employment_end_date(
                employee
            ),
            date.today(),
        )

        if valid_start > valid_end:
            continue

        leave_count = random.randint(
            1,
            6,
        )

        for _ in range(
            leave_count
        ):

            start_date = random_date_between(
                valid_start,
                valid_end,
            )

            requested_days = random.randint(
                1,
                10,
            )

            # Cap the end date at the employee's employment boundary.
            maximum_days_available = (
                valid_end
                - start_date
            ).days + 1

            days = min(
                requested_days,
                maximum_days_available,
            )

            # Safety check for an unexpected zero-day range.
            if days <= 0:
                continue

            end_date = (
                start_date
                + timedelta(
                    days=days - 1
                )
            )

            manager = (
                employee.manager
                or random.choice(
                    managers
                )
            )

            request_status = random.choice(
                [
                    "Approved",
                    "Approved",
                    "Pending",
                ]
            )

            # -----------------------------------------------------------
            # Request date.
            #
            # Original behaviour requests leave 7-30 days in advance.
            # For employees newly hired shortly before the leave start,
            # clamp the request date to hire_date so the fact cannot
            # predate employment.
            # -----------------------------------------------------------

            requested_date = max(
                employee.hire_date,
                start_date
                - timedelta(
                    days=random.randint(
                        7,
                        30,
                    )
                ),
            )

            # -----------------------------------------------------------
            # Approval date.
            #
            # Preserve the rule:
            # - Approved -> populated
            # - Pending  -> None
            #
            # Approval is also constrained to employment and cannot
            # precede the request date.
            # -----------------------------------------------------------

            if request_status == "Approved":

                proposed_approved_date = (
                    start_date
                    - timedelta(
                        days=random.randint(
                            1,
                            6,
                        )
                    )
                )

                approved_date = max(
                    employee.hire_date,
                    requested_date,
                    proposed_approved_date,
                )

                # Approval should not logically occur after leave starts.
                approved_date = min(
                    approved_date,
                    start_date,
                )

            else:
                approved_date = None

            records.append(
                LeaveRequest(
                    employee=employee,
                    leave_type=random.choice(
                        LEAVE_TYPES
                    ),
                    start_date=start_date,
                    end_date=end_date,
                    number_of_days=Decimal(
                        days
                    ),
                    request_status=request_status,
                    requested_date=requested_date,
                    approved_date=approved_date,
                    approved_by_manager=manager,
                    reason=None,
                )
            )

    return records