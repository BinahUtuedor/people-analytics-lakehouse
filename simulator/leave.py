"""
Leave simulation module.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Employee, LeaveRequest

random.seed(DEFAULT_RANDOM_SEED)

LEAVE_TYPES = [
    "Annual Leave",
    "Sick Leave",
    "Parental Leave",
    "Compassionate Leave",
    "Study Leave",
]


def random_date_in_history() -> date:
    start = date.today() - timedelta(days=365 * HISTORICAL_YEARS)
    end = date.today()

    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_leave_requests(employees: list[Employee]) -> list[LeaveRequest]:
    records = []

    managers = [employee for employee in employees if employee.is_manager]

    for employee in employees:
        leave_count = random.randint(1, 6)

        for _ in range(leave_count):
            start_date = max(employee.hire_date, random_date_in_history())
            days = random.randint(1, 10)
            end_date = start_date + timedelta(days=days - 1)

            manager = employee.manager or random.choice(managers)

            records.append(
                LeaveRequest(
                    employee=employee,
                    leave_type=random.choice(LEAVE_TYPES),
                    start_date=start_date,
                    end_date=end_date,
                    number_of_days=Decimal(days),
                    request_status=random.choice(["Approved", "Approved", "Pending"]),
                    requested_date=start_date - timedelta(days=random.randint(7, 30)),
                    approved_date=start_date - timedelta(days=random.randint(1, 6)),
                    approved_by_manager=manager,
                    reason=None,
                )
            )

    return records