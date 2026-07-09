"""
Transfer simulation module.
"""

from __future__ import annotations

import random
from datetime import timedelta

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Department, Employee, Location, Transfer

random.seed(DEFAULT_RANDOM_SEED)


def generate_transfers(
    employees: list[Employee],
    departments: list[Department],
    locations: list[Location],
) -> list[Transfer]:
    records = []

    eligible = [
        employee for employee in employees
        if not employee.is_manager and random.random() < 0.05
    ]

    managers = [employee for employee in employees if employee.is_manager]

    for employee in eligible:
        new_department = random.choice(departments)
        new_location = random.choice(locations)
        new_manager = random.choice(managers)

        records.append(
            Transfer(
                employee=employee,
                old_department_id=employee.department_id,
                new_department=new_department,
                old_location_id=employee.location_id,
                new_location=new_location,
                old_manager=employee.manager,
                new_manager=new_manager,
                transfer_date=employee.hire_date + timedelta(days=random.randint(90, 900)),
                transfer_type=random.choice(["Internal Transfer", "Department Move", "Location Move"]),
                transfer_reason="Business need and career development.",
            )
        )

    return records