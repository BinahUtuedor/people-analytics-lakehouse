"""
Promotion simulation module.
"""

from __future__ import annotations

import random
from datetime import timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, JobRole, Promotion

random.seed(DEFAULT_RANDOM_SEED)


def generate_promotions(
    employees: list[Employee],
    job_roles: list[JobRole],
) -> list[Promotion]:
    records = []

    eligible = [
        employee for employee in employees
        if not employee.is_manager and random.random() < 0.08
    ]

    for employee in eligible:
        new_role = random.choice(job_roles)
        old_salary = Decimal(employee.annual_salary)
        increase = old_salary * Decimal(str(round(random.uniform(0.08, 0.20), 2)))
        new_salary = old_salary + increase

        records.append(
            Promotion(
                employee=employee,
                old_role_id=employee.role_id,
                new_role=new_role,
                promotion_date=employee.hire_date + timedelta(days=random.randint(180, 900)),
                old_salary=old_salary,
                new_salary=new_salary.quantize(Decimal("0.01")),
                salary_increase_amount=increase.quantize(Decimal("0.01")),
                salary_increase_percent=((increase / old_salary) * 100).quantize(Decimal("0.01")),
                promotion_reason="Strong performance and increased responsibility.",
                approved_by_manager=employee.manager,
            )
        )

    return records