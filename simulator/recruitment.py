"""
Recruitment simulation module.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from faker import Faker

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Department, Employee, JobRole, Recruitment

fake = Faker("en_GB")
random.seed(DEFAULT_RANDOM_SEED)


def generate_recruitment(
    departments: list[Department],
    job_roles: list[JobRole],
    employees: list[Employee],
) -> list[Recruitment]:
    records = []

    vacancy_count = max(50, int(len(employees) * 0.08))
    start = date.today() - timedelta(days=365 * HISTORICAL_YEARS)

    for index in range(1, vacancy_count + 1):
        opening_date = start + timedelta(days=random.randint(0, 365 * HISTORICAL_YEARS))
        filled = random.choice([True, True, True, False])

        records.append(
            Recruitment(
                department=random.choice(departments),
                job_role=random.choice(job_roles),
                vacancy_reference=f"VAC-{index:05d}",
                candidate_name=fake.name() if filled else None,
                opening_date=opening_date,
                closing_date=opening_date + timedelta(days=random.randint(20, 60)),
                hire_date=opening_date + timedelta(days=random.randint(30, 90)) if filled else None,
                recruitment_status="Filled" if filled else "Open",
                source_channel=random.choice(["LinkedIn", "Agency", "Referral", "Company Website"]),
                number_of_applicants=random.randint(10, 250),
                number_shortlisted=random.randint(3, 20),
                number_interviewed=random.randint(1, 8),
                recruitment_cost=Decimal(random.randint(500, 12000)),
                successful_employee=random.choice(employees) if filled else None,
            )
        )

    return records