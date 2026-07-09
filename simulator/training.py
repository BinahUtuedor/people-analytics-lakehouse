"""
Training simulation module.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Employee, Training

random.seed(DEFAULT_RANDOM_SEED)

COURSES = [
    ("Data Protection Essentials", "Compliance"),
    ("Leadership Fundamentals", "Leadership"),
    ("Advanced Excel", "Technical"),
    ("Power BI Reporting", "Technical"),
    ("Cloud Fundamentals", "Technical"),
    ("Inclusive Leadership", "Leadership"),
    ("Health and Safety", "Compliance"),
    ("Project Management Basics", "Professional Development"),
]


def random_training_date(employee: Employee) -> date:
    start = max(employee.hire_date, date.today() - timedelta(days=365 * HISTORICAL_YEARS))
    end = date.today()

    return start + timedelta(days=random.randint(0, max(1, (end - start).days)))


def generate_training(employees: list[Employee]) -> list[Training]:
    records = []

    for employee in employees:
        training_count = random.randint(1, 4)

        for _ in range(training_count):
            course_name, category = random.choice(COURSES)
            start_date = random_training_date(employee)
            completed = random.choice([True, True, True, False])

            records.append(
                Training(
                    employee=employee,
                    course_name=course_name,
                    course_category=category,
                    provider=random.choice(["Internal Academy", "LinkedIn Learning", "Coursera", "Udemy"]),
                    start_date=start_date,
                    completion_date=start_date + timedelta(days=random.randint(1, 30)) if completed else None,
                    completion_status="Completed" if completed else "In Progress",
                    score=Decimal(random.randint(60, 100)) if completed else None,
                    training_hours=Decimal(random.randint(2, 40)),
                    cost=Decimal(random.randint(0, 2500)),
                    certification_awarded=random.choice([True, False]) if completed else False,
                )
            )

    return records