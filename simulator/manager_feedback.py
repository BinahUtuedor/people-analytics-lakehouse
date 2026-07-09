"""
Manager feedback simulation module.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, ManagerFeedback

random.seed(DEFAULT_RANDOM_SEED)


def generate_manager_feedback(employees: list[Employee]) -> list[ManagerFeedback]:
    records = []

    current_year = date.today().year

    for employee in employees:
        if employee.manager is None:
            continue

        for year in range(current_year - 2, current_year + 1):
            score = Decimal(str(round(random.uniform(2.5, 5.0), 2)))

            records.append(
                ManagerFeedback(
                    employee=employee,
                    manager=employee.manager,
                    feedback_date=date(year, random.choice([4, 8, 11]), 10),
                    feedback_type=random.choice(["Quarterly Check-in", "Performance", "Development"]),
                    feedback_score=score,
                    comments="Employee is contributing well and developing capability.",
                    strengths="Reliable, collaborative and delivery-focused.",
                    improvement_areas="Continue building confidence and stakeholder influence.",
                    sentiment_label="Positive" if score >= Decimal("3.50") else "Neutral",
                    sentiment_score=Decimal(str(round(random.uniform(0.2, 0.95), 4))),
                )
            )

    return records