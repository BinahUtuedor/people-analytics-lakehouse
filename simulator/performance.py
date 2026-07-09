"""
Performance review simulation module.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, PerformanceReview

random.seed(DEFAULT_RANDOM_SEED)


def score() -> Decimal:
    return Decimal(str(round(random.uniform(2.5, 5.0), 2)))


def generate_performance_reviews(employees: list[Employee]) -> list[PerformanceReview]:
    records = []

    current_year = date.today().year

    for employee in employees:
        for year in range(current_year - 2, current_year + 1):
            overall = score()

            records.append(
                PerformanceReview(
                    employee=employee,
                    reviewer=employee.manager,
                    review_date=date(year, 12, 15),
                    review_period=str(year),
                    overall_rating=overall,
                    productivity_score=score(),
                    quality_score=score(),
                    teamwork_score=score(),
                    leadership_score=score() if employee.is_manager else None,
                    strengths="Consistently contributes to team objectives.",
                    development_areas="Continue developing strategic and technical capability.",
                    promotion_recommended=overall >= Decimal("4.50"),
                    retention_risk=random.choice(["Low", "Low", "Medium", "High"]),
                )
            )

    return records