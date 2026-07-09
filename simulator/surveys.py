"""
Employee survey simulation module.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, EmployeeSurvey

random.seed(DEFAULT_RANDOM_SEED)

COMMENTS = [
    "I feel supported by my team and manager.",
    "Workload is high but manageable.",
    "Career progression could be clearer.",
    "The culture is inclusive and collaborative.",
    "More flexibility would improve wellbeing.",
]


def survey_score() -> Decimal:
    return Decimal(str(round(random.uniform(2.0, 5.0), 2)))


def generate_employee_surveys(employees: list[Employee]) -> list[EmployeeSurvey]:
    records = []

    current_year = date.today().year

    for employee in employees:
        for year in range(current_year - 2, current_year + 1):
            engagement = survey_score()

            records.append(
                EmployeeSurvey(
                    employee=employee,
                    survey_date=date(year, random.choice([3, 6, 9, 12]), 15),
                    survey_type="Engagement",
                    engagement_score=engagement,
                    satisfaction_score=survey_score(),
                    wellbeing_score=survey_score(),
                    manager_support_score=survey_score(),
                    work_life_balance_score=survey_score(),
                    career_growth_score=survey_score(),
                    free_text_response=random.choice(COMMENTS),
                    sentiment_label="Positive" if engagement >= Decimal("3.50") else "Neutral",
                    sentiment_score=Decimal(str(round(random.uniform(0.1, 0.95), 4))),
                )
            )

    return records