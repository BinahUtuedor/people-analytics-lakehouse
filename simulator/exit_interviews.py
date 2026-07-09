"""
Exit interview simulation module.
"""

from __future__ import annotations

import random
from datetime import timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, ExitInterview

random.seed(DEFAULT_RANDOM_SEED)

EXIT_REASONS = [
    "Career Progression",
    "Compensation",
    "Relocation",
    "Workload",
    "Retirement",
    "Personal Reasons",
]


def generate_exit_interviews(employees: list[Employee]) -> list[ExitInterview]:
    records = []

    leavers = [
        employee for employee in employees
        if not employee.is_manager and random.random() < 0.04
    ]

    for employee in leavers:
        termination_date = employee.hire_date + timedelta(days=random.randint(180, 1000))

        records.append(
            ExitInterview(
                employee=employee,
                termination_date=termination_date,
                exit_reason=random.choice(EXIT_REASONS),
                voluntary_exit=random.choice([True, True, True, False]),
                destination_type=random.choice(["Competitor", "Different Industry", "Further Study", "Unknown"]),
                satisfaction_at_exit=Decimal(str(round(random.uniform(1.5, 5.0), 2))),
                likelihood_to_recommend=Decimal(str(round(random.uniform(1.0, 5.0), 2))),
                interview_text="The employee valued the team but cited career and workload factors.",
                key_themes="Career;Workload;Reward",
                sentiment_label=random.choice(["Positive", "Neutral", "Negative"]),
                sentiment_score=Decimal(str(round(random.uniform(0.1, 0.9), 4))),
            )
        )

        employee.is_active = False
        employee.employment_status = "Terminated"
        employee.termination_date = termination_date

    return records