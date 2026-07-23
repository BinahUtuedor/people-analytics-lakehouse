"""
Manager feedback simulation module.

Manager feedback records are generated only while both the employee and
the feedback date fall within the employee's employment window.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, ManagerFeedback
from simulator.effective_dates import date_within_employment_window


random.seed(DEFAULT_RANDOM_SEED)


FEEDBACK_TYPES = [
    "Quarterly Check-in",
    "Performance",
    "Development",
]


# Keep the existing context-aware content structure concise here.
FEEDBACK_CONTENT = {
    "exceptional": {
        "comments": [
            "The employee continues to make an excellent contribution and consistently exceeds expectations.",
            "The employee demonstrates outstanding performance, ownership and professional capability.",
        ],
        "strengths": [
            "Delivery;Ownership;Leadership",
            "Strategic Thinking;Collaboration;Results",
        ],
        "improvement_areas": [
            "Continue broadening strategic exposure and organisational influence.",
            "Seek opportunities to lead more complex cross-functional initiatives.",
        ],
    },
    "strong": {
        "comments": [
            "The employee continues to deliver reliably and contributes positively to team objectives.",
            "The employee demonstrates good capability and consistent performance across core responsibilities.",
        ],
        "strengths": [
            "Reliability;Teamwork;Delivery",
            "Collaboration;Communication;Accountability",
        ],
        "improvement_areas": [
            "Continue developing stakeholder influence and strategic awareness.",
            "Seek opportunities to take greater ownership of complex work.",
        ],
    },
    "developing": {
        "comments": [
            "The employee is making progress but would benefit from greater consistency in key areas.",
            "The employee continues to develop capability and would benefit from additional support.",
        ],
        "strengths": [
            "Learning Agility;Commitment;Teamwork",
            "Adaptability;Collaboration;Potential",
        ],
        "improvement_areas": [
            "Improve prioritisation, consistency and communication of delivery risks.",
            "Continue building confidence and seek support earlier when required.",
        ],
    },
}


def determine_feedback_context(
    feedback_score: Decimal,
) -> str:
    if (
        feedback_score
        >= Decimal("4.50")
    ):
        return "exceptional"

    if (
        feedback_score
        >= Decimal("3.50")
    ):
        return "strong"

    return "developing"


def generate_sentiment_score(
    sentiment_label: str,
) -> Decimal:
    if sentiment_label == "Positive":
        value = random.uniform(
            0.65,
            0.95,
        )
    else:
        value = random.uniform(
            0.30,
            0.64,
        )

    return Decimal(
        str(
            round(
                value,
                4,
            )
        )
    )


def generate_manager_feedback(
    employees: list[Employee],
) -> list[ManagerFeedback]:
    """
    Generate three years of feedback only for valid employment dates.

    The top-level employee still has no manager and remains excluded.
    """

    records: list[ManagerFeedback] = []
    current_year = date.today().year

    for employee in employees:
        if employee.manager is None:
            continue

        for year in range(
            current_year - 2,
            current_year + 1,
        ):
            feedback_date = date(
                year,
                random.choice(
                    [
                        4,
                        8,
                        11,
                    ]
                ),
                10,
            )

            # Prevent the ORM object from being constructed at all when
            # the feedback date falls outside employment.
            if not date_within_employment_window(
                employee,
                feedback_date,
            ):
                continue

            feedback_score = Decimal(
                str(
                    round(
                        random.uniform(
                            2.5,
                            5.0,
                        ),
                        2,
                    )
                )
            )

            feedback_type = random.choice(
                FEEDBACK_TYPES
            )

            context = determine_feedback_context(
                feedback_score
            )

            content = FEEDBACK_CONTENT[
                context
            ]

            sentiment_label = (
                "Positive"
                if feedback_score
                >= Decimal("3.50")
                else "Neutral"
            )

            records.append(
                ManagerFeedback(
                    employee=employee,
                    manager=employee.manager,
                    feedback_date=feedback_date,
                    feedback_type=feedback_type,
                    feedback_score=feedback_score,
                    comments=random.choice(
                        content[
                            "comments"
                        ]
                    ),
                    strengths=random.choice(
                        content[
                            "strengths"
                        ]
                    ),
                    improvement_areas=random.choice(
                        content[
                            "improvement_areas"
                        ]
                    ),
                    sentiment_label=sentiment_label,
                    sentiment_score=(
                        generate_sentiment_score(
                            sentiment_label
                        )
                    ),
                )
            )

    return records