"""
Performance review simulation module.

Performance reviews are generated only when the review date falls
inside the employee's employment window.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, PerformanceReview
from simulator.effective_dates import date_within_employment_window


random.seed(DEFAULT_RANDOM_SEED)


def score() -> Decimal:
    """Generate a performance score between 2.50 and 5.00."""

    return Decimal(
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


def calculate_overall_rating(
    productivity_score: Decimal,
    quality_score: Decimal,
    teamwork_score: Decimal,
    leadership_score: Decimal | None,
) -> Decimal:
    """Calculate overall rating from applicable component ratings."""

    ratings = [
        productivity_score,
        quality_score,
        teamwork_score,
    ]

    if leadership_score is not None:
        ratings.append(
            leadership_score
        )

    return (
        sum(
            ratings,
            Decimal("0.00"),
        )
        / Decimal(
            len(ratings)
        )
    ).quantize(
        Decimal("0.01")
    )


STRENGTH_OPTIONS = {
    "productivity": [
        "Consistently delivers work efficiently and meets agreed deadlines.",
        "Demonstrates strong productivity and manages workload effectively.",
        "Maintains a high level of output while responding well to changing priorities.",
        "Shows excellent focus and consistently delivers against key objectives.",
    ],
    "quality": [
        "Produces consistently high-quality work with strong attention to detail.",
        "Demonstrates a strong commitment to accuracy and professional standards.",
        "Maintains high quality standards and identifies issues before they affect delivery.",
        "Produces reliable outputs that require minimal correction or rework.",
    ],
    "teamwork": [
        "Works collaboratively with colleagues and contributes positively to team objectives.",
        "Builds strong working relationships and supports colleagues effectively.",
        "Demonstrates a collaborative approach and contributes to a positive team environment.",
        "Communicates well with colleagues and actively supports shared priorities.",
    ],
    "leadership": [
        "Demonstrates effective leadership and provides clear direction to colleagues.",
        "Supports team development and creates a positive environment for delivery.",
        "Demonstrates strong judgement and takes ownership of team outcomes.",
        "Provides effective leadership and contributes positively to wider organisational priorities.",
    ],
    "balanced": [
        "Consistently contributes to team objectives and demonstrates reliable performance across key areas.",
        "Demonstrates well-rounded performance and contributes positively across multiple responsibilities.",
        "Maintains consistent performance across delivery, quality and collaboration.",
        "Shows a balanced range of capabilities and contributes effectively to organisational objectives.",
    ],
}


DEVELOPMENT_OPTIONS = {
    "productivity": [
        "Continue improving prioritisation and time management when managing competing demands.",
        "Focus on increasing efficiency while maintaining the required quality standards.",
        "Develop more structured approaches to workload planning and prioritisation.",
        "Continue improving delivery speed when working on complex or competing priorities.",
    ],
    "quality": [
        "Continue strengthening attention to detail and applying consistent quality checks.",
        "Focus on improving accuracy and reducing the need for rework.",
        "Develop a more structured quality-assurance approach before completing deliverables.",
        "Continue improving consistency and accuracy across complex pieces of work.",
    ],
    "teamwork": [
        "Continue developing collaboration skills and proactively engage colleagues across the wider team.",
        "Build confidence in communicating ideas and contributing during team discussions.",
        "Develop stronger cross-functional relationships and increase knowledge sharing.",
        "Continue strengthening communication and stakeholder engagement skills.",
    ],
    "leadership": [
        "Continue developing strategic leadership capability and confidence when influencing others.",
        "Focus on strengthening delegation, coaching and team-development skills.",
        "Develop greater confidence when making decisions and providing direction to colleagues.",
        "Continue building strategic awareness and leadership capability across wider organisational priorities.",
    ],
    "strategic": [
        "Continue developing strategic and technical capability to support future progression.",
        "Build greater understanding of wider organisational priorities and long-term business objectives.",
        "Seek opportunities to broaden strategic exposure and contribute to cross-functional initiatives.",
        "Continue developing commercial awareness and strategic decision-making capability.",
    ],
}


def generate_review_context(
    productivity_score: Decimal,
    quality_score: Decimal,
    teamwork_score: Decimal,
    leadership_score: Decimal | None,
) -> tuple[str, str]:
    """Generate strengths and development areas from component ratings."""

    scores = {
        "productivity": productivity_score,
        "quality": quality_score,
        "teamwork": teamwork_score,
    }

    if leadership_score is not None:
        scores["leadership"] = leadership_score

    strongest_dimension = max(
        scores,
        key=scores.get,
    )

    weakest_dimension = min(
        scores,
        key=scores.get,
    )

    score_values = list(
        scores.values()
    )

    score_range = (
        max(score_values)
        - min(score_values)
    )

    if score_range <= Decimal("0.30"):
        strengths = random.choice(
            STRENGTH_OPTIONS[
                "balanced"
            ]
        )
    else:
        strengths = random.choice(
            STRENGTH_OPTIONS[
                strongest_dimension
            ]
        )

    if (
        scores[weakest_dimension]
        >= Decimal("4.00")
    ):
        development_areas = random.choice(
            DEVELOPMENT_OPTIONS[
                "strategic"
            ]
        )
    else:
        development_areas = random.choice(
            DEVELOPMENT_OPTIONS[
                weakest_dimension
            ]
        )

    return (
        strengths,
        development_areas,
    )


def generate_performance_reviews(
    employees: list[Employee],
) -> list[PerformanceReview]:
    """
    Generate annual reviews only while the employee is employed.

    A review object is never constructed for a date before hire or
    after termination, eliminating transient SQLAlchemy relationship
    warnings caused by post-generation filtering.
    """

    records: list[PerformanceReview] = []
    current_year = date.today().year

    for employee in employees:
        for year in range(
            current_year - 2,
            current_year + 1,
        ):
            review_date = date(
                year,
                12,
                15,
            )

            # Effective-date check happens BEFORE ORM construction.
            if not date_within_employment_window(
                employee,
                review_date,
            ):
                continue

            productivity_score = score()
            quality_score = score()
            teamwork_score = score()

            leadership_score = (
                score()
                if employee.is_manager
                else None
            )

            overall = calculate_overall_rating(
                productivity_score=productivity_score,
                quality_score=quality_score,
                teamwork_score=teamwork_score,
                leadership_score=leadership_score,
            )

            (
                strengths,
                development_areas,
            ) = generate_review_context(
                productivity_score=productivity_score,
                quality_score=quality_score,
                teamwork_score=teamwork_score,
                leadership_score=leadership_score,
            )

            records.append(
                PerformanceReview(
                    employee=employee,
                    reviewer=employee.manager,
                    review_date=review_date,
                    review_period=str(year),
                    overall_rating=overall,
                    productivity_score=productivity_score,
                    quality_score=quality_score,
                    teamwork_score=teamwork_score,
                    leadership_score=leadership_score,
                    strengths=strengths,
                    development_areas=development_areas,
                    promotion_recommended=(
                        overall
                        >= Decimal("4.50")
                    ),
                    retention_risk=random.choice(
                        [
                            "Low",
                            "Low",
                            "Medium",
                            "High",
                        ]
                    ),
                )
            )

    return records