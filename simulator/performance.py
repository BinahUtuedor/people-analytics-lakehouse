"""
Performance review simulation module.

This module generates annual employee performance reviews.

The qualitative review fields:
- strengths
- development_areas

are generated dynamically based on the employee's simulated performance
scores instead of using the same text for every employee.

This creates more realistic data for downstream:
- performance analytics;
- promotion prediction;
- retention analysis;
- NLP and sentiment analysis;
- manager effectiveness analysis.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, PerformanceReview


# -------------------------------------------------------------------
# Random seed
#
# Ensures reproducible simulation results when using the same seed.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


def score() -> Decimal:
    """
    Generate a performance score between 2.50 and 5.00.

    Returns:
        Decimal:
            Simulated performance score rounded to two decimal places.
    """

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


# -------------------------------------------------------------------
# Qualitative review content
#
# These feedback options are mapped to the employee's strongest and
# weakest performance dimensions.
# -------------------------------------------------------------------

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
    """
    Generate context-sensitive strengths and development areas.

    The strongest performance dimension determines the employee's
    main strength.

    The weakest performance dimension determines the primary
    development area.

    Args:
        productivity_score:
            Employee productivity rating.

        quality_score:
            Employee quality rating.

        teamwork_score:
            Employee teamwork rating.

        leadership_score:
            Employee leadership rating.
            This is only populated for managers.

    Returns:
        tuple[str, str]:
            Generated strengths and development areas.
    """

    # -------------------------------------------------------------------
    # Build score dictionary.
    #
    # Leadership is only included when the employee is a manager.
    # -------------------------------------------------------------------

    scores = {
        "productivity": productivity_score,
        "quality": quality_score,
        "teamwork": teamwork_score,
    }

    if leadership_score is not None:
        scores["leadership"] = leadership_score

    # -------------------------------------------------------------------
    # Identify strongest and weakest dimensions.
    # -------------------------------------------------------------------

    strongest_dimension = max(
        scores,
        key=scores.get,
    )

    weakest_dimension = min(
        scores,
        key=scores.get,
    )

    # -------------------------------------------------------------------
    # If all scores are very close together, treat performance as
    # well balanced rather than highlighting one specific strength.
    # -------------------------------------------------------------------

    score_values = list(
        scores.values()
    )

    score_range = max(
        score_values
    ) - min(
        score_values
    )

    if score_range <= Decimal("0.30"):
        strengths = random.choice(
            STRENGTH_OPTIONS["balanced"]
        )

    else:
        strengths = random.choice(
            STRENGTH_OPTIONS[
                strongest_dimension
            ]
        )

    # -------------------------------------------------------------------
    # Generate development area.
    #
    # If the weakest score is still relatively high, provide a broader
    # strategic development recommendation instead of negative feedback.
    # -------------------------------------------------------------------

    if scores[weakest_dimension] >= Decimal("4.00"):
        development_areas = random.choice(
            DEVELOPMENT_OPTIONS["strategic"]
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
    Generate three years of annual performance reviews.

    One review is generated for each employee for:
    - the current year;
    - the previous year;
    - two years before the current year.

    Qualitative review feedback is dynamically generated from the
    employee's performance scores.

    Args:
        employees:
            List of Employee ORM objects.

    Returns:
        list[PerformanceReview]:
            Generated performance review records.
    """

    # Container for all generated performance reviews.
    records: list[PerformanceReview] = []

    # Determine the current calendar year.
    current_year = date.today().year

    # -------------------------------------------------------------------
    # Generate performance reviews for every employee.
    # -------------------------------------------------------------------

    for employee in employees:

        # Generate one review per year across a three-year period.
        for year in range(
            current_year - 2,
            current_year + 1,
        ):

            # -----------------------------------------------------------
            # Generate individual performance metrics first.
            #
            # These scores are reused when generating qualitative
            # strengths and development areas.
            # -----------------------------------------------------------

            overall = score()

            productivity_score = score()

            quality_score = score()

            teamwork_score = score()

            # Leadership score only applies to employees identified
            # as managers.
            leadership_score = (
                score()
                if employee.is_manager
                else None
            )

            # -----------------------------------------------------------
            # Generate context-sensitive qualitative review feedback.
            # -----------------------------------------------------------

            strengths, development_areas = (
                generate_review_context(
                    productivity_score=productivity_score,
                    quality_score=quality_score,
                    teamwork_score=teamwork_score,
                    leadership_score=leadership_score,
                )
            )

            # -----------------------------------------------------------
            # Create PerformanceReview ORM object.
            # -----------------------------------------------------------

            performance_review = PerformanceReview(

                # Employee being reviewed.
                employee=employee,

                # Employee's manager acts as reviewer.
                #
                # The top-level employee may have reviewer=None,
                # which is expected if the ORM allows nullable reviewer.
                reviewer=employee.manager,

                # Annual review date.
                review_date=date(
                    year,
                    12,
                    15,
                ),

                # Review period stored as calendar year.
                review_period=str(
                    year
                ),

                # Overall employee performance rating.
                overall_rating=overall,

                # Individual performance dimensions.
                productivity_score=productivity_score,
                quality_score=quality_score,
                teamwork_score=teamwork_score,
                leadership_score=leadership_score,

                # Dynamically generated qualitative feedback.
                strengths=strengths,

                # Dynamically generated development recommendation.
                development_areas=development_areas,

                # Employees with an overall rating of 4.50 or above
                # are recommended for promotion.
                promotion_recommended=(
                    overall
                    >= Decimal(
                        "4.50"
                    )
                ),

                # Simulated employee retention risk.
                #
                # "Low" appears twice to produce a higher proportion
                # of low-risk employees.
                retention_risk=random.choice(
                    [
                        "Low",
                        "Low",
                        "Medium",
                        "High",
                    ]
                ),
            )

            # Add generated review to output collection.
            records.append(
                performance_review
            )

    # Return all generated PerformanceReview ORM objects
    # to the simulation orchestrator.
    return records
