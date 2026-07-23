"""
Employee survey simulation module.

Survey records are generated only while an employee is actively within
their employment window.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, EmployeeSurvey
from simulator.effective_dates import date_within_employment_window


random.seed(DEFAULT_RANDOM_SEED)


POSITIVE_COMMENTS = [
    "I feel supported by my team and manager.",
    "The culture is inclusive and collaborative.",
    "I feel motivated and positive about my work.",
    "My manager provides good support and regular feedback.",
    "I have a good balance between work demands and personal wellbeing.",
]

NEUTRAL_COMMENTS = [
    "Workload is high but manageable.",
    "Overall the experience is positive, although there are areas that could improve.",
    "The team works well together, but some processes could be more efficient.",
    "I generally feel supported but would welcome more development opportunities.",
    "The role is manageable, although priorities can sometimes compete.",
]

DEVELOPMENT_COMMENTS = [
    "Career progression could be clearer.",
    "More flexibility would improve wellbeing.",
    "Workload pressures sometimes affect my work-life balance.",
    "I would benefit from more regular support and feedback from my manager.",
    "More opportunities for development and progression would improve my experience.",
]


MIN_SURVEY_SCORE = Decimal("2.00")
MAX_SURVEY_SCORE = Decimal("5.00")


def clamp_score(
    value: float | Decimal,
) -> Decimal:
    """Constrain a score to the 2.00-5.00 survey scale."""

    decimal_value = Decimal(
        str(value)
    )

    decimal_value = max(
        MIN_SURVEY_SCORE,
        min(
            MAX_SURVEY_SCORE,
            decimal_value,
        ),
    )

    return decimal_value.quantize(
        Decimal("0.01")
    )


def generate_base_experience_score() -> Decimal:
    return clamp_score(
        random.uniform(
            2.0,
            5.0,
        )
    )


def correlated_score(
    base_score: Decimal,
    variation: float = 0.45,
) -> Decimal:
    return clamp_score(
        float(base_score)
        + random.uniform(
            -variation,
            variation,
        )
    )


def generate_survey_scores() -> dict[str, Decimal]:
    """Generate related employee-experience dimensions."""

    base_experience = (
        generate_base_experience_score()
    )

    manager_support = correlated_score(
        base_experience,
        0.50,
    )

    work_life_balance = correlated_score(
        base_experience,
        0.55,
    )

    wellbeing = clamp_score(
        (
            float(
                work_life_balance
            )
            * 0.70
        )
        + (
            float(
                base_experience
            )
            * 0.30
        )
        + random.uniform(
            -0.30,
            0.30,
        )
    )

    career_growth = correlated_score(
        base_experience,
        0.75,
    )

    satisfaction = clamp_score(
        (
            float(
                base_experience
            )
            * 0.40
        )
        + (
            float(
                manager_support
            )
            * 0.25
        )
        + (
            float(
                work_life_balance
            )
            * 0.20
        )
        + (
            float(
                career_growth
            )
            * 0.15
        )
        + random.uniform(
            -0.25,
            0.25,
        )
    )

    engagement = clamp_score(
        (
            float(
                satisfaction
            )
            * 0.50
        )
        + (
            float(
                manager_support
            )
            * 0.30
        )
        + (
            float(
                career_growth
            )
            * 0.20
        )
        + random.uniform(
            -0.25,
            0.25,
        )
    )

    return {
        "engagement_score": engagement,
        "satisfaction_score": satisfaction,
        "wellbeing_score": wellbeing,
        "manager_support_score": manager_support,
        "work_life_balance_score": work_life_balance,
        "career_growth_score": career_growth,
    }


def generate_sentiment_score(
    engagement_score: Decimal,
) -> Decimal:
    """Generate sentiment strength correlated with engagement."""

    normalised = (
        (
            float(
                engagement_score
            )
            - 2.0
        )
        / 3.0
    )

    sentiment = (
        0.10
        + normalised * 0.85
        + random.uniform(
            -0.08,
            0.08,
        )
    )

    sentiment = max(
        0.10,
        min(
            0.95,
            sentiment,
        ),
    )

    return Decimal(
        str(
            round(
                sentiment,
                4,
            )
        )
    )


def select_survey_comment(
    engagement_score: Decimal,
    satisfaction_score: Decimal,
    wellbeing_score: Decimal,
    manager_support_score: Decimal,
    work_life_balance_score: Decimal,
    career_growth_score: Decimal,
) -> str:
    """Select free-text feedback consistent with the survey dimensions."""

    if (
        career_growth_score
        < Decimal("3.00")
    ):
        return random.choice(
            [
                "Career progression could be clearer.",
                "More opportunities for development and progression would improve my experience.",
            ]
        )

    if (
        manager_support_score
        < Decimal("3.00")
    ):
        return random.choice(
            [
                "I would benefit from more regular support and feedback from my manager.",
                "More consistent management support would improve my experience at work.",
            ]
        )

    if (
        wellbeing_score
        < Decimal("3.00")
        or work_life_balance_score
        < Decimal("3.00")
    ):
        return random.choice(
            [
                "More flexibility would improve wellbeing.",
                "Workload pressures sometimes affect my work-life balance.",
            ]
        )

    if (
        engagement_score
        >= Decimal("4.00")
        and satisfaction_score
        >= Decimal("4.00")
    ):
        return random.choice(
            POSITIVE_COMMENTS
        )

    if (
        engagement_score
        >= Decimal("3.25")
    ):
        return random.choice(
            NEUTRAL_COMMENTS
        )

    return random.choice(
        DEVELOPMENT_COMMENTS
    )


def generate_employee_surveys(
    employees: list[Employee],
) -> list[EmployeeSurvey]:
    """
    Generate annual surveys only for dates inside active employment.
    """

    records: list[EmployeeSurvey] = []
    current_year = date.today().year

    for employee in employees:
        for year in range(
            current_year - 2,
            current_year + 1,
        ):
            # Pick the survey month first, then validate the resulting
            # date before generating any ORM object.
            survey_date = date(
                year,
                random.choice(
                    [
                        3,
                        6,
                        9,
                        12,
                    ]
                ),
                15,
            )

            if not date_within_employment_window(
                employee,
                survey_date,
            ):
                continue

            scores = generate_survey_scores()

            engagement = scores[
                "engagement_score"
            ]

            satisfaction = scores[
                "satisfaction_score"
            ]

            wellbeing = scores[
                "wellbeing_score"
            ]

            manager_support = scores[
                "manager_support_score"
            ]

            work_life_balance = scores[
                "work_life_balance_score"
            ]

            career_growth = scores[
                "career_growth_score"
            ]

            sentiment_label = (
                "Positive"
                if engagement
                >= Decimal("3.50")
                else "Neutral"
            )

            records.append(
                EmployeeSurvey(
                    employee=employee,
                    survey_date=survey_date,
                    survey_type="Engagement",
                    engagement_score=engagement,
                    satisfaction_score=satisfaction,
                    wellbeing_score=wellbeing,
                    manager_support_score=manager_support,
                    work_life_balance_score=work_life_balance,
                    career_growth_score=career_growth,
                    free_text_response=(
                        select_survey_comment(
                            engagement_score=engagement,
                            satisfaction_score=satisfaction,
                            wellbeing_score=wellbeing,
                            manager_support_score=manager_support,
                            work_life_balance_score=work_life_balance,
                            career_growth_score=career_growth,
                        )
                    ),
                    sentiment_label=sentiment_label,
                    sentiment_score=(
                        generate_sentiment_score(
                            engagement
                        )
                    ),
                )
            )

    return records