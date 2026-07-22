"""
Employee survey simulation module.

This module generates employee engagement survey records across a
three-year period.

The survey dimensions are intentionally correlated so that related
employee-experience measures tend to move together rather than being
generated independently.

Examples of expected relationships:

- engagement and satisfaction generally move together;
- manager support influences engagement and satisfaction;
- wellbeing and work-life balance are strongly related;
- career growth contributes to engagement and satisfaction;
- individual dimensions still contain controlled random variation.

This produces more realistic synthetic survey data for downstream:
- employee engagement analysis;
- attrition modelling;
- wellbeing analysis;
- manager effectiveness analysis;
- workforce sentiment analysis;
- machine learning.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, EmployeeSurvey


# -------------------------------------------------------------------
# Random seed
#
# Ensures reproducible survey data when the same configured seed is
# used.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Survey response comments.
#
# These comments are selected according to the overall employee survey
# experience rather than being completely random.
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Score boundaries
#
# Survey scores remain within the original 2.00 to 5.00 range.
# -------------------------------------------------------------------

MIN_SURVEY_SCORE = Decimal("2.00")
MAX_SURVEY_SCORE = Decimal("5.00")


def clamp_score(value: float | Decimal) -> Decimal:
    """
    Constrain a survey score to the valid 2.00 to 5.00 range.

    Args:
        value:
            Proposed survey score.

    Returns:
        Decimal:
            Score rounded to two decimal places and constrained to
            the configured survey scale.
    """

    decimal_value = Decimal(
        str(value)
    )

    if decimal_value < MIN_SURVEY_SCORE:
        decimal_value = MIN_SURVEY_SCORE

    elif decimal_value > MAX_SURVEY_SCORE:
        decimal_value = MAX_SURVEY_SCORE

    return decimal_value.quantize(
        Decimal("0.01")
    )


def generate_base_experience_score() -> Decimal:
    """
    Generate the employee's underlying survey experience level.

    This latent score acts as the common factor behind the individual
    survey dimensions.

    Using a shared base prevents strongly related dimensions from
    behaving as if they were completely independent.

    Returns:
        Decimal:
            Base employee experience score between 2.00 and 5.00.
    """

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
    """
    Generate a score correlated with an underlying base score.

    A relatively small amount of random noise is added to the shared
    employee-experience score.

    This allows related survey dimensions to move in broadly the same
    direction without becoming identical.

    Args:
        base_score:
            Underlying employee experience score.

        variation:
            Maximum positive or negative random adjustment.

    Returns:
        Decimal:
            Correlated survey score between 2.00 and 5.00.
    """

    adjustment = random.uniform(
        -variation,
        variation,
    )

    return clamp_score(
        float(base_score)
        + adjustment
    )


def generate_survey_scores() -> dict[str, Decimal]:
    """
    Generate a coherent set of related employee survey scores.

    Relationships modelled
    ----------------------

    Manager support:
        Generated around the overall employee experience.

    Work-life balance:
        Generated around the employee experience with independent
        variation.

    Wellbeing:
        Strongly influenced by work-life balance.

    Career growth:
        Related to employee experience, but allowed greater variation
        because progression opportunities can differ independently.

    Satisfaction:
        Influenced by:
        - underlying experience;
        - manager support;
        - work-life balance;
        - career growth.

    Engagement:
        Influenced by:
        - satisfaction;
        - manager support;
        - career growth.

    This produces realistic correlation while retaining enough noise
    for analytical and machine-learning use.

    Returns:
        dict[str, Decimal]:
            Generated survey dimensions.
    """

    # -------------------------------------------------------------------
    # Shared latent employee-experience factor.
    # -------------------------------------------------------------------

    base_experience = generate_base_experience_score()

    # -------------------------------------------------------------------
    # Manager support
    #
    # Manager support generally reflects the employee's overall
    # workplace experience but retains individual variation.
    # -------------------------------------------------------------------

    manager_support = correlated_score(
        base_score=base_experience,
        variation=0.50,
    )

    # -------------------------------------------------------------------
    # Work-life balance
    #
    # Closely related to overall experience but can vary independently.
    # -------------------------------------------------------------------

    work_life_balance = correlated_score(
        base_score=base_experience,
        variation=0.55,
    )

    # -------------------------------------------------------------------
    # Wellbeing
    #
    # Strongly related to work-life balance.
    #
    # 70% of the value is driven by work-life balance and 30% by the
    # underlying employee experience, followed by a small amount of
    # noise.
    # -------------------------------------------------------------------

    wellbeing_base = (
        float(work_life_balance) * 0.70
        + float(base_experience) * 0.30
    )

    wellbeing = clamp_score(
        wellbeing_base
        + random.uniform(
            -0.30,
            0.30,
        )
    )

    # -------------------------------------------------------------------
    # Career growth
    #
    # Career development is related to general employee experience,
    # but receives more variation because employees can enjoy their
    # workplace while still feeling progression opportunities are weak.
    # -------------------------------------------------------------------

    career_growth = correlated_score(
        base_score=base_experience,
        variation=0.75,
    )

    # -------------------------------------------------------------------
    # Satisfaction
    #
    # Satisfaction is derived from several related experience factors.
    #
    # Weighting:
    #
    # 40% underlying experience
    # 25% manager support
    # 20% work-life balance
    # 15% career growth
    # -------------------------------------------------------------------

    satisfaction_base = (
        float(base_experience) * 0.40
        + float(manager_support) * 0.25
        + float(work_life_balance) * 0.20
        + float(career_growth) * 0.15
    )

    satisfaction = clamp_score(
        satisfaction_base
        + random.uniform(
            -0.25,
            0.25,
        )
    )

    # -------------------------------------------------------------------
    # Engagement
    #
    # Engagement is primarily influenced by satisfaction, manager
    # support and perceived career opportunity.
    #
    # Weighting:
    #
    # 50% satisfaction
    # 30% manager support
    # 20% career growth
    # -------------------------------------------------------------------

    engagement_base = (
        float(satisfaction) * 0.50
        + float(manager_support) * 0.30
        + float(career_growth) * 0.20
    )

    engagement = clamp_score(
        engagement_base
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
    """
    Generate a sentiment score aligned with employee engagement.

    Higher engagement produces stronger positive sentiment.

    The sentiment score remains within the original approximate
    0.10 to 0.95 range.

    Args:
        engagement_score:
            Employee's engagement score.

    Returns:
        Decimal:
            Sentiment score rounded to four decimal places.
    """

    # Convert the 2.00-5.00 engagement scale approximately onto the
    # existing 0.10-0.95 sentiment range.
    normalised = (
        (
            float(engagement_score)
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

    # Keep score inside the expected range.
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
    """
    Select free-text feedback consistent with the survey scores.

    Specific weaker dimensions are prioritised so that comments provide
    useful context rather than being unrelated to the numeric responses.

    Args:
        engagement_score:
            Employee engagement rating.

        satisfaction_score:
            Employee satisfaction rating.

        wellbeing_score:
            Employee wellbeing rating.

        manager_support_score:
            Employee perception of manager support.

        work_life_balance_score:
            Employee work-life balance rating.

        career_growth_score:
            Employee career development rating.

    Returns:
        str:
            Context-sensitive employee survey comment.
    """

    # -------------------------------------------------------------------
    # Career progression concern.
    # -------------------------------------------------------------------

    if career_growth_score < Decimal("3.00"):
        return random.choice(
            [
                "Career progression could be clearer.",
                "More opportunities for development and progression would improve my experience.",
            ]
        )

    # -------------------------------------------------------------------
    # Manager-support concern.
    # -------------------------------------------------------------------

    if manager_support_score < Decimal("3.00"):
        return random.choice(
            [
                "I would benefit from more regular support and feedback from my manager.",
                "More consistent management support would improve my experience at work.",
            ]
        )

    # -------------------------------------------------------------------
    # Wellbeing/work-life-balance concern.
    # -------------------------------------------------------------------

    if (
        wellbeing_score < Decimal("3.00")
        or work_life_balance_score < Decimal("3.00")
    ):
        return random.choice(
            [
                "More flexibility would improve wellbeing.",
                "Workload pressures sometimes affect my work-life balance.",
            ]
        )

    # -------------------------------------------------------------------
    # Strong overall employee experience.
    # -------------------------------------------------------------------

    if (
        engagement_score >= Decimal("4.00")
        and satisfaction_score >= Decimal("4.00")
    ):
        return random.choice(
            POSITIVE_COMMENTS
        )

    # -------------------------------------------------------------------
    # Moderate / mixed experience.
    # -------------------------------------------------------------------

    if engagement_score >= Decimal("3.25"):
        return random.choice(
            NEUTRAL_COMMENTS
        )

    # -------------------------------------------------------------------
    # Lower engagement where no single specific issue dominates.
    # -------------------------------------------------------------------

    return random.choice(
        DEVELOPMENT_COMMENTS
    )


def generate_employee_surveys(
    employees: list[Employee],
) -> list[EmployeeSurvey]:
    """
    Generate employee engagement surveys across three years.

    Existing behaviour is preserved:

    - every employee receives one survey per year;
    - three years of surveys are generated;
    - survey months remain March, June, September or December;
    - survey type remains "Engagement".

    The individual survey dimensions are now correlated rather than
    independently random.

    Args:
        employees:
            Employee ORM objects.

    Returns:
        list[EmployeeSurvey]:
            Generated employee survey records.
    """

    records: list[EmployeeSurvey] = []

    current_year = date.today().year

    for employee in employees:

        # ---------------------------------------------------------------
        # Generate one annual survey for each of the last three years.
        # ---------------------------------------------------------------

        for year in range(
            current_year - 2,
            current_year + 1,
        ):

            # -----------------------------------------------------------
            # Generate a coherent set of correlated employee-experience
            # scores.
            # -----------------------------------------------------------

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

            # -----------------------------------------------------------
            # Preserve the existing sentiment classification threshold.
            # -----------------------------------------------------------

            sentiment_label = (
                "Positive"
                if engagement >= Decimal("3.50")
                else "Neutral"
            )

            # -----------------------------------------------------------
            # Generate sentiment strength that is also related to
            # engagement rather than being completely independent.
            # -----------------------------------------------------------

            sentiment_score = generate_sentiment_score(
                engagement_score=engagement
            )

            # -----------------------------------------------------------
            # Generate context-aware free-text survey feedback.
            # -----------------------------------------------------------

            free_text_response = select_survey_comment(
                engagement_score=engagement,
                satisfaction_score=satisfaction,
                wellbeing_score=wellbeing,
                manager_support_score=manager_support,
                work_life_balance_score=work_life_balance,
                career_growth_score=career_growth,
            )

            # -----------------------------------------------------------
            # Create EmployeeSurvey ORM record.
            # -----------------------------------------------------------

            records.append(
                EmployeeSurvey(

                    employee=employee,

                    survey_date=date(
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
                    ),

                    survey_type="Engagement",

                    engagement_score=engagement,

                    satisfaction_score=satisfaction,

                    wellbeing_score=wellbeing,

                    manager_support_score=manager_support,

                    work_life_balance_score=work_life_balance,

                    career_growth_score=career_growth,

                    free_text_response=free_text_response,

                    sentiment_label=sentiment_label,

                    sentiment_score=sentiment_score,
                )
            )

    return records
