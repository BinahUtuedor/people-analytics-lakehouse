"""
Promotion simulation module.

This module generates simulated employee promotion records.

The promotion reason is generated dynamically based on context such as:
- salary increase percentage;
- whether the employee is moving into a more senior-looking role;
- employee performance/potential assumptions inferred from the promotion event.

This creates more varied and realistic promotion data for downstream:
- People Analytics;
- promotion trend analysis;
- career progression analysis;
- compensation analytics;
- machine learning.
"""

from __future__ import annotations

import random
from datetime import timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, JobRole, Promotion


# -------------------------------------------------------------------
# Random seed
#
# Ensures reproducibility when the simulation is rerun with the same
# configured random seed.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Promotion reason templates
#
# Reasons are grouped by broad promotion context.
# -------------------------------------------------------------------

PROMOTION_REASONS = {
    "high_increase": [
        (
            "Promoted following sustained high performance and a significant "
            "increase in role scope and responsibility."
        ),
        (
            "Promotion awarded in recognition of exceptional contribution, "
            "expanded responsibilities and readiness for a more senior role."
        ),
        (
            "Employee demonstrated consistently strong results and was promoted "
            "to reflect increased accountability and organisational impact."
        ),
    ],

    "moderate_increase": [
        (
            "Promoted following strong performance and successful delivery "
            "against role expectations."
        ),
        (
            "Promotion reflects continued professional development, reliable "
            "performance and increased responsibility."
        ),
        (
            "Employee progressed into a more senior role after demonstrating "
            "the capability to take on broader responsibilities."
        ),
    ],

    "career_progression": [
        (
            "Promoted as part of planned career progression following successful "
            "development in the current role."
        ),
        (
            "Employee progressed after demonstrating readiness for the next "
            "stage of their career pathway."
        ),
        (
            "Promotion supports the employee's continued development and reflects "
            "increased capability and experience."
        ),
    ],

    "leadership": [
        (
            "Promoted to recognise strong leadership potential and the ability "
            "to take greater ownership of team and business outcomes."
        ),
        (
            "Employee demonstrated readiness for broader leadership "
            "responsibilities and increased decision-making accountability."
        ),
        (
            "Promotion reflects strong leadership capability, collaboration and "
            "consistent contribution to wider team objectives."
        ),
    ],
}


def generate_promotion_reason(
    salary_increase_percent: Decimal,
    new_role: JobRole,
) -> str:
    """
    Generate a promotion reason based on promotion context.

    The salary increase is used as the main signal:

    - >= 15%:
        Significant promotion / major increase in responsibility.

    - 10% to 14.99%:
        Strong performance and broader responsibilities.

    - below 10%:
        Career progression and development.

    A leadership-oriented reason may also be selected where the target
    role name appears managerial or senior in nature.

    Args:
        salary_increase_percent:
            Percentage salary increase associated with the promotion.

        new_role:
            Target JobRole ORM object.

    Returns:
        str:
            Contextual promotion reason.
    """

    # -------------------------------------------------------------------
    # Inspect the target role name for leadership/seniority indicators.
    # -------------------------------------------------------------------

    role_name = (
        new_role.role_name.lower()
        if new_role.role_name
        else ""
    )

    leadership_keywords = [
        "manager",
        "lead",
        "head",
        "director",
        "senior",
    ]

    is_leadership_role = any(
        keyword in role_name
        for keyword in leadership_keywords
    )

    # -------------------------------------------------------------------
    # Where the new role appears managerial or senior, occasionally use
    # leadership-specific wording.
    # -------------------------------------------------------------------

    if (
        is_leadership_role
        and random.random() < 0.60
    ):
        return random.choice(
            PROMOTION_REASONS["leadership"]
        )

    # -------------------------------------------------------------------
    # Significant salary increase.
    # -------------------------------------------------------------------

    if salary_increase_percent >= Decimal("15.00"):
        return random.choice(
            PROMOTION_REASONS["high_increase"]
        )

    # -------------------------------------------------------------------
    # Moderate salary increase.
    # -------------------------------------------------------------------

    if salary_increase_percent >= Decimal("10.00"):
        return random.choice(
            PROMOTION_REASONS["moderate_increase"]
        )

    # -------------------------------------------------------------------
    # Lower-end promotion increase.
    #
    # The promotion is framed primarily as career progression.
    # -------------------------------------------------------------------

    return random.choice(
        PROMOTION_REASONS["career_progression"]
    )


def generate_promotions(
    employees: list[Employee],
    job_roles: list[JobRole],
) -> list[Promotion]:
    """
    Generate simulated promotion records.

    Approximately 8% of non-manager employees are selected as promotion
    candidates.

    Args:
        employees:
            List of Employee ORM objects.

        job_roles:
            List of available JobRole ORM objects.

    Returns:
        list[Promotion]:
            Generated promotion records.
    """

    # Container for generated promotion records.
    records: list[Promotion] = []

    # -------------------------------------------------------------------
    # Select employees eligible for promotion.
    #
    # Managers are excluded in the current simulation logic.
    # Approximately 8% of remaining employees are selected.
    # -------------------------------------------------------------------

    eligible = [
        employee
        for employee in employees
        if (
            not employee.is_manager
            and random.random() < 0.08
        )
    ]

    # -------------------------------------------------------------------
    # Generate a promotion record for each selected employee.
    # -------------------------------------------------------------------

    for employee in eligible:

        # Randomly select the employee's new role.
        new_role = random.choice(
            job_roles
        )

        # Current annual salary before promotion.
        old_salary = Decimal(
            employee.annual_salary
        )

        # Generate salary increase between 8% and 20%.
        increase_rate = Decimal(
            str(
                round(
                    random.uniform(
                        0.08,
                        0.20,
                    ),
                    2,
                )
            )
        )

        # Calculate the monetary salary increase.
        increase = (
            old_salary
            * increase_rate
        )

        # Calculate salary following promotion.
        new_salary = (
            old_salary
            + increase
        )

        # Calculate salary increase percentage.
        salary_increase_percent = (
            (
                increase
                / old_salary
            )
            * Decimal(
                "100"
            )
        ).quantize(
            Decimal(
                "0.01"
            )
        )

        # -------------------------------------------------------------------
        # Generate contextual promotion reason.
        #
        # This ensures that not every promotion receives the same reason.
        # -------------------------------------------------------------------

        promotion_reason = (
            generate_promotion_reason(
                salary_increase_percent=salary_increase_percent,
                new_role=new_role,
            )
        )

        # -------------------------------------------------------------------
        # Create Promotion ORM object.
        # -------------------------------------------------------------------

        promotion = Promotion(

            # Employee receiving the promotion.
            employee=employee,

            # Existing role before promotion.
            old_role_id=employee.role_id,

            # New role following promotion.
            new_role=new_role,

            # Simulated promotion date.
            promotion_date=(
                employee.hire_date
                + timedelta(
                    days=random.randint(
                        180,
                        900,
                    )
                )
            ),

            # Salary before promotion.
            old_salary=old_salary,

            # Salary following promotion.
            new_salary=new_salary.quantize(
                Decimal(
                    "0.01"
                )
            ),

            # Monetary value of salary increase.
            salary_increase_amount=(
                increase.quantize(
                    Decimal(
                        "0.01"
                    )
                )
            ),

            # Percentage salary increase.
            salary_increase_percent=(
                salary_increase_percent
            ),

            # Context-sensitive promotion reason.
            promotion_reason=(
                promotion_reason
            ),

            # Promotion approval is attributed to the employee's manager.
            approved_by_manager=(
                employee.manager
            ),
        )

        # Add generated promotion to output collection.
        records.append(
            promotion
        )

    # Return all generated Promotion ORM objects to the
    # simulation orchestrator.
    return records
