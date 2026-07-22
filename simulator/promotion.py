"""
Promotion simulation module.

This module generates simulated employee promotion records.

Promotion eligibility and role progression are designed to align with
the JobRole reference data already used by the application.

A valid promotion must satisfy the following conditions:

- the employee must be selected as a promotion candidate;
- the new role must not be the employee's current role;
- the new role must belong to a strictly higher organisational grade;
- same-grade moves are excluded because they represent lateral moves;
- lower-grade moves are excluded because they represent demotions;
- the promoted salary must fall within the salary band of the new role.

The existing JobRole.grade field is used as the authoritative indicator
of organisational seniority.

Current configured hierarchy:

    Associate
        ↓
    Senior Associate
        ↓
    Manager

Salary progression follows this approach:

1. Generate the normal promotion increase of 8% to 20%.
2. Calculate the proposed promoted salary.
3. Compare the proposed salary with the new role's salary band.
4. If below the minimum, use the new role's salary-band minimum.
5. If above the maximum, use the new role's salary-band maximum.
6. Recalculate the actual salary increase and percentage using the
   final salary.

This keeps promotion compensation consistent with the salary bands
already defined in the JobRole reference data.

The generated data supports downstream:
- People Analytics;
- career progression analysis;
- compensation analytics;
- workforce planning;
- promotion modelling;
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
# Ensures that simulation results remain reproducible when the same
# configured seed is used.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Organisational grade hierarchy
#
# These values correspond directly to the grades defined in the
# application's JobRole reference data.
#
# Higher numbers represent more senior organisational grades.
# -------------------------------------------------------------------

GRADE_SENIORITY = {
    "Associate": 1,
    "Senior Associate": 2,
    "Manager": 3,
}


# -------------------------------------------------------------------
# Promotion reason templates
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


# -------------------------------------------------------------------
# Role seniority
# -------------------------------------------------------------------

def get_role_seniority(role: JobRole) -> int:
    """
    Return the organisational seniority level for a JobRole.

    The existing JobRole.grade field is used rather than attempting
    to infer seniority from keywords in the role title.

    Raises:
        ValueError:
            If the role has no recognised grade.
    """

    grade = role.grade.strip() if role.grade else None

    if grade not in GRADE_SENIORITY:
        raise ValueError(
            f"Unknown job grade '{grade}' "
            f"for role '{role.role_name}'. "
            f"Expected one of: {', '.join(GRADE_SENIORITY.keys())}."
        )

    return GRADE_SENIORITY[grade]


# -------------------------------------------------------------------
# Current role lookup
# -------------------------------------------------------------------

def get_employee_current_role(
    employee: Employee,
    job_roles: list[JobRole],
) -> JobRole | None:
    """
    Resolve the employee's current JobRole from employee.role_id.
    """

    return next(
        (
            role
            for role in job_roles
            if role.role_id == employee.role_id
        ),
        None,
    )


# -------------------------------------------------------------------
# Promotion role selection
# -------------------------------------------------------------------

def select_promotion_role(
    employee: Employee,
    job_roles: list[JobRole],
) -> JobRole | None:
    """
    Select a valid higher-grade role for an employee.

    Only roles with a strictly higher organisational grade are
    considered valid promotion destinations.

    This prevents:

    - same-role movements;
    - lateral movements;
    - demotions.
    """

    current_role = get_employee_current_role(
        employee=employee,
        job_roles=job_roles,
    )

    if current_role is None:
        return None

    current_seniority = get_role_seniority(current_role)

    eligible_roles = [
        role
        for role in job_roles
        if (
            role.role_id != employee.role_id
            and get_role_seniority(role) > current_seniority
        )
    ]

    if not eligible_roles:
        return None

    return random.choice(eligible_roles)


# -------------------------------------------------------------------
# Salary calculation
# -------------------------------------------------------------------

def calculate_promoted_salary(
    old_salary: Decimal,
    new_role: JobRole,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Calculate the employee's new salary following promotion.

    The existing simulator behaviour of generating an 8% to 20%
    promotion increase is retained.

    However, the resulting salary is constrained to the salary band
    belonging to the employee's new role.

    Rules:

    1. Generate an initial promotion increase between 8% and 20%.
    2. Calculate the proposed salary.
    3. If proposed salary is below the new role minimum, use the
       salary-band minimum.
    4. If proposed salary exceeds the new role maximum, use the
       salary-band maximum.
    5. Otherwise, retain the proposed salary.
    6. Recalculate the actual increase amount and percentage from
       the final salary.

    Args:
        old_salary:
            Employee's annual salary before promotion.

        new_role:
            JobRole into which the employee is being promoted.

    Returns:
        tuple containing:

        - final promoted salary;
        - actual salary increase amount;
        - actual salary increase percentage.
    """

    # ---------------------------------------------------------------
    # Read salary boundaries directly from the destination JobRole.
    # ---------------------------------------------------------------

    salary_band_min = Decimal(str(new_role.salary_band_min))
    salary_band_max = Decimal(str(new_role.salary_band_max))

    # ---------------------------------------------------------------
    # Validate the configured salary band.
    #
    # This protects the simulator against malformed reference data.
    # ---------------------------------------------------------------

    if salary_band_min > salary_band_max:
        raise ValueError(
            f"Invalid salary band for role '{new_role.role_name}': "
            f"minimum {salary_band_min} exceeds maximum {salary_band_max}."
        )

    # ---------------------------------------------------------------
    # Preserve the existing random promotion increase of 8% to 20%.
    # ---------------------------------------------------------------

    increase_rate = Decimal(
        str(
            round(
                random.uniform(0.08, 0.20),
                2,
            )
        )
    )

    # ---------------------------------------------------------------
    # Calculate salary before applying the new role's salary band.
    # ---------------------------------------------------------------

    proposed_salary = (
        old_salary
        * (Decimal("1.00") + increase_rate)
    )

    # ---------------------------------------------------------------
    # Constrain the proposed salary to the destination salary band.
    #
    # Example:
    #
    # Old salary:        £42,000
    # Proposed salary:   £47,040
    # New role minimum:  £50,000
    #
    # Final salary:      £50,000
    #
    # The employee therefore enters the new role at its minimum
    # salary rather than being paid below the approved range.
    # ---------------------------------------------------------------

    if proposed_salary < salary_band_min:
        final_salary = salary_band_min

    elif proposed_salary > salary_band_max:
        final_salary = salary_band_max

    else:
        final_salary = proposed_salary

    # ---------------------------------------------------------------
    # Round the final salary before calculating the stored increase.
    # ---------------------------------------------------------------

    final_salary = final_salary.quantize(
        Decimal("0.01")
    )

    # ---------------------------------------------------------------
    # Recalculate the ACTUAL monetary increase.
    #
    # This is important because applying the salary-band minimum or
    # maximum may change the increase from the original random rate.
    # ---------------------------------------------------------------

    salary_increase_amount = (
        final_salary - old_salary
    ).quantize(
        Decimal("0.01")
    )

    # ---------------------------------------------------------------
    # Calculate the ACTUAL percentage increase from the final salary.
    # ---------------------------------------------------------------

    salary_increase_percent = (
        (
            salary_increase_amount
            / old_salary
        )
        * Decimal("100")
    ).quantize(
        Decimal("0.01")
    )

    return (
        final_salary,
        salary_increase_amount,
        salary_increase_percent,
    )


# -------------------------------------------------------------------
# Promotion reason
# -------------------------------------------------------------------

def generate_promotion_reason(
    salary_increase_percent: Decimal,
    new_role: JobRole,
) -> str:
    """
    Generate a contextual promotion reason.

    The promotion reason considers:

    - the actual salary increase following salary-band adjustment;
    - whether the destination role is Manager grade.
    """

    is_manager_grade = new_role.grade == "Manager"

    # Manager-grade promotions may receive leadership-focused reasons.
    if is_manager_grade and random.random() < 0.60:
        return random.choice(
            PROMOTION_REASONS["leadership"]
        )

    # Significant salary increase.
    if salary_increase_percent >= Decimal("15.00"):
        return random.choice(
            PROMOTION_REASONS["high_increase"]
        )

    # Moderate salary increase.
    if salary_increase_percent >= Decimal("10.00"):
        return random.choice(
            PROMOTION_REASONS["moderate_increase"]
        )

    # Lower-end salary increase.
    return random.choice(
        PROMOTION_REASONS["career_progression"]
    )


# -------------------------------------------------------------------
# Promotion generation
# -------------------------------------------------------------------

def generate_promotions(
    employees: list[Employee],
    job_roles: list[JobRole],
) -> list[Promotion]:
    """
    Generate simulated employee promotion records.

    Existing promotion-candidate behaviour is preserved:

    - managers are excluded;
    - approximately 8% of remaining employees are selected;
    - employees must move to a strictly higher grade;
    - promoted salaries must fall within the destination role's
      configured salary band.
    """

    records: list[Promotion] = []

    # ---------------------------------------------------------------
    # Preserve the existing 8% promotion-candidate selection logic.
    # ---------------------------------------------------------------

    eligible = [
        employee
        for employee in employees
        if (
            not employee.is_manager
            and random.random() < 0.08
        )
    ]

    for employee in eligible:

        # -----------------------------------------------------------
        # Select a valid higher-grade role.
        # -----------------------------------------------------------

        new_role = select_promotion_role(
            employee=employee,
            job_roles=job_roles,
        )

        # Skip employees for whom no valid higher-grade role exists.
        if new_role is None:
            continue

        # -----------------------------------------------------------
        # Employee's salary immediately before promotion.
        # -----------------------------------------------------------

        old_salary = Decimal(
            str(employee.annual_salary)
        ).quantize(
            Decimal("0.01")
        )

        # -----------------------------------------------------------
        # Calculate the promoted salary.
        #
        # The returned salary is guaranteed to fall within the
        # destination JobRole's salary band.
        # -----------------------------------------------------------

        (
            new_salary,
            salary_increase_amount,
            salary_increase_percent,
        ) = calculate_promoted_salary(
            old_salary=old_salary,
            new_role=new_role,
        )

        # -----------------------------------------------------------
        # Generate the promotion reason using the ACTUAL increase
        # after salary-band adjustment.
        # -----------------------------------------------------------

        promotion_reason = generate_promotion_reason(
            salary_increase_percent=salary_increase_percent,
            new_role=new_role,
        )

        # -----------------------------------------------------------
        # Create Promotion ORM record.
        # -----------------------------------------------------------

        promotion = Promotion(

            # Employee receiving the promotion.
            employee=employee,

            # Role before promotion.
            old_role_id=employee.role_id,

            # Higher-grade destination role.
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

            # Final salary constrained to the destination salary band.
            new_salary=new_salary,

            # Actual monetary increase after salary-band adjustment.
            salary_increase_amount=salary_increase_amount,

            # Actual percentage increase after salary-band adjustment.
            salary_increase_percent=salary_increase_percent,

            # Context-sensitive promotion reason.
            promotion_reason=promotion_reason,

            # Promotion approved by the employee's manager.
            approved_by_manager=employee.manager,
        )

        records.append(promotion)

    return records
