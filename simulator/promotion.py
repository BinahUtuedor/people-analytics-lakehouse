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
- lower-grade moves are excluded because they represent demotions.

The existing JobRole.grade field is used as the authoritative indicator
of organisational seniority.

Current configured hierarchy:

    Associate
        ↓
    Senior Associate
        ↓
    Manager

The promotion reason is generated dynamically based on:
- salary increase percentage;
- whether the destination role is a Manager-grade role.

This creates more realistic promotion data for downstream:
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
# application's JOB_ROLES seed/reference data.
#
# The numeric values exist only to support comparison.
# A higher number represents a more senior organisational grade.
# -------------------------------------------------------------------

GRADE_SENIORITY = {
    "Associate": 1,
    "Senior Associate": 2,
    "Manager": 3,
}


# -------------------------------------------------------------------
# Promotion reason templates
#
# Promotion reasons are grouped according to the size and context
# of the employee's progression.
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


def get_role_seniority(
    role: JobRole,
) -> int:
    """
    Return the organisational seniority level for a JobRole.

    The role's existing ``grade`` field is used instead of attempting
    to infer seniority from keywords in the role title.

    This is important because roles such as:

        Finance Business Partner

    are Manager-grade roles even though the word "Manager" does not
    appear in the role name.

    Similarly:

        Data Engineer
        Software Engineer

    are Senior Associate roles despite not containing the word
    "Senior" in their titles.

    Args:
        role:
            JobRole ORM object.

    Returns:
        int:
            Numeric seniority level corresponding to the role's grade.

    Raises:
        ValueError:
            If the JobRole has no grade or contains a grade that has
            not been configured in GRADE_SENIORITY.
    """

    # ---------------------------------------------------------------
    # Safely normalise the grade value.
    # ---------------------------------------------------------------

    grade = (
        role.grade.strip()
        if role.grade
        else None
    )

    # ---------------------------------------------------------------
    # Do not silently guess the seniority of an unknown grade.
    #
    # Failing explicitly is safer because an unknown grade could
    # otherwise cause a promotion to become an accidental lateral
    # move or demotion.
    # ---------------------------------------------------------------

    if grade not in GRADE_SENIORITY:
        raise ValueError(
            f"Unknown job grade '{grade}' "
            f"for role '{role.role_name}'. "
            f"Expected one of: "
            f"{', '.join(GRADE_SENIORITY.keys())}."
        )

    return GRADE_SENIORITY[
        grade
    ]


def get_employee_current_role(
    employee: Employee,
    job_roles: list[JobRole],
) -> JobRole | None:
    """
    Resolve an employee's current JobRole object.

    The Employee model already stores ``role_id``. The supplied list
    of JobRole objects is therefore searched for the matching role.

    This keeps the implementation compatible with the existing
    simulator and avoids requiring any new ORM relationships.

    Args:
        employee:
            Employee whose current role should be resolved.

        job_roles:
            JobRole records available to the simulator.

    Returns:
        JobRole | None:
            Current role where found, otherwise None.
    """

    return next(
        (
            role
            for role in job_roles
            if role.role_id
            == employee.role_id
        ),
        None,
    )


def select_promotion_role(
    employee: Employee,
    job_roles: list[JobRole],
) -> JobRole | None:
    """
    Select a valid higher-grade role for an employee.

    A role qualifies as a promotion destination only when:

        new grade > current grade

    The following transitions are therefore excluded:

    Same role:
        Data Analyst -> Data Analyst

    Same grade / lateral move:
        Finance Analyst -> HR Advisor
        Project Manager -> Operations Manager

    Demotion:
        Data Engineer -> Data Analyst
        Finance Business Partner -> Finance Analyst

    Valid examples include:

        Data Analyst
            Associate
                ->
        Data Engineer
            Senior Associate

    and:

        Data Engineer
            Senior Associate
                ->
        Senior Data Engineer
            Manager

    Args:
        employee:
            Employee being considered for promotion.

        job_roles:
            Available JobRole ORM objects.

    Returns:
        JobRole | None:
            A randomly selected higher-grade role.

            None is returned when:
            - the employee's current role cannot be resolved; or
            - no higher-grade role exists.
    """

    # ---------------------------------------------------------------
    # Resolve the employee's existing role.
    # ---------------------------------------------------------------

    current_role = get_employee_current_role(
        employee=employee,
        job_roles=job_roles,
    )

    # ---------------------------------------------------------------
    # If the current role cannot be identified, do not create an
    # unreliable promotion record.
    # ---------------------------------------------------------------

    if current_role is None:
        return None

    # ---------------------------------------------------------------
    # Determine current organisational seniority.
    # ---------------------------------------------------------------

    current_seniority = get_role_seniority(
        current_role
    )

    # ---------------------------------------------------------------
    # Keep only roles belonging to a strictly higher grade.
    #
    # The role_id check explicitly prevents the current role from
    # being selected, although a strictly higher grade would normally
    # exclude it anyway.
    # ---------------------------------------------------------------

    eligible_roles = [
        role
        for role in job_roles
        if (
            role.role_id
            != employee.role_id
            and get_role_seniority(
                role
            )
            > current_seniority
        )
    ]

    # ---------------------------------------------------------------
    # Employees already at the highest available grade may have no
    # valid promotion destination.
    #
    # They are skipped instead of generating an artificial promotion.
    # ---------------------------------------------------------------

    if not eligible_roles:
        return None

    # ---------------------------------------------------------------
    # Random selection is retained, but only within the set of roles
    # that genuinely represent upward organisational progression.
    # ---------------------------------------------------------------

    return random.choice(
        eligible_roles
    )


def generate_promotion_reason(
    salary_increase_percent: Decimal,
    new_role: JobRole,
) -> str:
    """
    Generate a contextual promotion reason.

    The existing salary-increase logic is preserved:

        >= 15%
            Significant promotion / expanded responsibilities.

        10% to 14.99%
            Strong performance / broader responsibilities.

        < 10%
            Career progression.

    Manager-grade destination roles may receive leadership-focused
    promotion reasons.

    Unlike the previous implementation, Manager-grade status is
    determined from the formal JobRole.grade field rather than from
    keywords in the role title.

    Args:
        salary_increase_percent:
            Percentage salary increase associated with the promotion.

        new_role:
            Destination JobRole.

    Returns:
        str:
            Context-sensitive promotion reason.
    """

    # ---------------------------------------------------------------
    # The grade field provides a reliable indication of whether this
    # promotion moves the employee into the highest currently defined
    # organisational grade.
    # ---------------------------------------------------------------

    is_manager_grade = (
        new_role.grade == "Manager"
    )

    # ---------------------------------------------------------------
    # Manager-grade promotions have a reasonable probability of
    # receiving a leadership-focused promotion reason.
    #
    # Existing 60% behaviour is retained from the previous simulator.
    # ---------------------------------------------------------------

    if (
        is_manager_grade
        and random.random() < 0.60
    ):
        return random.choice(
            PROMOTION_REASONS[
                "leadership"
            ]
        )

    # ---------------------------------------------------------------
    # Significant salary increase.
    # ---------------------------------------------------------------

    if (
        salary_increase_percent
        >= Decimal("15.00")
    ):
        return random.choice(
            PROMOTION_REASONS[
                "high_increase"
            ]
        )

    # ---------------------------------------------------------------
    # Moderate salary increase.
    # ---------------------------------------------------------------

    if (
        salary_increase_percent
        >= Decimal("10.00")
    ):
        return random.choice(
            PROMOTION_REASONS[
                "moderate_increase"
            ]
        )

    # ---------------------------------------------------------------
    # Lower-end promotion increase.
    # ---------------------------------------------------------------

    return random.choice(
        PROMOTION_REASONS[
            "career_progression"
        ]
    )


def generate_promotions(
    employees: list[Employee],
    job_roles: list[JobRole],
) -> list[Promotion]:
    """
    Generate simulated employee promotion records.

    Existing promotion-candidate behaviour is preserved:

        - managers are excluded;
        - approximately 8% of remaining employees are initially
          selected as promotion candidates.

    A promotion record is then created only when a genuine higher-grade
    role exists.

    Args:
        employees:
            Employee ORM objects.

        job_roles:
            Available JobRole ORM objects.

    Returns:
        list[Promotion]:
            Generated valid promotion records.
    """

    # Container for generated promotion records.
    records: list[Promotion] = []

    # -------------------------------------------------------------------
    # Select initial promotion candidates.
    #
    # This preserves the existing simulator behaviour:
    #
    # - employees currently identified as managers are excluded;
    # - approximately 8% of remaining employees become candidates.
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
    # Generate promotion records.
    # -------------------------------------------------------------------

    for employee in eligible:

        # ---------------------------------------------------------------
        # Select a genuine higher-grade destination role.
        #
        # This replaces the previous:
        #
        #     new_role = random.choice(job_roles)
        #
        # which could result in:
        # - the same role;
        # - a same-grade lateral move;
        # - a lower-grade role.
        # ---------------------------------------------------------------

        new_role = select_promotion_role(
            employee=employee,
            job_roles=job_roles,
        )

        # ---------------------------------------------------------------
        # No valid progression path exists.
        #
        # Skip this candidate rather than generating an invalid
        # promotion record.
        # ---------------------------------------------------------------

        if new_role is None:
            continue

        # ---------------------------------------------------------------
        # Current annual salary before promotion.
        # ---------------------------------------------------------------

        old_salary = Decimal(
            employee.annual_salary
        )

        # ---------------------------------------------------------------
        # Generate salary increase between 8% and 20%.
        #
        # Existing salary logic is intentionally unchanged.
        # ---------------------------------------------------------------

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

        # ---------------------------------------------------------------
        # Calculate monetary increase.
        # ---------------------------------------------------------------

        increase = (
            old_salary
            * increase_rate
        )

        # ---------------------------------------------------------------
        # Calculate salary following promotion.
        # ---------------------------------------------------------------

        new_salary = (
            old_salary
            + increase
        )

        # ---------------------------------------------------------------
        # Calculate salary increase percentage.
        # ---------------------------------------------------------------

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

        # ---------------------------------------------------------------
        # Generate promotion reason from:
        #
        # - salary increase;
        # - destination grade.
        # ---------------------------------------------------------------

        promotion_reason = (
            generate_promotion_reason(
                salary_increase_percent=(
                    salary_increase_percent
                ),
                new_role=new_role,
            )
        )

        # ---------------------------------------------------------------
        # Create Promotion ORM object.
        # ---------------------------------------------------------------

        promotion = Promotion(

            # Employee receiving the promotion.
            employee=employee,

            # Existing role before promotion.
            old_role_id=(
                employee.role_id
            ),

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

            # Salary after promotion.
            new_salary=(
                new_salary.quantize(
                    Decimal(
                        "0.01"
                    )
                )
            ),

            # Monetary salary increase.
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

            # Existing approval behaviour is retained.
            #
            # Promotion approval is attributed to the employee's
            # current manager.
            approved_by_manager=(
                employee.manager
            ),
        )

        # Add valid promotion record to output collection.
        records.append(
            promotion
        )

    # -------------------------------------------------------------------
    # Return generated Promotion ORM objects to the simulator.
    # -------------------------------------------------------------------

    return records
