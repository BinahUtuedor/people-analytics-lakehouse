"""
Promotion simulation module.

Generates valid upward career moves and updates Employee current state.

The Promotion table preserves the before/after event, while Employee is
updated to the latest role and salary after the event is created.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, JobRole, Promotion


random.seed(DEFAULT_RANDOM_SEED)


GRADE_SENIORITY = {
    "Associate": 1,
    "Senior Associate": 2,
    "Manager": 3,
}


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
    """Return the seniority level associated with JobRole.grade."""

    grade = (
        role.grade.strip()
        if role.grade
        else None
    )

    if grade not in GRADE_SENIORITY:
        raise ValueError(
            f"Unknown job grade '{grade}' "
            f"for role '{role.role_name}'."
        )

    return GRADE_SENIORITY[
        grade
    ]


def get_employee_current_role(
    employee: Employee,
    job_roles: list[JobRole],
) -> JobRole | None:
    """Resolve the employee's current JobRole."""

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
    """Select a strictly higher-grade destination role."""

    current_role = get_employee_current_role(
        employee=employee,
        job_roles=job_roles,
    )

    if current_role is None:
        return None

    current_seniority = get_role_seniority(
        current_role
    )

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

    if not eligible_roles:
        return None

    return random.choice(
        eligible_roles
    )


def calculate_promoted_salary(
    old_salary: Decimal,
    new_role: JobRole,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Calculate the promoted salary and constrain it to the new role band.
    """

    salary_band_min = Decimal(
        str(new_role.salary_band_min)
    )

    salary_band_max = Decimal(
        str(new_role.salary_band_max)
    )

    if salary_band_min > salary_band_max:
        raise ValueError(
            f"Invalid salary band for role '{new_role.role_name}'."
        )

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

    proposed_salary = (
        old_salary
        * (
            Decimal("1.00")
            + increase_rate
        )
    )

    final_salary = min(
        salary_band_max,
        max(
            salary_band_min,
            proposed_salary,
        ),
    ).quantize(
        Decimal("0.01")
    )

    salary_increase_amount = (
        final_salary
        - old_salary
    ).quantize(
        Decimal("0.01")
    )

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


def generate_promotion_reason(
    salary_increase_percent: Decimal,
    new_role: JobRole,
) -> str:
    """Generate a reason aligned with promotion magnitude and grade."""

    if (
        new_role.grade == "Manager"
        and random.random() < 0.60
    ):
        return random.choice(
            PROMOTION_REASONS[
                "leadership"
            ]
        )

    if (
        salary_increase_percent
        >= Decimal("15.00")
    ):
        return random.choice(
            PROMOTION_REASONS[
                "high_increase"
            ]
        )

    if (
        salary_increase_percent
        >= Decimal("10.00")
    ):
        return random.choice(
            PROMOTION_REASONS[
                "moderate_increase"
            ]
        )

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
    Generate promotion events and update Employee current state.

    Event history is written using the employee's pre-promotion role
    and salary. After the event object is created, Employee is updated
    to the destination role and promoted salary.
    """

    records: list[Promotion] = []

    today = date.today()

    eligible = [
        employee
        for employee in employees
        if (
            employee.is_active
            and not employee.is_manager
            and random.random() < 0.08
        )
    ]

    for employee in eligible:
        new_role = select_promotion_role(
            employee=employee,
            job_roles=job_roles,
        )

        if new_role is None:
            continue

        old_role_id = employee.role_id

        old_salary = Decimal(
            str(employee.annual_salary)
        ).quantize(
            Decimal("0.01")
        )

        (
            new_salary,
            salary_increase_amount,
            salary_increase_percent,
        ) = calculate_promoted_salary(
            old_salary=old_salary,
            new_role=new_role,
        )

        earliest_date = (
            employee.hire_date
            + timedelta(days=180)
        )

        if earliest_date > today:
            continue

        promotion_date = (
            employee.hire_date
            + timedelta(
                days=random.randint(
                    180,
                    min(
                        900,
                        (
                            today
                            - employee.hire_date
                        ).days,
                    ),
                )
            )
        )

        promotion = Promotion(
            employee=employee,
            old_role_id=old_role_id,
            new_role=new_role,
            promotion_date=promotion_date,
            old_salary=old_salary,
            new_salary=new_salary,
            salary_increase_amount=(
                salary_increase_amount
            ),
            salary_increase_percent=(
                salary_increase_percent
            ),
            promotion_reason=(
                generate_promotion_reason(
                    salary_increase_percent=(
                        salary_increase_percent
                    ),
                    new_role=new_role,
                )
            ),
            approved_by_manager=(
                employee.manager
            ),
        )

        records.append(
            promotion
        )

        # ---------------------------------------------------------------
        # Update Employee current state AFTER preserving old values in
        # the Promotion event.
        # ---------------------------------------------------------------

        employee.job_role = new_role
        employee.annual_salary = (
            new_salary
        )

        # Manager-grade roles are represented as managers in the current
        # employee master. Existing managers still report upwards.
        if new_role.grade == "Manager":
            employee.is_manager = True

    return records