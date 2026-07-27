"""
Training simulation module.

Training records are generated only inside each employee's employment
window.

Training categories are supplied from PostgreSQL reference tables seeded
from YAML reference data.

Completed training:
- starts during employment;
- completion_date cannot exceed termination_date/today.

In-progress training:
- starts during employment;
- retains completion_date=None.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import (
    Employee,
    Training,
    TrainingCategory,
)
from simulator.effective_dates import employment_end_date


random.seed(DEFAULT_RANDOM_SEED)


COURSES = [
    (
        "Data Protection Essentials",
        "Compliance",
    ),
    (
        "Leadership Fundamentals",
        "Leadership",
    ),
    (
        "Advanced Excel",
        "Technical",
    ),
    (
        "Power BI Reporting",
        "Technical",
    ),
    (
        "Cloud Fundamentals",
        "Technical",
    ),
    (
        "Inclusive Leadership",
        "Leadership",
    ),
    (
        "Health and Safety",
        "Compliance",
    ),
    (
        "Project Management Basics",
        "Professional Development",
    ),
]


def get_training_window(
    employee: Employee,
) -> tuple[date, date] | None:
    """
    Return the valid training date range for an employee.

    The range is bounded by:
    - HISTORICAL_YEARS;
    - hire_date;
    - termination_date;
    - today.
    """

    historical_start = (
        date.today()
        - timedelta(
            days=365
            * HISTORICAL_YEARS
        )
    )

    start = max(
        employee.hire_date,
        historical_start,
    )

    end = min(
        employment_end_date(
            employee
        ),
        date.today(),
    )

    if start > end:
        return None

    return (
        start,
        end,
    )


def random_training_date(
    employee: Employee,
) -> date | None:
    """Generate a training start date inside the employment window."""

    training_window = (
        get_training_window(
            employee
        )
    )

    if training_window is None:
        return None

    start, end = (
        training_window
    )

    return (
        start
        + timedelta(
            days=random.randint(
                0,
                (
                    end
                    - start
                ).days,
            )
        )
    )


def get_training_category_name(
    category_name: str,
    training_categories: list[TrainingCategory],
) -> str:
    """
    Resolve a course category against the governed reference dataset.

    Course definitions remain local to this simulator for now.
    Only the category vocabulary is reference-data driven.
    """

    for category in training_categories:
        if (
            category.training_category_name
            == category_name
        ):
            return (
                category
                .training_category_name
            )

    raise ValueError(
        "Training course references unknown "
        f"training category: '{category_name}'."
    )


def generate_training(
    employees: list[Employee],
    training_categories: list[TrainingCategory],
) -> list[Training]:
    """
    Generate training records without constructing records outside the
    employee's valid employment period.

    Training categories are resolved against centrally governed
    PostgreSQL reference data.
    """

    if not training_categories:
        raise ValueError(
            "Training simulation requires training-category "
            "reference data. "
            "Run python -m database.seed first."
        )

    records: list[Training] = []

    for employee in employees:

        training_window = (
            get_training_window(
                employee
            )
        )

        if training_window is None:
            continue

        _, valid_end = (
            training_window
        )

        training_count = random.randint(
            1,
            4,
        )

        for _ in range(
            training_count
        ):
            (
                course_name,
                category_name,
            ) = random.choice(
                COURSES
            )

            category = (
                get_training_category_name(
                    category_name=category_name,
                    training_categories=(
                        training_categories
                    ),
                )
            )

            start_date = (
                random_training_date(
                    employee
                )
            )

            if start_date is None:
                continue

            completed = random.choice(
                [
                    True,
                    True,
                    True,
                    False,
                ]
            )

            completion_date = None

            if completed:

                proposed_completion = (
                    start_date
                    + timedelta(
                        days=random.randint(
                            1,
                            30,
                        )
                    )
                )

                completion_date = min(
                    proposed_completion,
                    valid_end,
                )

                if (
                    completion_date
                    <= start_date
                ):
                    completed = False
                    completion_date = None

            records.append(
                Training(
                    employee=employee,
                    course_name=course_name,

                    # Still stored as the existing string field.
                    course_category=category,

                    provider=random.choice(
                        [
                            "Internal Academy",
                            "LinkedIn Learning",
                            "Coursera",
                            "Udemy",
                        ]
                    ),
                    start_date=start_date,
                    completion_date=completion_date,
                    completion_status=(
                        "Completed"
                        if completed
                        else "In Progress"
                    ),
                    score=(
                        Decimal(
                            random.randint(
                                60,
                                100,
                            )
                        )
                        if completed
                        else None
                    ),
                    training_hours=Decimal(
                        random.randint(
                            2,
                            40,
                        )
                    ),
                    cost=Decimal(
                        random.randint(
                            0,
                            2500,
                        )
                    ),
                    certification_awarded=(
                        random.choice(
                            [
                                True,
                                False,
                            ]
                        )
                        if completed
                        else False
                    ),
                )
            )

    return records
