"""
Recruitment simulation module.

This module generates recruitment campaigns and creates real Employee
records for successfully filled vacancies.

A filled recruitment event therefore increases the employee population
and links Recruitment.successful_employee to the employee created from
that recruitment process.
"""

from __future__ import annotations

import random
import re
from datetime import date, timedelta
from decimal import Decimal

from faker import Faker

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import (
    Department,
    Employee,
    JobRole,
    Location,
    Recruitment,
)


fake = Faker("en_GB")
fake.seed_instance(DEFAULT_RANDOM_SEED)
random.seed(DEFAULT_RANDOM_SEED)


def _split_candidate_name(candidate_name: str) -> tuple[str, str]:
    """Split a generated candidate name into first and last names."""

    parts = candidate_name.strip().split()

    if len(parts) == 1:
        return parts[0], "Employee"

    return parts[0], parts[-1]


def _build_unique_email(
    first_name: str,
    last_name: str,
    vacancy_index: int,
    existing_emails: set[str],
) -> str:
    """Create a deterministic email address that cannot clash with existing employees."""

    def clean(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    base_email = (
        f"{clean(first_name)}.{clean(last_name)}."
        f"rec{vacancy_index:05d}@peopleanalytics.example"
    )

    email = base_email
    suffix = 1

    while email in existing_emails:
        email = (
            f"{clean(first_name)}.{clean(last_name)}."
            f"rec{vacancy_index:05d}.{suffix}@peopleanalytics.example"
        )
        suffix += 1

    existing_emails.add(email)

    return email


def _build_unique_employee_number(
    vacancy_index: int,
    existing_numbers: set[str],
) -> str:
    """Create a unique employee number for a recruitment hire."""

    employee_number = f"REC-{vacancy_index:05d}"
    suffix = 1

    while employee_number in existing_numbers:
        employee_number = f"REC-{vacancy_index:05d}-{suffix}"
        suffix += 1

    existing_numbers.add(employee_number)

    return employee_number


def _select_manager(
    department: Department,
    employees: list[Employee],
) -> Employee:
    """
    Select an active manager for a new hire.

    A manager from the same department is preferred. If one is not
    available, any active manager is used.
    """

    active_managers = [
        employee
        for employee in employees
        if employee.is_manager and employee.is_active
    ]

    if not active_managers:
        raise ValueError(
            "Recruitment cannot create employees because no active managers exist."
        )

    department_managers = [
        manager
        for manager in active_managers
        if manager.department_id == department.department_id
    ]

    if department_managers:
        return random.choice(department_managers)

    return random.choice(active_managers)


def _generate_date_of_birth(hire_date: date) -> date:
    """Generate a realistic adult date of birth for a new employee."""

    age_at_hire = random.randint(21, 60)

    return date(
        hire_date.year - age_at_hire,
        random.randint(1, 12),
        random.randint(1, 28),
    )


def _generate_salary(job_role: JobRole) -> Decimal:
    """Generate starting salary within the destination role salary band."""

    minimum = Decimal(str(job_role.salary_band_min))
    maximum = Decimal(str(job_role.salary_band_max))

    if minimum > maximum:
        raise ValueError(
            f"Invalid salary band for role '{job_role.role_name}': "
            f"{minimum} > {maximum}."
        )

    salary = Decimal(
        str(
            random.randint(
                int(minimum),
                int(maximum),
            )
        )
    )

    return salary.quantize(
        Decimal("0.01")
    )


def _create_recruited_employee(
    *,
    candidate_name: str,
    vacancy_index: int,
    hire_date: date,
    department: Department,
    job_role: JobRole,
    locations: list[Location],
    employees: list[Employee],
    existing_numbers: set[str],
    existing_emails: set[str],
) -> Employee:
    """Create the Employee master record associated with a filled vacancy."""

    first_name, last_name = _split_candidate_name(
        candidate_name
    )

    manager = _select_manager(
        department=department,
        employees=employees,
    )

    location = random.choice(
        locations
    )

    employee = Employee(
        employee_number=_build_unique_employee_number(
            vacancy_index=vacancy_index,
            existing_numbers=existing_numbers,
        ),
        email=_build_unique_email(
            first_name=first_name,
            last_name=last_name,
            vacancy_index=vacancy_index,
            existing_emails=existing_emails,
        ),
        first_name=first_name,
        last_name=last_name,
        gender=random.choice(
            [
                "Female",
                "Male",
                "Non-binary",
                None,
            ]
        ),
        date_of_birth=_generate_date_of_birth(
            hire_date
        ),
        hire_date=hire_date,
        termination_date=None,
        employment_status="Active",
        employment_type="Permanent",
        contract_type="Full-Time",
        annual_salary=_generate_salary(
            job_role
        ),
        currency="GBP",
        department=department,
        job_role=job_role,
        location=location,
        manager=manager,
        # A Manager-grade recruit is represented as a manager in the
        # employee master, but still reports to an existing manager.
        is_manager=(
            job_role.grade == "Manager"
        ),
        is_active=True,
    )

    return employee


def generate_recruitment(
    departments: list[Department],
    job_roles: list[JobRole],
    locations: list[Location],
    employees: list[Employee],
) -> tuple[list[Recruitment], list[Employee]]:
    """
    Generate recruitment campaigns and successful new hires.

    Approximately 8% of the starting workforce is used to determine the
    number of recruitment campaigns, with a minimum of 50 vacancies.

    Filled vacancies:
    - receive a candidate;
    - receive a historical hire date;
    - create a new Employee;
    - link Recruitment.successful_employee to that new Employee.

    Open vacancies do not create employees.

    Returns:
        tuple[list[Recruitment], list[Employee]]:
            Recruitment events and newly recruited employees.
    """

    recruitment_records: list[Recruitment] = []
    new_employees: list[Employee] = []

    vacancy_count = max(
        50,
        int(len(employees) * 0.08),
    )

    today = date.today()
    start = today - timedelta(
        days=365 * HISTORICAL_YEARS
    )

    existing_numbers = {
        employee.employee_number
        for employee in employees
    }

    existing_emails = {
        employee.email
        for employee in employees
    }

    # New recruits are appended to this working population so later
    # recruits can see newly created Manager-grade employees if needed.
    manager_population = list(
        employees
    )

    for index in range(
        1,
        vacancy_count + 1,
    ):
        department = random.choice(
            departments
        )

        job_role = random.choice(
            job_roles
        )

        filled = random.choice(
            [
                True,
                True,
                True,
                False,
            ]
        )

        # Open vacancies can occur at any point in the historical window.
        opening_date = start + timedelta(
            days=random.randint(
                0,
                max(
                    0,
                    (today - start).days,
                ),
            )
        )

        candidate_name = None
        hire_date = None
        successful_employee = None

        if filled:
            # Ensure a filled vacancy has a hire date no later than today.
            hire_delay = random.randint(
                30,
                90,
            )

            latest_opening = today - timedelta(
                days=hire_delay
            )

            if opening_date > latest_opening:
                opening_date = latest_opening

            hire_date = opening_date + timedelta(
                days=hire_delay
            )

            candidate_name = fake.name()

            successful_employee = _create_recruited_employee(
                candidate_name=candidate_name,
                vacancy_index=index,
                hire_date=hire_date,
                department=department,
                job_role=job_role,
                locations=locations,
                employees=manager_population,
                existing_numbers=existing_numbers,
                existing_emails=existing_emails,
            )

            new_employees.append(
                successful_employee
            )

            manager_population.append(
                successful_employee
            )

        closing_date = opening_date + timedelta(
            days=random.randint(
                20,
                60,
            )
        )

        # A filled campaign should not close after its hire date.
        if (
            filled
            and hire_date is not None
            and closing_date > hire_date
        ):
            closing_date = hire_date

        recruitment_records.append(
            Recruitment(
                department=department,
                job_role=job_role,
                vacancy_reference=f"VAC-{index:05d}",
                candidate_name=candidate_name,
                opening_date=opening_date,
                closing_date=closing_date,
                hire_date=hire_date,
                recruitment_status=(
                    "Filled"
                    if filled
                    else "Open"
                ),
                source_channel=random.choice(
                    [
                        "LinkedIn",
                        "Agency",
                        "Referral",
                        "Company Website",
                    ]
                ),
                number_of_applicants=random.randint(
                    10,
                    250,
                ),
                number_shortlisted=random.randint(
                    3,
                    20,
                ),
                number_interviewed=random.randint(
                    1,
                    8,
                ),
                recruitment_cost=Decimal(
                    random.randint(
                        500,
                        12000,
                    )
                ),
                successful_employee=(
                    successful_employee
                ),
            )
        )

    return (
        recruitment_records,
        new_employees,
    )