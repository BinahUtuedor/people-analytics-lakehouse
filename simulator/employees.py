"""
Employee simulation module.

Generates synthetic employee records with a realistic organisational hierarchy.

Hierarchy:
1. CEO                       -> manager_id NULL
2. Executives                -> report to CEO
3. Directors                 -> report to executives
4. Managers                  -> report to directors
5. Individual contributors   -> report to managers
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from faker import Faker

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Department, Employee, JobRole, Location


fake = Faker("en_GB")

Faker.seed(DEFAULT_RANDOM_SEED)
random.seed(DEFAULT_RANDOM_SEED)


EMPLOYMENT_TYPES = [
    "Permanent",
    "Fixed Term",
    "Contractor",
]

GENDERS = [
    "Female",
    "Male",
    "Non-binary",
    "Prefer not to say",
]


def random_date_between(start_date: date, end_date: date) -> date:
    """Return a random date between two dates."""

    delta_days = (end_date - start_date).days

    return start_date + timedelta(
        days=random.randint(0, delta_days)
    )


def generate_employee_number(index: int) -> str:
    """Generate a stable employee number."""

    return f"EMP{index:05d}"


def generate_salary(job_role: JobRole, multiplier: float = 1.0) -> Decimal:
    """
    Generate salary within the selected job role salary band.

    The multiplier allows senior hierarchy levels to earn more.
    """

    salary_min = int(job_role.salary_band_min)
    salary_max = int(job_role.salary_band_max)

    salary = random.randint(salary_min, salary_max)

    salary = salary * multiplier

    return Decimal(salary).quantize(Decimal("0.01"))


def get_random_reference_data(
    departments: list[Department],
    locations: list[Location],
    job_roles: list[JobRole],
) -> tuple[Department, Location, JobRole]:
    """Return random department, location and job role."""

    return (
        random.choice(departments),
        random.choice(locations),
        random.choice(job_roles),
    )


def create_employee(
    index: int,
    departments: list[Department],
    locations: list[Location],
    job_roles: list[JobRole],
    manager: Employee | None,
    is_manager: bool,
    title_prefix: str | None = None,
    salary_multiplier: float = 1.0,
) -> Employee:
    """
    Create one Employee ORM object.

    Important:
    Assign the manager relationship directly, not manager_id.
    SQLAlchemy will resolve manager_id during commit.
    """

    department, location, job_role = get_random_reference_data(
        departments=departments,
        locations=locations,
        job_roles=job_roles,
    )

    first_name = fake.first_name()
    last_name = fake.last_name()

    hire_date = random_date_between(
        start_date=date.today() - timedelta(days=365 * 3),
        end_date=date.today(),
    )

    date_of_birth = random_date_between(
        start_date=date.today() - timedelta(days=365 * 60),
        end_date=date.today() - timedelta(days=365 * 22),
    )

    employee = Employee(
        employee_number=generate_employee_number(index),
        first_name=first_name,
        last_name=last_name,
        email=(
            f"{first_name.lower()}."
            f"{last_name.lower()}."
            f"{index}@examplecompany.com"
        ),
        gender=random.choice(GENDERS),
        date_of_birth=date_of_birth,
        hire_date=hire_date,
        employment_status="Active",
        employment_type=random.choice(EMPLOYMENT_TYPES),
        contract_type="Full Time",
        annual_salary=generate_salary(
            job_role=job_role,
            multiplier=salary_multiplier,
        ),
        currency="GBP",
        department_id=department.department_id,
        role_id=job_role.role_id,
        location_id=location.location_id,
        manager=manager,
        is_manager=is_manager,
        is_active=True,
    )

    if title_prefix:
        employee.contract_type = title_prefix

    return employee


def generate_employees(
    count: int,
    departments: list[Department],
    locations: list[Location],
    job_roles: list[JobRole],
) -> list[Employee]:
    """
    Generate employees with a realistic hierarchy.

    Only the CEO has no manager.
    Everyone else reports to someone.
    """

    if count < 20:
        raise ValueError(
            "Employee count must be at least 20 to generate hierarchy."
        )

    employees: list[Employee] = []

    current_index = 1

    # ---------------------------------------------------------
    # Level 1: CEO
    # ---------------------------------------------------------

    ceo = create_employee(
        index=current_index,
        departments=departments,
        locations=locations,
        job_roles=job_roles,
        manager=None,
        is_manager=True,
        title_prefix="CEO",
        salary_multiplier=2.5,
    )

    employees.append(ceo)
    current_index += 1

    # ---------------------------------------------------------
    # Level 2: Executives
    # ---------------------------------------------------------

    executive_count = max(4, int(count * 0.005))

    executives: list[Employee] = []

    for _ in range(executive_count):
        executive = create_employee(
            index=current_index,
            departments=departments,
            locations=locations,
            job_roles=job_roles,
            manager=ceo,
            is_manager=True,
            title_prefix="Executive",
            salary_multiplier=2.0,
        )

        executives.append(executive)
        employees.append(executive)
        current_index += 1

    # ---------------------------------------------------------
    # Level 3: Directors
    # ---------------------------------------------------------

    director_count = max(10, int(count * 0.02))

    directors: list[Employee] = []

    for _ in range(director_count):
        director = create_employee(
            index=current_index,
            departments=departments,
            locations=locations,
            job_roles=job_roles,
            manager=random.choice(executives),
            is_manager=True,
            title_prefix="Director",
            salary_multiplier=1.7,
        )

        directors.append(director)
        employees.append(director)
        current_index += 1

    # ---------------------------------------------------------
    # Level 4: Managers
    # ---------------------------------------------------------

    manager_count = max(50, int(count * 0.10))

    managers: list[Employee] = []

    for _ in range(manager_count):
        manager = create_employee(
            index=current_index,
            departments=departments,
            locations=locations,
            job_roles=job_roles,
            manager=random.choice(directors),
            is_manager=True,
            title_prefix="Manager",
            salary_multiplier=1.3,
        )

        managers.append(manager)
        employees.append(manager)
        current_index += 1

    # ---------------------------------------------------------
    # Level 5: Individual Contributors
    # ---------------------------------------------------------

    while current_index <= count:
        employee = create_employee(
            index=current_index,
            departments=departments,
            locations=locations,
            job_roles=job_roles,
            manager=random.choice(managers),
            is_manager=False,
            title_prefix="Full Time",
            salary_multiplier=1.0,
        )

        employees.append(employee)
        current_index += 1

    return employees