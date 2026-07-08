"""
Seed database lookup tables.

This script inserts static reference data used by the simulator.

Run from the project root:

    python -m database.seed
"""

from sqlalchemy.exc import SQLAlchemyError

from config.logger import logger
from database.connection import get_session
from database.models import (
    BusinessUnit,
    Department,
    Location,
    JobRole,
)


# ---------------------------------------------------------
# Seed Data
# ---------------------------------------------------------

BUSINESS_UNITS = [
    {
        "unit_name": "Corporate Services",
        "description": "Finance, HR, legal and central support functions.",
    },
    {
        "unit_name": "Technology",
        "description": "Software engineering, data, infrastructure and cyber teams.",
    },
    {
        "unit_name": "Commercial",
        "description": "Sales, marketing, customer success and partnerships.",
    },
    {
        "unit_name": "Operations",
        "description": "Service delivery, business operations and support functions.",
    },
]


LOCATIONS = [
    {
        "country": "United Kingdom",
        "city": "London",
        "office_name": "London HQ",
        "timezone": "Europe/London",
    },
    {
        "country": "United Kingdom",
        "city": "Manchester",
        "office_name": "Manchester Office",
        "timezone": "Europe/London",
    },
    {
        "country": "United Kingdom",
        "city": "Birmingham",
        "office_name": "Birmingham Office",
        "timezone": "Europe/London",
    },
    {
        "country": "United Kingdom",
        "city": "Bristol",
        "office_name": "Bristol Office",
        "timezone": "Europe/London",
    },
    {
        "country": "United Kingdom",
        "city": "Edinburgh",
        "office_name": "Edinburgh Office",
        "timezone": "Europe/London",
    },
]


JOB_ROLES = [
    {
        "role_name": "HR Advisor",
        "grade": "Associate",
        "salary_band_min": 30000,
        "salary_band_max": 42000,
    },
    {
        "role_name": "Finance Analyst",
        "grade": "Associate",
        "salary_band_min": 35000,
        "salary_band_max": 50000,
    },
    {
        "role_name": "Finance Business Partner",
        "grade": "Manager",
        "salary_band_min": 55000,
        "salary_band_max": 75000,
    },
    {
        "role_name": "Data Analyst",
        "grade": "Associate",
        "salary_band_min": 35000,
        "salary_band_max": 52000,
    },
    {
        "role_name": "Data Engineer",
        "grade": "Senior Associate",
        "salary_band_min": 50000,
        "salary_band_max": 75000,
    },
    {
        "role_name": "Senior Data Engineer",
        "grade": "Manager",
        "salary_band_min": 70000,
        "salary_band_max": 95000,
    },
    {
        "role_name": "Software Engineer",
        "grade": "Senior Associate",
        "salary_band_min": 50000,
        "salary_band_max": 80000,
    },
    {
        "role_name": "Project Manager",
        "grade": "Manager",
        "salary_band_min": 55000,
        "salary_band_max": 78000,
    },
    {
        "role_name": "Operations Manager",
        "grade": "Manager",
        "salary_band_min": 52000,
        "salary_band_max": 72000,
    },
    {
        "role_name": "Customer Support Specialist",
        "grade": "Associate",
        "salary_band_min": 28000,
        "salary_band_max": 40000,
    },
]


DEPARTMENTS = [
    {
        "department_name": "Human Resources",
        "cost_center": "CC-HR-001",
        "business_unit": "Corporate Services",
    },
    {
        "department_name": "Finance",
        "cost_center": "CC-FIN-001",
        "business_unit": "Corporate Services",
    },
    {
        "department_name": "Data Engineering",
        "cost_center": "CC-DATA-001",
        "business_unit": "Technology",
    },
    {
        "department_name": "Software Engineering",
        "cost_center": "CC-SWE-001",
        "business_unit": "Technology",
    },
    {
        "department_name": "Cyber Security",
        "cost_center": "CC-CYB-001",
        "business_unit": "Technology",
    },
    {
        "department_name": "Sales",
        "cost_center": "CC-SALES-001",
        "business_unit": "Commercial",
    },
    {
        "department_name": "Marketing",
        "cost_center": "CC-MKT-001",
        "business_unit": "Commercial",
    },
    {
        "department_name": "Customer Success",
        "cost_center": "CC-CS-001",
        "business_unit": "Commercial",
    },
    {
        "department_name": "Operations",
        "cost_center": "CC-OPS-001",
        "business_unit": "Operations",
    },
    {
        "department_name": "Service Delivery",
        "cost_center": "CC-SD-001",
        "business_unit": "Operations",
    },
]


# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def get_or_create(session, model, defaults=None, **lookup):
    """
    Return an existing record or create it if missing.

    This makes the seed script safe to rerun without creating duplicates.
    """

    instance = session.query(model).filter_by(**lookup).one_or_none()

    if instance:
        return instance, False

    params = dict(lookup)

    if defaults:
        params.update(defaults)

    instance = model(**params)

    session.add(instance)

    return instance, True


# ---------------------------------------------------------
# Seed Logic
# ---------------------------------------------------------

def seed_business_units(session) -> dict[str, BusinessUnit]:
    """
    Insert business units and return a name-to-object mapping.
    """

    business_units = {}

    for item in BUSINESS_UNITS:
        unit, created = get_or_create(
            session,
            BusinessUnit,
            unit_name=item["unit_name"],
            defaults={
                "description": item["description"],
            },
        )

        business_units[unit.unit_name] = unit

        if created:
            logger.info(f"Created business unit: {unit.unit_name}")
        else:
            logger.info(f"Business unit already exists: {unit.unit_name}")

    session.flush()

    return business_units


def seed_locations(session) -> None:
    """
    Insert office locations.
    """

    for item in LOCATIONS:
        location, created = get_or_create(
            session,
            Location,
            office_name=item["office_name"],
            defaults={
                "country": item["country"],
                "city": item["city"],
                "timezone": item["timezone"],
            },
        )

        if created:
            logger.info(f"Created location: {location.office_name}")
        else:
            logger.info(f"Location already exists: {location.office_name}")

    session.flush()


def seed_job_roles(session) -> None:
    """
    Insert job roles.
    """

    for item in JOB_ROLES:
        role, created = get_or_create(
            session,
            JobRole,
            role_name=item["role_name"],
            defaults={
                "grade": item["grade"],
                "salary_band_min": item["salary_band_min"],
                "salary_band_max": item["salary_band_max"],
            },
        )

        if created:
            logger.info(f"Created job role: {role.role_name}")
        else:
            logger.info(f"Job role already exists: {role.role_name}")

    session.flush()


def seed_departments(
    session,
    business_units: dict[str, BusinessUnit],
) -> None:
    """
    Insert departments and link each one to its business unit.
    """

    for item in DEPARTMENTS:
        unit_name = item["business_unit"]

        business_unit = business_units.get(unit_name)

        if business_unit is None:
            raise ValueError(
                f"Business unit not found for department: {unit_name}"
            )

        department, created = get_or_create(
            session,
            Department,
            department_name=item["department_name"],
            defaults={
                "cost_center": item["cost_center"],
                "business_unit_id": business_unit.business_unit_id,
            },
        )

        if created:
            logger.info(f"Created department: {department.department_name}")
        else:
            logger.info(f"Department already exists: {department.department_name}")

    session.flush()


def seed_database() -> None:
    """
    Run all seed operations in the correct dependency order.
    """

    logger.info("Starting database seed...")

    session = get_session()

    try:
        business_units = seed_business_units(session)

        seed_locations(session)

        seed_job_roles(session)

        seed_departments(session, business_units)

        session.commit()

        logger.info("Database seed completed successfully.")

    except (SQLAlchemyError, ValueError) as error:
        session.rollback()

        logger.error("Database seed failed.")
        logger.error(error)

        raise

    finally:
        session.close()


if __name__ == "__main__":
    seed_database()