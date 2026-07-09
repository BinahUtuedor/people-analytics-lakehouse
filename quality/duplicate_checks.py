"""
Duplicate data quality checks.
"""

from sqlalchemy import func

from database.models import Employee


def find_duplicate_employee_numbers(session):
    return (
        session.query(
            Employee.employee_number,
            func.count(Employee.employee_id).label("record_count"),
        )
        .group_by(Employee.employee_number)
        .having(func.count(Employee.employee_id) > 1)
        .all()
    )


def find_duplicate_emails(session):
    return (
        session.query(
            Employee.email,
            func.count(Employee.employee_id).label("record_count"),
        )
        .group_by(Employee.email)
        .having(func.count(Employee.employee_id) > 1)
        .all()
    )