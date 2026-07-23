"""
Referential integrity checks.
"""

from database.models import (
    Attendance,
    Department,
    Employee,
    EmployeeExit,
    ExitInterview,
    JobRole,
    Location,
    Payroll,
)


def find_employees_with_missing_department(session):
    return (
        session.query(Employee)
        .outerjoin(
            Department,
            Employee.department_id
            == Department.department_id,
        )
        .filter(
            Department.department_id.is_(None)
        )
        .all()
    )


def find_employees_with_missing_job_role(session):
    return (
        session.query(Employee)
        .outerjoin(
            JobRole,
            Employee.role_id
            == JobRole.role_id,
        )
        .filter(
            JobRole.role_id.is_(None)
        )
        .all()
    )


def find_employees_with_missing_location(session):
    return (
        session.query(Employee)
        .outerjoin(
            Location,
            Employee.location_id
            == Location.location_id,
        )
        .filter(
            Location.location_id.is_(None)
        )
        .all()
    )


def find_orphan_attendance_records(session):
    return (
        session.query(Attendance)
        .outerjoin(
            Employee,
            Attendance.employee_id
            == Employee.employee_id,
        )
        .filter(
            Employee.employee_id.is_(None)
        )
        .all()
    )


def find_orphan_payroll_records(session):
    return (
        session.query(Payroll)
        .outerjoin(
            Employee,
            Payroll.employee_id
            == Employee.employee_id,
        )
        .filter(
            Employee.employee_id.is_(None)
        )
        .all()
    )


def find_orphan_employee_exit_records(session):
    """
    Find EmployeeExit events whose employee no longer exists.

    The database foreign-key constraint should normally prevent this,
    but retaining an explicit quality check gives the lifecycle event
    the same independent assurance as attendance and payroll.
    """

    return (
        session.query(EmployeeExit)
        .outerjoin(
            Employee,
            EmployeeExit.employee_id
            == Employee.employee_id,
        )
        .filter(
            Employee.employee_id.is_(None)
        )
        .all()
    )


def find_exit_interviews_without_exit_event(session):
    """
    Find exit interviews that have no corresponding EmployeeExit event.

    EmployeeExit is now the authoritative employment-exit event.
    """

    return (
        session.query(ExitInterview)
        .outerjoin(
            EmployeeExit,
            ExitInterview.employee_id
            == EmployeeExit.employee_id,
        )
        .filter(
            EmployeeExit.exit_event_id.is_(None)
        )
        .all()
    )