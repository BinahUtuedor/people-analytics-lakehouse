"""
Business rule validation checks.
"""

from database.models import Employee, ExitInterview, Payroll


def find_employees_without_manager(session):
    return session.query(Employee).filter(Employee.manager_id.is_(None)).all()


def find_non_managers_without_manager(session):
    return (
        session.query(Employee)
        .filter(Employee.manager_id.is_(None))
        .filter(Employee.is_manager.is_(False))
        .all()
    )


def find_employees_with_negative_salary(session):
    return session.query(Employee).filter(Employee.annual_salary < 0).all()


def find_employees_with_termination_before_hire(session):
    return (
        session.query(Employee)
        .filter(Employee.termination_date.isnot(None))
        .filter(Employee.termination_date < Employee.hire_date)
        .all()
    )


def find_exit_interviews_for_active_employees(session):
    return (
        session.query(ExitInterview)
        .join(Employee, ExitInterview.employee_id == Employee.employee_id)
        .filter(Employee.is_active.is_(True))
        .all()
    )


def find_negative_payroll_records(session):
    return (
        session.query(Payroll)
        .filter(
            (Payroll.gross_pay < 0)
            | (Payroll.net_pay < 0)
            | (Payroll.base_salary < 0)
        )
        .all()
    )