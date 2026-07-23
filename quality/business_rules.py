"""
Business rule validation checks.
"""

from database.models import (
    Employee,
    EmployeeExit,
    ExitInterview,
    Payroll,
)


def find_employees_without_manager(session):
    return (
        session.query(Employee)
        .filter(
            Employee.manager_id.is_(None)
        )
        .all()
    )


def find_non_managers_without_manager(session):
    return (
        session.query(Employee)
        .filter(
            Employee.manager_id.is_(None)
        )
        .filter(
            Employee.is_manager.is_(False)
        )
        .all()
    )


def find_employees_with_negative_salary(session):
    return (
        session.query(Employee)
        .filter(
            Employee.annual_salary < 0
        )
        .all()
    )


def find_employees_with_termination_before_hire(session):
    return (
        session.query(Employee)
        .filter(
            Employee.termination_date.isnot(None)
        )
        .filter(
            Employee.termination_date
            < Employee.hire_date
        )
        .all()
    )


def find_exit_interviews_for_active_employees(session):
    return (
        session.query(ExitInterview)
        .join(
            Employee,
            ExitInterview.employee_id
            == Employee.employee_id,
        )
        .filter(
            Employee.is_active.is_(True)
        )
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


def find_active_employees_with_exit_event(session):
    """
    Find employees with an EmployeeExit event who are still marked active.
    """

    return (
        session.query(Employee)
        .join(
            EmployeeExit,
            Employee.employee_id
            == EmployeeExit.employee_id,
        )
        .filter(
            Employee.is_active.is_(True)
        )
        .all()
    )


def find_exit_events_before_hire_date(session):
    """
    Find exit events occurring before the employee's hire date.
    """

    return (
        session.query(EmployeeExit)
        .join(
            Employee,
            EmployeeExit.employee_id
            == Employee.employee_id,
        )
        .filter(
            EmployeeExit.exit_date
            < Employee.hire_date
        )
        .all()
    )


def find_exit_date_mismatches(session):
    """
    Find employees whose current-state termination date differs from
    their authoritative EmployeeExit event date.
    """

    return (
        session.query(EmployeeExit)
        .join(
            Employee,
            EmployeeExit.employee_id
            == Employee.employee_id,
        )
        .filter(
            (
                Employee.termination_date.is_(None)
            )
            | (
                Employee.termination_date
                != EmployeeExit.exit_date
            )
        )
        .all()
    )


def find_exit_events_with_non_terminated_status(session):
    """
    Find employees with an exit event whose current employment status
    was not updated to Terminated.
    """

    return (
        session.query(EmployeeExit)
        .join(
            Employee,
            EmployeeExit.employee_id
            == Employee.employee_id,
        )
        .filter(
            Employee.employment_status
            != "Terminated"
        )
        .all()
    )


def find_terminated_employees_without_exit_event(session):
    """
    Find terminated/inactive employee records without EmployeeExit history.

    This check establishes EmployeeExit as the authoritative source for
    termination events.
    """

    return (
        session.query(Employee)
        .outerjoin(
            EmployeeExit,
            Employee.employee_id
            == EmployeeExit.employee_id,
        )
        .filter(
            (
                Employee.is_active.is_(False)
            )
            | (
                Employee.termination_date.isnot(None)
            )
            | (
                Employee.employment_status
                == "Terminated"
            )
        )
        .filter(
            EmployeeExit.exit_event_id.is_(None)
        )
        .all()
    )