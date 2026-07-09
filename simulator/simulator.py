"""
Simulator entry point.

Run:

    python -m simulator.simulator

Full refresh:

    python -m simulator.simulator --full-refresh
"""

from __future__ import annotations

import argparse

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from config.constants import INITIAL_EMPLOYEE_COUNT
from config.logger import logger
from database.connection import get_session
from database.models import (
    Attendance,
    Department,
    Employee,
    EmployeeSurvey,
    ExitInterview,
    JobRole,
    LeaveRequest,
    Location,
    ManagerFeedback,
    Payroll,
    PerformanceReview,
    Promotion,
    Recruitment,
    Training,
    Transfer,
)
from simulator.attendance import generate_attendance
from simulator.employees import generate_employees
from simulator.exit_interviews import generate_exit_interviews
from simulator.leave import generate_leave_requests
from simulator.manager_feedback import generate_manager_feedback
from simulator.payroll import generate_payroll
from simulator.performance import generate_performance_reviews
from simulator.promotion import generate_promotions
from simulator.recruitment import generate_recruitment
from simulator.surveys import generate_employee_surveys
from simulator.training import generate_training
from simulator.transfer import generate_transfers


def load_reference_data(session):
    departments = session.query(Department).all()
    locations = session.query(Location).all()
    job_roles = session.query(JobRole).all()

    if not departments:
        raise ValueError("No departments found. Run python -m database.seed first.")
    if not locations:
        raise ValueError("No locations found. Run python -m database.seed first.")
    if not job_roles:
        raise ValueError("No job roles found. Run python -m database.seed first.")

    return departments, locations, job_roles


def full_refresh_generated_data(session) -> None:
    logger.warning("Full refresh requested. Clearing generated data...")

    session.execute(
        text(
            """
            TRUNCATE TABLE
                attendance,
                payroll,
                leave_requests,
                training,
                performance_reviews,
                promotions,
                transfers,
                recruitment,
                employee_surveys,
                manager_feedback,
                exit_interviews,
                employees
            RESTART IDENTITY CASCADE;
            """
        )
    )

    session.commit()

    logger.info("Generated data cleared successfully.")


def table_is_empty(session, model) -> bool:
    return session.query(model).count() == 0


def run_simulation(full_refresh: bool = False) -> None:
    logger.info("Starting people analytics simulation...")
    logger.info(f"Configured employee count: {INITIAL_EMPLOYEE_COUNT}")

    session = get_session()

    try:
        if full_refresh:
            full_refresh_generated_data(session)

        departments, locations, job_roles = load_reference_data(session)

        if table_is_empty(session, Employee):
            employees = generate_employees(
                count=INITIAL_EMPLOYEE_COUNT,
                departments=departments,
                locations=locations,
                job_roles=job_roles,
            )
            session.add_all(employees)
            session.commit()
            logger.info(f"Generated {len(employees)} employees.")
        else:
            logger.warning("Employees already exist. Skipping.")

        employees = session.query(Employee).all()

        generation_steps = [
            ("attendance", Attendance, lambda: generate_attendance(employees)),
            ("payroll", Payroll, lambda: generate_payroll(employees)),
            ("leave requests", LeaveRequest, lambda: generate_leave_requests(employees)),
            ("training", Training, lambda: generate_training(employees)),
            ("performance reviews", PerformanceReview, lambda: generate_performance_reviews(employees)),
            ("promotions", Promotion, lambda: generate_promotions(employees, job_roles)),
            ("transfers", Transfer, lambda: generate_transfers(employees, departments, locations)),
            ("recruitment", Recruitment, lambda: generate_recruitment(departments, job_roles, employees)),
            ("employee surveys", EmployeeSurvey, lambda: generate_employee_surveys(employees)),
            ("manager feedback", ManagerFeedback, lambda: generate_manager_feedback(employees)),
            ("exit interviews", ExitInterview, lambda: generate_exit_interviews(employees)),
        ]

        for name, model, generator in generation_steps:
            if table_is_empty(session, model):
                records = generator()
                session.add_all(records)
                session.commit()
                logger.info(f"Generated {len(records)} {name} records.")
            else:
                logger.warning(f"{name.title()} already exists. Skipping.")

        logger.info("People analytics simulation completed successfully.")

    except (SQLAlchemyError, ValueError) as error:
        session.rollback()
        logger.error("People analytics simulation failed.")
        logger.error(error)
        raise

    finally:
        session.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the People Analytics simulator."
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Clear generated data and rebuild the simulation.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_simulation(full_refresh=args.full_refresh)