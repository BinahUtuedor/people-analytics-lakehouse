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
    AbsenceReason,
    Attendance,
    AttendanceStatus,
    Department,
    Employee,
    EmployeeExit,
    EmployeeSurvey,
    EmploymentType,
    ExitInterview,
    ExitReason,
    Gender,
    JobRole,
    LeaveRequest,
    LeaveType,
    Location,
    ManagerFeedback,
    Payroll,
    PerformanceReview,
    Promotion,
    PublicHoliday,
    Recruitment,
    Training,
    TrainingCategory,
    Transfer,
)

from simulator.attendance import generate_attendance
from simulator.employees import generate_employees
from simulator.exits import generate_employee_exits
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
    """
    Load all lookup/reference records currently consumed by simulator
    modules.

    Reference data is seeded from YAML into PostgreSQL before simulation.
    """

    departments = (
        session.query(
            Department
        )
        .all()
    )

    locations = (
        session.query(
            Location
        )
        .all()
    )

    job_roles = (
        session.query(
            JobRole
        )
        .all()
    )

    employment_types = (
        session.query(
            EmploymentType
        )
        .all()
    )

    genders = (
        session.query(
            Gender
        )
        .all()
    )

    attendance_statuses = (
        session.query(
            AttendanceStatus
        )
        .all()
    )

    absence_reasons = (
        session.query(
            AbsenceReason
        )
        .all()
    )

    public_holidays = (
        session.query(
            PublicHoliday
        )
        .all()
    )

    leave_types = (
        session.query(
            LeaveType
        )
        .all()
    )

    training_categories = (
        session.query(
            TrainingCategory
        )
        .all()
    )

    exit_reasons = (
        session.query(
            ExitReason
        )
        .all()
    )

    reference_datasets = {
        "departments": departments,
        "locations": locations,
        "job_roles": job_roles,
        "employment_types": employment_types,
        "genders": genders,
        "attendance_statuses": attendance_statuses,
        "absence_reasons": absence_reasons,
        "public_holidays": public_holidays,
        "leave_types": leave_types,
        "training_categories": training_categories,
        "exit_reasons": exit_reasons,
    }

    missing = [
        dataset_name
        for (
            dataset_name,
            records,
        ) in reference_datasets.items()
        if not records
    ]

    if missing:
        raise ValueError(
            "Missing required simulator reference data: "
            + ", ".join(
                missing
            )
            + ". Run python -m database.seed first."
        )

    logger.info(
        "Reference data loaded | "
        f"Departments={len(departments)} | "
        f"Locations={len(locations)} | "
        f"JobRoles={len(job_roles)} | "
        f"EmploymentTypes={len(employment_types)} | "
        f"Genders={len(genders)} | "
        f"AttendanceStatuses={len(attendance_statuses)} | "
        f"AbsenceReasons={len(absence_reasons)} | "
        f"PublicHolidays={len(public_holidays)} | "
        f"LeaveTypes={len(leave_types)} | "
        f"TrainingCategories={len(training_categories)} | "
        f"ExitReasons={len(exit_reasons)}"
    )

    return (
        departments,
        locations,
        job_roles,
        employment_types,
        genders,
        attendance_statuses,
        absence_reasons,
        public_holidays,
        leave_types,
        training_categories,
        exit_reasons,
    )


def full_refresh_generated_data(
    session,
) -> None:
    """
    Remove all generated operational data.

    Reference-data tables are intentionally preserved.

    employee_exits is included before employees so the exit-event
    foreign key is cleared as part of the existing full-refresh process.
    """

    logger.warning(
        "Full refresh requested. "
        "Clearing generated data..."
    )

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
                employee_exits,
                employees
            RESTART IDENTITY CASCADE;
            """
        )
    )

    session.commit()

    logger.info(
        "Generated data cleared successfully."
    )


def table_is_empty(
    session,
    model,
) -> bool:
    """Return True when the supplied table contains no records."""

    return (
        session.query(
            model
        )
        .count()
        == 0
    )


def run_generation_step(
    session,
    name: str,
    model,
    generator,
):
    """
    Run one idempotent simulation step.

    Existing tables are left unchanged during non-full-refresh runs,
    preserving the simulator's current behaviour.
    """

    if table_is_empty(
        session,
        model,
    ):
        records = generator()

        session.add_all(
            records
        )

        session.commit()

        logger.info(
            f"Generated "
            f"{len(records):,} "
            f"{name} records."
        )

        return records

    logger.warning(
        f"{name.title()} "
        "already exists. Skipping."
    )

    return (
        session.query(
            model
        )
        .all()
    )


def run_simulation(
    full_refresh: bool = False,
) -> None:
    """
    Run the complete People Analytics simulation.

    Current reference-data migrations:

    Employee:
        EmploymentType
        Gender

    Recruitment employees:
        EmploymentType
        Gender

    Attendance:
        AttendanceStatus
        AbsenceReason
        PublicHoliday

    Leave:
        LeaveType

    Training:
        TrainingCategory

    Employee exits:
        ExitReason

    Existing operational table schemas remain unchanged.
    """

    logger.info(
        "Starting people analytics simulation..."
    )

    logger.info(
        f"Configured employee count: "
        f"{INITIAL_EMPLOYEE_COUNT}"
    )

    session = get_session()

    try:

        if full_refresh:
            full_refresh_generated_data(
                session
            )

        (
            departments,
            locations,
            job_roles,
            employment_types,
            genders,
            attendance_statuses,
            absence_reasons,
            public_holidays,
            leave_types,
            training_categories,
            exit_reasons,
        ) = load_reference_data(
            session
        )

        # ---------------------------------------------------------------
        # Employee master population.
        # ---------------------------------------------------------------

        if table_is_empty(
            session,
            Employee,
        ):

            employees = generate_employees(
                count=INITIAL_EMPLOYEE_COUNT,
                departments=departments,
                locations=locations,
                job_roles=job_roles,
                employment_types=(
                    employment_types
                ),
                genders=genders,
            )

            session.add_all(
                employees
            )

            session.commit()

            logger.info(
                f"Generated "
                f"{len(employees):,} "
                "employees."
            )

        else:
            logger.warning(
                "Employees already exist. "
                "Skipping."
            )

        employees = (
            session.query(
                Employee
            )
            .all()
        )

        # ---------------------------------------------------------------
        # Recruitment.
        #
        # Filled vacancies create additional real Employee records.
        # ---------------------------------------------------------------

        if table_is_empty(
            session,
            Recruitment,
        ):

            (
                recruitment_records,
                recruited_employees,
            ) = generate_recruitment(
                departments=departments,
                job_roles=job_roles,
                locations=locations,
                employees=employees,
                employment_types=(
                    employment_types
                ),
                genders=genders,
            )

            session.add_all(
                recruited_employees
            )

            session.add_all(
                recruitment_records
            )

            session.commit()

            logger.info(
                f"Generated "
                f"{len(recruitment_records):,} "
                "recruitment records."
            )

            logger.info(
                f"Created "
                f"{len(recruited_employees):,} "
                "employees from filled vacancies."
            )

        else:
            logger.warning(
                "Recruitment already exists. "
                "Skipping."
            )

        # Refresh authoritative workforce after recruitment.
        employees = (
            session.query(
                Employee
            )
            .all()
        )

        logger.info(
            "Employee population after recruitment: "
            f"{len(employees):,}"
        )

        # ---------------------------------------------------------------
        # Employee exits.
        #
        # Exits must occur before downstream employment-window facts.
        # ---------------------------------------------------------------

        exit_records = (
            run_generation_step(
                session=session,
                name="employee exits",
                model=EmployeeExit,
                generator=lambda: (
                    generate_employee_exits(
                        employees=employees,
                        exit_reasons=(
                            exit_reasons
                        ),
                    )
                ),
            )
        )

        # ---------------------------------------------------------------
        # Operational generation steps.
        # ---------------------------------------------------------------

        generation_steps = [
            (
                "attendance",
                Attendance,
                lambda: (
                    generate_attendance(
                        employees=employees,
                        attendance_statuses=(
                            attendance_statuses
                        ),
                        absence_reasons=(
                            absence_reasons
                        ),
                        public_holidays=(
                            public_holidays
                        ),
                    )
                ),
            ),
            (
                "payroll",
                Payroll,
                lambda: (
                    generate_payroll(
                        employees
                    )
                ),
            ),
            (
                "leave requests",
                LeaveRequest,
                lambda: (
                    generate_leave_requests(
                        employees=employees,
                        leave_types=(
                            leave_types
                        ),
                    )
                ),
            ),
            (
                "training",
                Training,
                lambda: (
                    generate_training(
                        employees=employees,
                        training_categories=(
                            training_categories
                        ),
                    )
                ),
            ),
            (
                "performance reviews",
                PerformanceReview,
                lambda: (
                    generate_performance_reviews(
                        employees
                    )
                ),
            ),
            (
                "promotions",
                Promotion,
                lambda: (
                    generate_promotions(
                        employees,
                        job_roles,
                    )
                ),
            ),
            (
                "transfers",
                Transfer,
                lambda: (
                    generate_transfers(
                        employees,
                        departments,
                        locations,
                    )
                ),
            ),
            (
                "employee surveys",
                EmployeeSurvey,
                lambda: (
                    generate_employee_surveys(
                        employees
                    )
                ),
            ),
            (
                "manager feedback",
                ManagerFeedback,
                lambda: (
                    generate_manager_feedback(
                        employees
                    )
                ),
            ),
        ]

        for (
            name,
            model,
            generator,
        ) in generation_steps:

            run_generation_step(
                session=session,
                name=name,
                model=model,
                generator=generator,
            )

        # ---------------------------------------------------------------
        # Exit interviews.
        #
        # EmployeeExit remains the authoritative event source.
        # ---------------------------------------------------------------

        run_generation_step(
            session=session,
            name="exit interviews",
            model=ExitInterview,
            generator=lambda: (
                generate_exit_interviews(
                    exit_records
                )
            ),
        )

        logger.info(
            "People analytics simulation "
            "completed successfully."
        )

    except (
        SQLAlchemyError,
        ValueError,
    ) as error:

        session.rollback()

        logger.error(
            "People analytics simulation failed."
        )

        logger.error(
            error
        )

        raise

    finally:
        session.close()


def parse_args():
    """Parse simulator command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Run the People Analytics simulator."
        )
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help=(
            "Clear generated data and rebuild "
            "the simulation."
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    run_simulation(
        full_refresh=(
            args.full_refresh
        )
    )
