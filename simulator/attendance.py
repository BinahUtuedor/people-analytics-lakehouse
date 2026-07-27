"""
Attendance simulation module.

- Attendance captures realistic work patterns:
  Present, Remote, Hybrid, Training, Business Travel, Absent.
- absence_reason is only populated when status = Absent.
- Leave is generated separately in leave_requests.
- Attendance is generated only within each employee's employment window.
- Attendance statuses, absence reasons and public holidays are supplied
  from PostgreSQL reference tables seeded from YAML reference data.

Employment window:

    employee.hire_date
        <= attendance.work_date
        <= employee.termination_date

For employees without a termination date, today's date is used as the
upper boundary.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import (
    AbsenceReason,
    Attendance,
    AttendanceStatus,
    Employee,
    PublicHoliday,
)
from simulator.effective_dates import employment_end_date


random.seed(DEFAULT_RANDOM_SEED)


def get_simulation_start_date() -> date:
    """Return the configured attendance simulation start date."""

    return date.today() - timedelta(
        days=365 * HISTORICAL_YEARS
    )


def is_weekend(
    work_date: date,
) -> bool:
    """Return True for Saturday or Sunday."""

    return work_date.weekday() >= 5


def is_public_holiday(
    work_date: date,
    public_holidays: list[PublicHoliday],
) -> bool:
    """
    Return whether a date matches one of the active public holidays.

    Public-holiday definitions are loaded from PostgreSQL reference data.

    The current reference-data model uses recurring MM-DD values,
    preserving the simulator's existing simplified holiday behaviour.
    """

    month_day = work_date.strftime(
        "%m-%d"
    )

    return any(
        holiday.active
        and holiday.month_day
        == month_day
        for holiday in public_holidays
    )


def generate_clock_in(
    status: str,
) -> time | None:
    """Generate clock-in time based on attendance type."""

    if status == "Absent":
        return None

    if status == "Remote":
        return time(
            hour=random.choice(
                [
                    8,
                    8,
                    9,
                ]
            ),
            minute=random.randint(
                0,
                45,
            ),
        )

    if status == "Hybrid":
        return time(
            hour=random.choice(
                [
                    8,
                    9,
                    9,
                ]
            ),
            minute=random.randint(
                0,
                50,
            ),
        )

    if status == "Business Travel":
        return time(
            hour=random.choice(
                [
                    7,
                    8,
                    9,
                ]
            ),
            minute=random.randint(
                0,
                59,
            ),
        )

    return time(
        hour=random.choice(
            [
                8,
                8,
                8,
                9,
            ]
        ),
        minute=random.randint(
            0,
            45,
        ),
    )


def generate_clock_out(
    clock_in: time | None,
    status: str,
) -> time | None:
    """Generate clock-out time based on attendance type."""

    if clock_in is None:
        return None

    base_datetime = datetime.combine(
        date.today(),
        clock_in,
    )

    if status == "Training":
        working_minutes = random.randint(
            6 * 60,
            7 * 60,
        )

    elif status == "Business Travel":
        working_minutes = random.randint(
            8 * 60,
            10 * 60,
        )

    else:
        working_minutes = random.randint(
            7 * 60,
            9 * 60,
        )

    clock_out_datetime = (
        base_datetime
        + timedelta(
            minutes=working_minutes
        )
    )

    return clock_out_datetime.time()


def calculate_hours(
    clock_in: time | None,
    clock_out: time | None,
) -> Decimal:
    """Calculate hours worked."""

    if (
        clock_in is None
        or clock_out is None
    ):
        return Decimal("0.00")

    start = datetime.combine(
        date.today(),
        clock_in,
    )

    end = datetime.combine(
        date.today(),
        clock_out,
    )

    hours = (
        end - start
    ).seconds / 3600

    return Decimal(
        str(hours)
    ).quantize(
        Decimal("0.01")
    )


def calculate_overtime(
    hours_worked: Decimal,
) -> Decimal:
    """Calculate overtime above a 7.5-hour standard day."""

    return max(
        Decimal("0.00"),
        hours_worked
        - Decimal("7.50"),
    ).quantize(
        Decimal("0.01")
    )


def validate_attendance_reference_data(
    attendance_statuses: list[AttendanceStatus],
    absence_reasons: list[AbsenceReason],
    public_holidays: list[PublicHoliday],
) -> None:
    """
    Confirm that all reference datasets required by attendance exist.
    """

    missing: list[str] = []

    if not attendance_statuses:
        missing.append(
            "attendance_statuses"
        )

    if not absence_reasons:
        missing.append(
            "absence_reasons"
        )

    if not public_holidays:
        missing.append(
            "public_holidays"
        )

    if missing:
        raise ValueError(
            "Attendance simulation is missing required "
            "reference data: "
            + ", ".join(
                missing
            )
            + "."
        )


def select_attendance_status(
    attendance_statuses: list[AttendanceStatus],
) -> str:
    """
    Select an attendance status using reference-data weights.

    This preserves the previous distribution:

        Present          weight 3
        Remote           weight 2
        Hybrid           weight 1
        Training         weight 1
        Business Travel  weight 1
        Absent           weight 1
    """

    selected_status = random.choices(
        population=attendance_statuses,
        weights=[
            status.weight
            for status
            in attendance_statuses
        ],
        k=1,
    )[0]

    return (
        selected_status
        .attendance_status_name
    )


def select_absence_reason(
    absence_reasons: list[AbsenceReason],
) -> str:
    """
    Select one absence reason from reference data.
    """

    return (
        random.choice(
            absence_reasons
        )
        .absence_reason_name
    )


def generate_attendance_for_employee(
    employee: Employee,
    start_date: date,
    end_date: date,
    attendance_statuses: list[AttendanceStatus],
    absence_reasons: list[AbsenceReason],
    public_holidays: list[PublicHoliday],
) -> list[Attendance]:
    """
    Generate attendance for one employee within their employment window.

    The effective start is the later of:
    - global simulation start;
    - employee hire date.

    The effective end is the earlier of:
    - requested simulation end;
    - employee termination date;
    - today.
    """

    records: list[Attendance] = []

    effective_start = max(
        employee.hire_date,
        start_date,
    )

    effective_end = min(
        end_date,
        employment_end_date(
            employee
        ),
        date.today(),
    )

    # Employee may have no overlap with the requested simulation period.
    if effective_start > effective_end:
        return records

    current_date = effective_start

    while current_date <= effective_end:

        if (
            is_weekend(
                current_date
            )
            or is_public_holiday(
                current_date,
                public_holidays,
            )
        ):
            current_date += timedelta(
                days=1
            )
            continue

        status = select_attendance_status(
            attendance_statuses
        )

        clock_in = generate_clock_in(
            status
        )

        clock_out = generate_clock_out(
            clock_in,
            status,
        )

        hours_worked = calculate_hours(
            clock_in,
            clock_out,
        )

        overtime_hours = calculate_overtime(
            hours_worked
        )

        absence_reason = (
            select_absence_reason(
                absence_reasons
            )
            if status == "Absent"
            else None
        )

        # The effective-date boundary has already been checked before
        # the ORM object is constructed.
        records.append(
            Attendance(
                employee=employee,
                work_date=current_date,
                status=status,
                clock_in_time=clock_in,
                clock_out_time=clock_out,
                hours_worked=hours_worked,
                overtime_hours=overtime_hours,
                absence_reason=absence_reason,
            )
        )

        current_date += timedelta(
            days=1
        )

    return records


def generate_attendance(
    employees: list[Employee],
    attendance_statuses: list[AttendanceStatus],
    absence_reasons: list[AbsenceReason],
    public_holidays: list[PublicHoliday],
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[Attendance]:
    """
    Generate attendance records for all employees with employment-period
    overlap in the configured simulation window.

    Terminated employees are intentionally included because they require
    historical attendance up to their termination date.

    Attendance statuses, absence reasons and public holidays are supplied
    from centrally governed reference tables.
    """

    validate_attendance_reference_data(
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

    if start_date is None:
        start_date = (
            get_simulation_start_date()
        )

    if end_date is None:
        end_date = date.today()

    records: list[Attendance] = []

    # Do not filter on Employee.is_active here. A terminated employee
    # must retain historical attendance prior to termination.
    for employee in employees:
        records.extend(
            generate_attendance_for_employee(
                employee=employee,
                start_date=start_date,
                end_date=end_date,
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
        )

    return records
