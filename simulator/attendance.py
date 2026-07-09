"""
Attendance simulation module.

Improved design:
- Attendance no longer stores Annual Leave or Sick Leave as statuses.
- Leave will later be generated in leave_requests.
- Attendance now captures realistic work patterns:
  Present, Remote, Hybrid, Training, Business Travel, Absent.
- absence_reason is only populated when status = Absent.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Attendance, Employee


random.seed(DEFAULT_RANDOM_SEED)


ATTENDANCE_STATUSES = [
    "Present",
    "Present",
    "Present",
    "Remote",
    "Remote",
    "Hybrid",
    "Training",
    "Business Travel",
    "Absent",
]

ABSENCE_REASONS = [
    "Unplanned Absence",
    "Medical Appointment",
    "Family Emergency",
    "Transport Disruption",
    "Unauthorised Absence",
]


UK_PUBLIC_HOLIDAYS = {
    "01-01",  # New Year's Day
    "12-25",  # Christmas Day
    "12-26",  # Boxing Day
}


def get_simulation_start_date() -> date:
    """Return attendance simulation start date."""

    return date.today() - timedelta(days=365 * HISTORICAL_YEARS)


def is_weekend(work_date: date) -> bool:
    """Return True for Saturday or Sunday."""

    return work_date.weekday() >= 5


def is_public_holiday(work_date: date) -> bool:
    """
    Basic fixed public holiday logic.

    This is intentionally simple for now.
    More detailed UK bank holiday logic can be added later.
    """

    return work_date.strftime("%m-%d") in UK_PUBLIC_HOLIDAYS


def generate_clock_in(status: str) -> time | None:
    """Generate clock-in time based on attendance type."""

    if status == "Absent":
        return None

    if status == "Remote":
        return time(
            hour=random.choice([8, 8, 9]),
            minute=random.randint(0, 45),
        )

    if status == "Hybrid":
        return time(
            hour=random.choice([8, 9, 9]),
            minute=random.randint(0, 50),
        )

    if status == "Business Travel":
        return time(
            hour=random.choice([7, 8, 9]),
            minute=random.randint(0, 59),
        )

    return time(
        hour=random.choice([8, 8, 8, 9]),
        minute=random.randint(0, 45),
    )


def generate_clock_out(clock_in: time | None, status: str) -> time | None:
    """Generate clock-out time based on attendance type."""

    if clock_in is None:
        return None

    base_datetime = datetime.combine(date.today(), clock_in)

    if status == "Training":
        working_minutes = random.randint(6 * 60, 7 * 60)

    elif status == "Business Travel":
        working_minutes = random.randint(8 * 60, 10 * 60)

    else:
        working_minutes = random.randint(7 * 60, 9 * 60)

    clock_out_datetime = base_datetime + timedelta(
        minutes=working_minutes
    )

    return clock_out_datetime.time()


def calculate_hours(
    clock_in: time | None,
    clock_out: time | None,
) -> Decimal:
    """Calculate hours worked."""

    if clock_in is None or clock_out is None:
        return Decimal("0.00")

    start = datetime.combine(date.today(), clock_in)
    end = datetime.combine(date.today(), clock_out)

    hours = (end - start).seconds / 3600

    return Decimal(hours).quantize(Decimal("0.01"))


def calculate_overtime(hours_worked: Decimal) -> Decimal:
    """Calculate overtime above a 7.5 hour standard day."""

    return max(
        Decimal("0.00"),
        hours_worked - Decimal("7.50"),
    ).quantize(Decimal("0.01"))


def generate_attendance_for_employee(
    employee: Employee,
    start_date: date,
    end_date: date,
) -> list[Attendance]:
    """Generate attendance records for one employee."""

    records: list[Attendance] = []

    current_date = max(employee.hire_date, start_date)

    while current_date <= end_date:

        if is_weekend(current_date) or is_public_holiday(current_date):
            current_date += timedelta(days=1)
            continue

        status = random.choice(ATTENDANCE_STATUSES)

        clock_in = generate_clock_in(status)
        clock_out = generate_clock_out(clock_in, status)
        hours_worked = calculate_hours(clock_in, clock_out)
        overtime_hours = calculate_overtime(hours_worked)

        absence_reason = None

        if status == "Absent":
            absence_reason = random.choice(ABSENCE_REASONS)

        record = Attendance(
            employee=employee,
            work_date=current_date,
            status=status,
            clock_in_time=clock_in,
            clock_out_time=clock_out,
            hours_worked=hours_worked,
            overtime_hours=overtime_hours,
            absence_reason=absence_reason,
        )

        records.append(record)

        current_date += timedelta(days=1)

    return records


def generate_attendance(
    employees: list[Employee],
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[Attendance]:
    """Generate attendance records for all active employees."""

    if start_date is None:
        start_date = get_simulation_start_date()

    if end_date is None:
        end_date = date.today()

    records: list[Attendance] = []

    active_employees = [
        employee for employee in employees
        if employee.is_active
    ]

    for employee in active_employees:
        records.extend(
            generate_attendance_for_employee(
                employee=employee,
                start_date=start_date,
                end_date=end_date,
            )
        )

    return records