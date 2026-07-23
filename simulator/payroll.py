"""
Payroll simulation module.

Generates monthly payroll records with:
- partial first-month pay;
- partial final-month pay for terminated employees;
- overtime for selected employees;
- no payroll periods outside the employee's employment window.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from config.constants import (
    DEFAULT_RANDOM_SEED,
    HISTORICAL_YEARS,
)
from database.models import Employee, Payroll


random.seed(DEFAULT_RANDOM_SEED)


STANDARD_WEEKLY_HOURS = Decimal("37.50")
WEEKS_PER_YEAR = Decimal("52")
OVERTIME_MULTIPLIER = Decimal("1.50")
OVERTIME_PROBABILITY = 0.15
MIN_OVERTIME_HOURS = 2
MAX_OVERTIME_HOURS = 20


def money(
    value: Decimal,
) -> Decimal:
    """Round a monetary value to two decimal places."""

    return value.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def generate_month_starts() -> list[date]:
    """Generate historical payroll month starts through the current month."""

    today = date.today()
    start_year = (
        today.year
        - HISTORICAL_YEARS
    )

    months: list[date] = []

    for year in range(
        start_year,
        today.year + 1,
    ):
        for month in range(
            1,
            13,
        ):
            month_start = date(
                year,
                month,
                1,
            )

            if month_start <= today:
                months.append(
                    month_start
                )

    return months


def get_month_end(
    month_start: date,
) -> date:
    """Return the final calendar day of the month."""

    if month_start.month == 12:
        return date(
            month_start.year,
            12,
            31,
        )

    next_month = date(
        month_start.year,
        month_start.month + 1,
        1,
    )

    return date.fromordinal(
        next_month.toordinal() - 1
    )


def calculate_payable_base_salary(
    employee: Employee,
    month_start: date,
    month_end: date,
    monthly_salary: Decimal,
) -> tuple[Decimal, date, date]:
    """
    Calculate base salary for the portion of the month actually employed.

    This handles both:
    - employees starting after the first day of a month;
    - employees terminating before the final day of a month.

    Returns:
        payable salary,
        effective payroll start,
        effective payroll end.
    """

    effective_start = max(
        month_start,
        employee.hire_date,
    )

    effective_end = min(
        month_end,
        employee.termination_date
        if employee.termination_date is not None
        else month_end,
        date.today(),
    )

    if effective_start > effective_end:
        return (
            Decimal("0.00"),
            effective_start,
            effective_end,
        )

    total_days_in_month = Decimal(
        str(month_end.day)
    )

    payable_days = Decimal(
        str(
            (
                effective_end
                - effective_start
            ).days
            + 1
        )
    )

    prorated_salary = (
        monthly_salary
        * payable_days
        / total_days_in_month
    )

    return (
        money(prorated_salary),
        effective_start,
        effective_end,
    )


def calculate_overtime_pay(
    employee: Employee,
    base_salary: Decimal,
    monthly_salary: Decimal,
) -> Decimal:
    """Generate overtime for selected payroll periods."""

    if (
        random.random()
        >= OVERTIME_PROBABILITY
    ):
        return Decimal("0.00")

    annual_salary = Decimal(
        str(employee.annual_salary)
    )

    hourly_rate = (
        annual_salary
        / WEEKS_PER_YEAR
        / STANDARD_WEEKLY_HOURS
    )

    overtime_hours = Decimal(
        str(
            random.randint(
                MIN_OVERTIME_HOURS,
                MAX_OVERTIME_HOURS,
            )
        )
    )

    proration_ratio = (
        base_salary
        / monthly_salary
        if monthly_salary
        > Decimal("0.00")
        else Decimal("1.00")
    )

    overtime_pay = (
        overtime_hours
        * proration_ratio
        * hourly_rate
        * OVERTIME_MULTIPLIER
    )

    return money(
        overtime_pay
    )


def generate_payroll(
    employees: list[Employee],
) -> list[Payroll]:
    """
    Generate payroll only for periods during active employment.

    Final-month payroll is prorated through termination_date and its
    pay_period_end is set to termination_date.
    """

    records: list[Payroll] = []

    today = date.today()

    for employee in employees:
        employment_end = (
            employee.termination_date
            if employee.termination_date
            is not None
            else today
        )

        for month_start in generate_month_starts():
            month_end = get_month_end(
                month_start
            )

            # Skip months entirely before hire or after termination.
            if month_end < employee.hire_date:
                continue

            if month_start > employment_end:
                continue

            monthly_salary = (
                Decimal(
                    str(
                        employee.annual_salary
                    )
                )
                / Decimal("12")
            )

            (
                base_salary,
                effective_start,
                effective_end,
            ) = calculate_payable_base_salary(
                employee=employee,
                month_start=month_start,
                month_end=month_end,
                monthly_salary=monthly_salary,
            )

            if base_salary <= Decimal("0.00"):
                continue

            overtime_pay = calculate_overtime_pay(
                employee=employee,
                base_salary=base_salary,
                monthly_salary=monthly_salary,
            )

            bonus = (
                base_salary
                * Decimal("0.03")
            )

            pension = (
                base_salary
                * Decimal("0.05")
            )

            tax = (
                base_salary
                * Decimal("0.20")
            )

            deductions = (
                pension
                + tax
            )

            gross_pay = (
                base_salary
                + overtime_pay
                + bonus
            )

            net_pay = (
                gross_pay
                - deductions
            )

            records.append(
                Payroll(
                    employee=employee,
                    # Effective payroll boundaries ensure no record
                    # starts before hire or ends after termination.
                    pay_period_start=effective_start,
                    pay_period_end=effective_end,
                    base_salary=money(
                        base_salary
                    ),
                    overtime_pay=money(
                        overtime_pay
                    ),
                    bonus=money(
                        bonus
                    ),
                    deductions=money(
                        deductions
                    ),
                    pension_contribution=money(
                        pension
                    ),
                    tax_amount=money(
                        tax
                    ),
                    gross_pay=money(
                        gross_pay
                    ),
                    net_pay=money(
                        net_pay
                    ),
                    currency="GBP",
                    payroll_status="Processed",
                )
            )

    return records