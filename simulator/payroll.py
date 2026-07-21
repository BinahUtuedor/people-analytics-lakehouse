"""
Payroll simulation module.

This module generates monthly payroll records for employees across the
configured historical period.

Enhancements included:
- Employees who start after the first day of a month receive prorated
  base pay for their first payroll period.
- A selected proportion of employees receive simulated overtime pay.
- Overtime is calculated using an implied hourly rate and an overtime
  multiplier.

All other payroll calculations remain consistent with the original
implementation.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from config.constants import DEFAULT_RANDOM_SEED, HISTORICAL_YEARS
from database.models import Employee, Payroll


# -------------------------------------------------------------------
# Random seed
#
# Ensures that overtime selection and overtime hours are reproducible
# when the simulator is run using the same configured seed.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Payroll assumptions
# -------------------------------------------------------------------

# Standard number of contracted working hours per week.
STANDARD_WEEKLY_HOURS = Decimal("37.50")

# Number of weeks used to calculate the employee's hourly rate.
WEEKS_PER_YEAR = Decimal("52")

# Overtime is paid at one-and-a-half times the normal hourly rate.
OVERTIME_MULTIPLIER = Decimal("1.50")

# Approximately 15% of payroll records will contain overtime.
OVERTIME_PROBABILITY = 0.15

# Selected employees work between 2 and 20 overtime hours
# during the relevant payroll period.
MIN_OVERTIME_HOURS = 2
MAX_OVERTIME_HOURS = 20


def money(value: Decimal) -> Decimal:
    """
    Round a monetary value to two decimal places.

    ROUND_HALF_UP is used to provide conventional financial rounding.

    Args:
        value:
            Decimal monetary amount.

    Returns:
        Decimal:
            Value rounded to two decimal places.
    """

    return value.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def generate_month_starts() -> list[date]:
    """
    Generate the first day of every payroll month in the historical period.

    Returns:
        list[date]:
            Monthly payroll period start dates.
    """

    today = date.today()
    start_year = today.year - HISTORICAL_YEARS

    months: list[date] = []

    for year in range(start_year, today.year + 1):
        for month in range(1, 13):
            month_start = date(
                year,
                month,
                1,
            )

            # Do not create payroll periods beginning after today.
            if month_start <= today:
                months.append(
                    month_start
                )

    return months


def get_month_end(month_start: date) -> date:
    """
    Return the final calendar day of a payroll month.

    Args:
        month_start:
            First calendar day of the month.

    Returns:
        date:
            Final calendar day of the same month.
    """

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


def calculate_prorated_base_salary(
    employee: Employee,
    month_start: date,
    month_end: date,
    monthly_salary: Decimal,
) -> Decimal:
    """
    Calculate base salary for the payroll period.

    Employees receive their full monthly salary except during the month
    in which they are hired.

    Where an employee starts after the first day of the month, their
    salary is prorated using calendar days:

        payable days / total calendar days in month

    Example:

        Monthly salary: £3,000
        Hire date:      16 July
        Days in July:   31
        Payable days:   16

        Prorated salary = £3,000 × 16 / 31

    Args:
        employee:
            Employee receiving payroll.

        month_start:
            First day of the payroll period.

        month_end:
            Last day of the payroll period.

        monthly_salary:
            Employee's normal full monthly salary.

    Returns:
        Decimal:
            Full or prorated base salary.
    """

    employee_hire_month = employee.hire_date.replace(
        day=1
    )

    # Every month after the employee's first month receives full pay.
    if month_start != employee_hire_month:
        return money(
            monthly_salary
        )

    # An employee who starts on the first day of the month receives
    # their full monthly salary.
    if employee.hire_date.day == 1:
        return money(
            monthly_salary
        )

    # Number of calendar days in the payroll month.
    total_days_in_month = Decimal(
        str(month_end.day)
    )

    # Include the employee's hire date as a payable day.
    payable_days = Decimal(
        str(
            (
                month_end
                - employee.hire_date
            ).days
            + 1
        )
    )

    prorated_salary = (
        monthly_salary
        * payable_days
        / total_days_in_month
    )

    return money(
        prorated_salary
    )


def calculate_overtime_pay(
    employee: Employee,
    base_salary: Decimal,
    monthly_salary: Decimal,
) -> Decimal:
    """
    Generate overtime pay for selected payroll records.

    Approximately 15% of payroll records receive overtime.

    The normal hourly rate is calculated as:

        annual salary / 52 weeks / 37.5 weekly hours

    Overtime is paid at 1.5 times this hourly rate.

    For an employee receiving prorated first-month pay, overtime is still
    permitted but is scaled using the same proration ratio. This prevents
    a partial-month employee from receiving an unrealistic full-month
    overtime allocation.

    Args:
        employee:
            Employee receiving payroll.

        base_salary:
            Base salary payable for the current month.

        monthly_salary:
            Employee's normal full monthly salary.

    Returns:
        Decimal:
            Calculated overtime pay, or zero where no overtime is selected.
    """

    # Most payroll periods do not include overtime.
    if random.random() >= OVERTIME_PROBABILITY:
        return Decimal("0.00")

    annual_salary = Decimal(
        str(employee.annual_salary)
    )

    # Calculate the employee's standard hourly rate.
    hourly_rate = (
        annual_salary
        / WEEKS_PER_YEAR
        / STANDARD_WEEKLY_HOURS
    )

    # Generate overtime hours for this payroll period.
    overtime_hours = Decimal(
        str(
            random.randint(
                MIN_OVERTIME_HOURS,
                MAX_OVERTIME_HOURS,
            )
        )
    )

    # If the employee receives partial first-month pay, reduce the
    # maximum effective overtime using the same proration ratio.
    if monthly_salary > Decimal("0.00"):
        proration_ratio = (
            base_salary
            / monthly_salary
        )
    else:
        proration_ratio = Decimal("1.00")

    adjusted_overtime_hours = (
        overtime_hours
        * proration_ratio
    )

    overtime_pay = (
        adjusted_overtime_hours
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
    Generate monthly payroll records for all employees.

    Payroll starts in the month in which the employee was hired.

    Employees starting after the first day of their first month receive
    prorated base pay. Selected payroll records also receive overtime pay.

    Args:
        employees:
            List of Employee ORM objects.

    Returns:
        list[Payroll]:
            Generated monthly payroll records.
    """

    records: list[Payroll] = []

    for employee in employees:
        for month_start in generate_month_starts():

            # Do not generate payroll before the employee's hire month.
            if month_start < employee.hire_date.replace(
                day=1
            ):
                continue

            # Determine the final day of the payroll period.
            month_end = get_month_end(
                month_start
            )

            # Calculate the employee's normal full monthly salary.
            monthly_salary = (
                Decimal(
                    str(employee.annual_salary)
                )
                / Decimal("12")
            )

            # -----------------------------------------------------------
            # Calculate partial first-month salary where applicable.
            # -----------------------------------------------------------

            base_salary = calculate_prorated_base_salary(
                employee=employee,
                month_start=month_start,
                month_end=month_end,
                monthly_salary=monthly_salary,
            )

            # -----------------------------------------------------------
            # Calculate overtime for selected payroll records.
            # -----------------------------------------------------------

            overtime_pay = calculate_overtime_pay(
                employee=employee,
                base_salary=base_salary,
                monthly_salary=monthly_salary,
            )

            # -----------------------------------------------------------
            # Existing payroll calculations.
            #
            # These percentages remain unchanged from the original code.
            # They are now applied to the payable base salary, which may
            # be prorated during the employee's first month.
            # -----------------------------------------------------------

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

            # Overtime is included in gross pay.
            gross_pay = (
                base_salary
                + overtime_pay
                + bonus
            )

            net_pay = (
                gross_pay
                - deductions
            )

            # -----------------------------------------------------------
            # Create Payroll ORM record.
            # -----------------------------------------------------------

            records.append(
                Payroll(
                    employee=employee,

                    pay_period_start=month_start,

                    pay_period_end=month_end,

                    # Full or prorated monthly salary.
                    base_salary=money(
                        base_salary
                    ),

                    # Overtime is zero for most records and populated
                    # for selected payroll periods.
                    overtime_pay=money(
                        overtime_pay
                    ),

                    # Existing 3% bonus calculation.
                    bonus=money(
                        bonus
                    ),

                    # Existing combined deductions.
                    deductions=money(
                        deductions
                    ),

                    # Existing 5% pension calculation.
                    pension_contribution=money(
                        pension
                    ),

                    # Existing 20% tax calculation.
                    tax_amount=money(
                        tax
                    ),

                    # Base salary, overtime and bonus.
                    gross_pay=money(
                        gross_pay
                    ),

                    # Gross pay less pension and tax deductions.
                    net_pay=money(
                        net_pay
                    ),

                    currency="GBP",

                    payroll_status="Processed",
                )
            )

    return records
