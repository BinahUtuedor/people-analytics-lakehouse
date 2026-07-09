"""
Payroll simulation module.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from config.constants import HISTORICAL_YEARS
from database.models import Employee, Payroll


def generate_month_starts() -> list[date]:
    today = date.today()
    start_year = today.year - HISTORICAL_YEARS

    months = []

    for year in range(start_year, today.year + 1):
        for month in range(1, 13):
            month_start = date(year, month, 1)

            if month_start <= today:
                months.append(month_start)

    return months


def get_month_end(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year, 12, 31)

    next_month = date(month_start.year, month_start.month + 1, 1)

    return date.fromordinal(next_month.toordinal() - 1)


def generate_payroll(employees: list[Employee]) -> list[Payroll]:
    records = []

    for employee in employees:
        for month_start in generate_month_starts():
            if month_start < employee.hire_date.replace(day=1):
                continue

            monthly_salary = Decimal(employee.annual_salary) / Decimal("12")
            bonus = monthly_salary * Decimal("0.03")
            pension = monthly_salary * Decimal("0.05")
            tax = monthly_salary * Decimal("0.20")
            deductions = pension + tax
            gross_pay = monthly_salary + bonus
            net_pay = gross_pay - deductions

            records.append(
                Payroll(
                    employee=employee,
                    pay_period_start=month_start,
                    pay_period_end=get_month_end(month_start),
                    base_salary=monthly_salary.quantize(Decimal("0.01")),
                    overtime_pay=Decimal("0.00"),
                    bonus=bonus.quantize(Decimal("0.01")),
                    deductions=deductions.quantize(Decimal("0.01")),
                    pension_contribution=pension.quantize(Decimal("0.01")),
                    tax_amount=tax.quantize(Decimal("0.01")),
                    gross_pay=gross_pay.quantize(Decimal("0.01")),
                    net_pay=net_pay.quantize(Decimal("0.01")),
                    currency="GBP",
                    payroll_status="Processed",
                )
            )

    return records