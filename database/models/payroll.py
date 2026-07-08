"""
Payroll ORM model.

Stores monthly payroll records for employees.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Payroll(Base):
    """Monthly payroll transaction."""

    __tablename__ = "payroll"

    payroll_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    pay_period_start: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    pay_period_end: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    base_salary: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    overtime_pay: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    bonus: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    deductions: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    pension_contribution: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    tax_amount: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    gross_pay: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    net_pay: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    currency: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="GBP",
    )

    payroll_status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="Processed",
    )

    notes: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        back_populates="payroll_records"
    )

    def __repr__(self) -> str:
        return (
            f"Payroll("
            f"id={self.payroll_id}, "
            f"employee_id={self.employee_id}, "
            f"period='{self.pay_period_start} to {self.pay_period_end}', "
            f"net_pay={self.net_pay}"
            f")"
        )