"""
Attendance ORM model.

Stores daily attendance and working-hour records for employees.
"""

from __future__ import annotations

from datetime import date, time
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Time
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Attendance(Base):
    """Daily employee attendance record."""

    __tablename__ = "attendance"

    attendance_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    work_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="Present",
    )

    clock_in_time: Mapped[Optional[time]] = mapped_column(
        Time,
        nullable=True,
    )

    clock_out_time: Mapped[Optional[time]] = mapped_column(
        Time,
        nullable=True,
    )

    hours_worked: Mapped[float] = mapped_column(
        Numeric(5, 2),
        nullable=False,
        default=0,
    )

    overtime_hours: Mapped[float] = mapped_column(
        Numeric(5, 2),
        nullable=False,
        default=0,
    )

    absence_reason: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        back_populates="attendance_records"
    )

    def __repr__(self) -> str:
        return (
            f"Attendance("
            f"id={self.attendance_id}, "
            f"employee_id={self.employee_id}, "
            f"date={self.work_date}, "
            f"status='{self.status}'"
            f")"
        )