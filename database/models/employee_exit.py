"""
Employee exit ORM model.

Stores one employment-exit event per employee while Employee remains
the current-state master record.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import Boolean, Date, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class EmployeeExit(Base):
    """Employment exit event."""

    __tablename__ = "employee_exits"

    exit_event_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        unique=True,
        index=True,
    )

    exit_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    exit_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
    )

    exit_reason: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    voluntary_flag: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
    )

    regrettable_flag: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
    )

    # Unidirectional in Phase 1 to avoid forcing an immediate rewrite
    # of the existing Employee model.
    employee: Mapped["Employee"] = relationship("Employee")

    def __repr__(self) -> str:
        return (
            f"EmployeeExit("
            f"id={self.exit_event_id}, "
            f"employee_id={self.employee_id}, "
            f"exit_date={self.exit_date}, "
            f"exit_type='{self.exit_type}'"
            f")"
        )