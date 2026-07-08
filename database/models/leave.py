"""
Leave Request ORM model.

Stores employee annual leave, sick leave, parental leave and other absence requests.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class LeaveRequest(Base):
    """Employee leave request record."""

    __tablename__ = "leave_requests"

    leave_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    leave_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
    )

    start_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    end_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    number_of_days: Mapped[float] = mapped_column(
        Numeric(5, 2),
        nullable=False,
    )

    request_status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="Pending",
        index=True,
    )

    requested_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    approved_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    approved_by_manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    reason: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        foreign_keys=[employee_id],
        back_populates="leave_requests",
    )

    approved_by_manager: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[approved_by_manager_id],
    )

    def __repr__(self) -> str:
        return (
            f"LeaveRequest("
            f"id={self.leave_id}, "
            f"employee_id={self.employee_id}, "
            f"type='{self.leave_type}', "
            f"status='{self.request_status}'"
            f")"
        )