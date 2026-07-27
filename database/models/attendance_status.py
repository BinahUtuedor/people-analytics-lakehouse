"""
Attendance status reference-data ORM model.

Stores the controlled vocabulary used by the attendance simulator,
including the probability weight and behavioural rules associated
with each attendance status.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class AttendanceStatus(Base):
    """Reference definition for an attendance status."""

    __tablename__ = "attendance_statuses"

    attendance_status_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    attendance_status_code: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
    )

    attendance_status_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )

    weight: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
    )

    requires_clock_times: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )

    absence_reason_required: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )

    def __repr__(self) -> str:
        return (
            "AttendanceStatus("
            f"id={self.attendance_status_id}, "
            f"code='{self.attendance_status_code}', "
            f"name='{self.attendance_status_name}', "
            f"weight={self.weight}"
            ")"
        )