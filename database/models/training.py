"""
Training ORM model.

Stores employee learning, development and certification activity.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Training(Base):
    """Employee training record."""

    __tablename__ = "training"

    training_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    course_name: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
        index=True,
    )

    course_category: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    provider: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )

    start_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    completion_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    completion_status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="In Progress",
        index=True,
    )

    score: Mapped[Optional[float]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
    )

    training_hours: Mapped[float] = mapped_column(
        Numeric(6, 2),
        nullable=False,
        default=0,
    )

    cost: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    certification_awarded: Mapped[bool] = mapped_column(
        nullable=False,
        default=False,
    )

    employee: Mapped["Employee"] = relationship(
        back_populates="training_records"
    )

    def __repr__(self) -> str:
        return (
            f"Training("
            f"id={self.training_id}, "
            f"employee_id={self.employee_id}, "
            f"course='{self.course_name}', "
            f"status='{self.completion_status}'"
            f")"
        )