"""
Exit Interview ORM model.

Stores exit interview information for employees who leave the organisation.
Each employee should normally have zero or one exit interview.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class ExitInterview(Base):
    """Employee exit interview record."""

    __tablename__ = "exit_interviews"

    exit_interview_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        unique=True,
        nullable=False,
        index=True,
    )

    termination_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    exit_reason: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
    )

    voluntary_exit: Mapped[bool] = mapped_column(
        nullable=False,
        default=True,
    )

    destination_type: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )

    satisfaction_at_exit: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    likelihood_to_recommend: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    interview_text: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    key_themes: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    sentiment_label: Mapped[Optional[str]] = mapped_column(
        String(30),
        nullable=True,
    )

    sentiment_score: Mapped[Optional[float]] = mapped_column(
        Numeric(5, 4),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        back_populates="exit_interview"
    )

    def __repr__(self) -> str:
        return (
            f"ExitInterview("
            f"id={self.exit_interview_id}, "
            f"employee_id={self.employee_id}, "
            f"reason='{self.exit_reason}'"
            f")"
        )