"""
Manager Feedback ORM model.

Stores qualitative and quantitative manager feedback about employees.
This table supports performance analytics, sentiment analysis and management insight.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class ManagerFeedback(Base):
    """Feedback given by a manager to or about an employee."""

    __tablename__ = "manager_feedback"

    feedback_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    manager_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    feedback_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    feedback_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="General",
    )

    feedback_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    comments: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    strengths: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    improvement_areas: Mapped[Optional[str]] = mapped_column(
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
        foreign_keys=[employee_id],
        back_populates="feedback_received",
    )

    manager: Mapped["Employee"] = relationship(
        foreign_keys=[manager_id],
        back_populates="feedback_given",
    )

    def __repr__(self) -> str:
        return (
            f"ManagerFeedback("
            f"id={self.feedback_id}, "
            f"employee_id={self.employee_id}, "
            f"manager_id={self.manager_id}"
            f")"
        )