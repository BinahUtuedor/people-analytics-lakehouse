"""
Employee Survey ORM model.

Stores structured engagement scores and unstructured survey comments.
This supports engagement analytics, sentiment analysis and attrition modelling.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class EmployeeSurvey(Base):
    """Employee engagement or satisfaction survey response."""

    __tablename__ = "employee_surveys"

    survey_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    survey_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    survey_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="Engagement",
    )

    engagement_score: Mapped[float] = mapped_column(
        Numeric(4, 2),
        nullable=False,
    )

    satisfaction_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    wellbeing_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    manager_support_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    work_life_balance_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    career_growth_score: Mapped[Optional[float]] = mapped_column(
        Numeric(4, 2),
        nullable=True,
    )

    free_text_response: Mapped[Optional[str]] = mapped_column(
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
        back_populates="surveys"
    )

    def __repr__(self) -> str:
        return (
            f"EmployeeSurvey("
            f"id={self.survey_id}, "
            f"employee_id={self.employee_id}, "
            f"engagement_score={self.engagement_score}"
            f")"
        )