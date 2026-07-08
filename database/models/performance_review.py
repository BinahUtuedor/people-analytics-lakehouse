"""
Performance Review ORM model.

Stores periodic employee performance review outcomes.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class PerformanceReview(Base):
    """Employee performance review record."""

    __tablename__ = "performance_reviews"

    review_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    reviewer_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    review_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    review_period: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    overall_rating: Mapped[float] = mapped_column(
        Numeric(3, 2),
        nullable=False,
    )

    productivity_score: Mapped[Optional[float]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
    )

    quality_score: Mapped[Optional[float]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
    )

    teamwork_score: Mapped[Optional[float]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
    )

    leadership_score: Mapped[Optional[float]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
    )

    strengths: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
    )

    development_areas: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
    )

    promotion_recommended: Mapped[bool] = mapped_column(
        nullable=False,
        default=False,
    )

    retention_risk: Mapped[Optional[str]] = mapped_column(
        String(30),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        foreign_keys=[employee_id],
        back_populates="performance_reviews",
    )

    reviewer: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[reviewer_id],
    )

    def __repr__(self) -> str:
        return (
            f"PerformanceReview("
            f"id={self.review_id}, "
            f"employee_id={self.employee_id}, "
            f"rating={self.overall_rating}, "
            f"date={self.review_date}"
            f")"
        )