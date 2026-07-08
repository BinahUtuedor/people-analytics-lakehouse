"""
Promotion ORM model.

Stores employee promotion history, including old and new job roles,
salary movement, effective date and approval details.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Promotion(Base):
    """Employee promotion event."""

    __tablename__ = "promotions"

    promotion_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    old_role_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("job_roles.role_id"),
        nullable=True,
        index=True,
    )

    new_role_id: Mapped[int] = mapped_column(
        ForeignKey("job_roles.role_id"),
        nullable=False,
        index=True,
    )

    promotion_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    old_salary: Mapped[Optional[float]] = mapped_column(
        Numeric(12, 2),
        nullable=True,
    )

    new_salary: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    salary_increase_amount: Mapped[Optional[float]] = mapped_column(
        Numeric(12, 2),
        nullable=True,
    )

    salary_increase_percent: Mapped[Optional[float]] = mapped_column(
        Numeric(5, 2),
        nullable=True,
    )

    promotion_reason: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
    )

    approved_by_manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    employee: Mapped["Employee"] = relationship(
        foreign_keys=[employee_id],
        back_populates="promotions",
    )

    old_role: Mapped[Optional["JobRole"]] = relationship(
        foreign_keys=[old_role_id],
    )

    new_role: Mapped["JobRole"] = relationship(
        foreign_keys=[new_role_id],
    )

    approved_by_manager: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[approved_by_manager_id],
    )

    def __repr__(self) -> str:
        return (
            f"Promotion("
            f"id={self.promotion_id}, "
            f"employee_id={self.employee_id}, "
            f"date={self.promotion_date}"
            f")"
        )
    