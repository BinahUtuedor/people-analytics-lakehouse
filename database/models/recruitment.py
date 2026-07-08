"""
Recruitment ORM model.

Stores recruitment activity for departments and job roles.
This table supports hiring pipeline, time-to-hire and workforce planning analytics.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Recruitment(Base):
    """Recruitment vacancy or hiring campaign."""

    __tablename__ = "recruitment"

    recruitment_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    department_id: Mapped[int] = mapped_column(
        ForeignKey("departments.department_id"),
        nullable=False,
        index=True,
    )

    role_id: Mapped[int] = mapped_column(
        ForeignKey("job_roles.role_id"),
        nullable=False,
        index=True,
    )

    vacancy_reference: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        nullable=False,
        index=True,
    )

    candidate_name: Mapped[Optional[str]] = mapped_column(
        String(150),
        nullable=True,
    )

    opening_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    closing_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    hire_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    recruitment_status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="Open",
        index=True,
    )

    source_channel: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )

    number_of_applicants: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    number_shortlisted: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    number_interviewed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    recruitment_cost: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=0,
    )

    successful_employee_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    department: Mapped["Department"] = relationship(
        back_populates="recruitments"
    )

    job_role: Mapped["JobRole"] = relationship(
        back_populates="recruitments"
    )

    successful_employee: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[successful_employee_id],
    )

    def __repr__(self) -> str:
        return (
            f"Recruitment("
            f"id={self.recruitment_id}, "
            f"reference='{self.vacancy_reference}', "
            f"status='{self.recruitment_status}'"
            f")"
        )