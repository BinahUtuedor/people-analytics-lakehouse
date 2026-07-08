"""
Job Role ORM model.

Defines organisational job roles.

One Job Role can be assigned to many Employees.
"""

from __future__ import annotations

from typing import List

from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from database.base import Base


class JobRole(Base):

    __tablename__ = "job_roles"

    # ---------------------------------------------------------
    # Primary Key
    # ---------------------------------------------------------

    role_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    # ---------------------------------------------------------
    # Business Columns
    # ---------------------------------------------------------

    role_name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
    )

    grade: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    salary_band_min: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    salary_band_max: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    # ---------------------------------------------------------
    # Relationships
    # ---------------------------------------------------------

    employees: Mapped[List["Employee"]] = relationship(
        back_populates="job_role"
    )

    recruitments: Mapped[List["Recruitment"]] = relationship(
        back_populates="job_role"
    )

    def __repr__(self):

        return (
            f"JobRole("
            f"id={self.role_id}, "
            f"role='{self.role_name}')"
        )