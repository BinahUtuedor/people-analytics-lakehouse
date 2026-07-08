"""
Department ORM model.

Departments belong to a Business Unit.

Each Department contains many Employees.
"""

from __future__ import annotations

from typing import List

from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from database.base import Base


class Department(Base):
    """
    Department lookup table.
    """

    __tablename__ = "departments"

    # ---------------------------------------------------------
    # Primary Key
    # ---------------------------------------------------------

    department_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    # ---------------------------------------------------------
    # Foreign Keys
    # ---------------------------------------------------------

    business_unit_id: Mapped[int] = mapped_column(
        ForeignKey("business_units.business_unit_id"),
        nullable=False,
    )

    # ---------------------------------------------------------
    # Business Columns
    # ---------------------------------------------------------

    department_name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
    )

    cost_center: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        nullable=False,
    )

    # ---------------------------------------------------------
    # Relationships
    # ---------------------------------------------------------

    business_unit: Mapped["BusinessUnit"] = relationship(
        back_populates="departments"
    )

    employees: Mapped[List["Employee"]] = relationship(
        back_populates="department"
    )

    recruitments: Mapped[List["Recruitment"]] = relationship(
        back_populates="department"
    )

    def __repr__(self):

        return (
            f"Department("
            f"id={self.department_id}, "
            f"name='{self.department_name}')"
        )