"""
Business Unit ORM model.

Business Units represent the highest organisational grouping within the
company (e.g. Technology, Finance, Operations).

One Business Unit can contain many Departments.
"""

from __future__ import annotations

from typing import List

from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from database.base import Base


class BusinessUnit(Base):
    """
    Business Unit lookup table.
    """

    __tablename__ = "business_units"

    # ---------------------------------------------------------
    # Primary Key
    # ---------------------------------------------------------

    business_unit_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    # ---------------------------------------------------------
    # Business Columns
    # ---------------------------------------------------------

    unit_name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
    )

    description: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    # ---------------------------------------------------------
    # Relationships
    # ---------------------------------------------------------

    departments: Mapped[List["Department"]] = relationship(
        back_populates="business_unit",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:

        return (
            f"BusinessUnit("
            f"id={self.business_unit_id}, "
            f"name='{self.unit_name}')"
        )