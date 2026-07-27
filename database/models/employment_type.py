"""
Employment type reference-data ORM model.

Stores the controlled employment-type values used by the employee
and recruitment simulators.
"""

from __future__ import annotations

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class EmploymentType(Base):
    """Reference definition for an employment type."""

    __tablename__ = "employment_types"

    employment_type_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employment_type_code: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
    )

    employment_type_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )

    description: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            "EmploymentType("
            f"id={self.employment_type_id}, "
            f"code='{self.employment_type_code}', "
            f"name='{self.employment_type_name}'"
            ")"
        )