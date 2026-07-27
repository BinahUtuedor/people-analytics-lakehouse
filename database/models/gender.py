"""
Gender reference-data ORM model.

Stores the controlled gender values used by the employee simulator.
"""

from __future__ import annotations

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class Gender(Base):
    """Reference definition for a gender value."""

    __tablename__ = "genders"

    gender_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    gender_code: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
    )

    gender_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            "Gender("
            f"id={self.gender_id}, "
            f"code='{self.gender_code}', "
            f"name='{self.gender_name}'"
            ")"
        )