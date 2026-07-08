"""
Location ORM model.

Represents a physical office location.

One Location can contain many Employees.
"""

from __future__ import annotations

from typing import List

from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from database.base import Base


class Location(Base):

    __tablename__ = "locations"

    # ---------------------------------------------------------
    # Primary Key
    # ---------------------------------------------------------

    location_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    # ---------------------------------------------------------
    # Business Columns
    # ---------------------------------------------------------

    country: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    city: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    office_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    timezone: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    # ---------------------------------------------------------
    # Relationships
    # ---------------------------------------------------------

    employees: Mapped[List["Employee"]] = relationship(
        back_populates="location"
    )

    def __repr__(self):

        return (
            f"Location("
            f"id={self.location_id}, "
            f"office='{self.office_name}')"
        )