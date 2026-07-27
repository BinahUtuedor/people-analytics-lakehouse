"""
Public holiday reference-data ORM model.

Stores recurring public-holiday definitions used by the attendance
simulator.

The current implementation uses month_day in MM-DD format so the same
holiday definition can be applied across simulation years.

Example:

    01-01 -> New Year's Day
    12-25 -> Christmas Day
    12-26 -> Boxing Day
"""

from __future__ import annotations

from sqlalchemy import Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class PublicHoliday(Base):
    """Reference definition for a recurring public holiday."""

    __tablename__ = "public_holidays"

    public_holiday_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    holiday_code: Mapped[str] = mapped_column(
        String(60),
        nullable=False,
        unique=True,
        index=True,
    )

    holiday_name: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
        unique=True,
        index=True,
    )

    month_day: Mapped[str] = mapped_column(
        String(5),
        nullable=False,
        index=True,
    )

    country_code: Mapped[str] = mapped_column(
        String(2),
        nullable=False,
        default="GB",
        index=True,
    )

    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
    )

    def __repr__(self) -> str:
        return (
            "PublicHoliday("
            f"id={self.public_holiday_id}, "
            f"code='{self.holiday_code}', "
            f"name='{self.holiday_name}', "
            f"month_day='{self.month_day}', "
            f"country='{self.country_code}'"
            ")"
        )