"""
Transfer ORM model.

Stores employee movement between departments, locations or managers.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Transfer(Base):
    """Employee transfer event."""

    __tablename__ = "transfers"

    transfer_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=False,
        index=True,
    )

    old_department_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("departments.department_id"),
        nullable=True,
        index=True,
    )

    new_department_id: Mapped[int] = mapped_column(
        ForeignKey("departments.department_id"),
        nullable=False,
        index=True,
    )

    old_location_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("locations.location_id"),
        nullable=True,
        index=True,
    )

    new_location_id: Mapped[int] = mapped_column(
        ForeignKey("locations.location_id"),
        nullable=False,
        index=True,
    )

    old_manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    new_manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    transfer_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    transfer_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="Internal Transfer",
    )

    transfer_reason: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
    )

    employee: Mapped["Employee"] = relationship(
        foreign_keys=[employee_id],
        back_populates="transfers",
    )

    old_department: Mapped[Optional["Department"]] = relationship(
        foreign_keys=[old_department_id],
    )

    new_department: Mapped["Department"] = relationship(
        foreign_keys=[new_department_id],
    )

    old_location: Mapped[Optional["Location"]] = relationship(
        foreign_keys=[old_location_id],
    )

    new_location: Mapped["Location"] = relationship(
        foreign_keys=[new_location_id],
    )

    old_manager: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[old_manager_id],
    )

    new_manager: Mapped[Optional["Employee"]] = relationship(
        foreign_keys=[new_manager_id],
    )

    def __repr__(self) -> str:
        return (
            f"Transfer("
            f"id={self.transfer_id}, "
            f"employee_id={self.employee_id}, "
            f"date={self.transfer_date}"
            f")"
        )