"""
Leave type reference-data ORM model.

Stores the controlled leave types used by the leave-request simulator.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class LeaveType(Base):
    """Reference definition for an employee leave type."""

    __tablename__ = "leave_types"

    leave_type_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    leave_type_code: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
    )

    leave_type_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )

    description: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    paid: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )

    requires_approval: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )

    def __repr__(self) -> str:
        return (
            "LeaveType("
            f"id={self.leave_type_id}, "
            f"code='{self.leave_type_code}', "
            f"name='{self.leave_type_name}'"
            ")"
        )