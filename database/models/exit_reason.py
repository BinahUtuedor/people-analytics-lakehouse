"""
Exit reason reference-data ORM model.

Stores employee exit categories together with the possible detailed
reasons that may be generated for each category.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Boolean, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class ExitReason(Base):
    """Reference definition for an employee exit category."""

    __tablename__ = "exit_reasons"

    exit_reason_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    exit_reason_code: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
    )

    exit_reason_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
    )

    voluntary: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
    )

    weight: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
    )

    reasons: Mapped[list[Any]] = mapped_column(
        JSON,
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            "ExitReason("
            f"id={self.exit_reason_id}, "
            f"code='{self.exit_reason_code}', "
            f"name='{self.exit_reason_name}', "
            f"voluntary={self.voluntary}, "
            f"weight={self.weight}"
            ")"
        )