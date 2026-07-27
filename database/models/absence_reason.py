"""
Absence reason reference-data ORM model.

Stores the controlled absence reasons used when attendance status is
'Absent'.
"""

from __future__ import annotations

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class AbsenceReason(Base):
    """Reference definition for an attendance absence reason."""

    __tablename__ = "absence_reasons"

    absence_reason_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    absence_reason_code: Mapped[str] = mapped_column(
        String(60),
        nullable=False,
        unique=True,
        index=True,
    )

    absence_reason_name: Mapped[str] = mapped_column(
        String(120),
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
            "AbsenceReason("
            f"id={self.absence_reason_id}, "
            f"code='{self.absence_reason_code}', "
            f"name='{self.absence_reason_name}'"
            ")"
        )