"""
Training category reference-data ORM model.

Stores the controlled training categories used by the training simulator.
"""

from __future__ import annotations

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from database.base import Base


class TrainingCategory(Base):
    """Reference definition for a training category."""

    __tablename__ = "training_categories"

    training_category_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    training_category_code: Mapped[str] = mapped_column(
        String(60),
        nullable=False,
        unique=True,
        index=True,
    )

    training_category_name: Mapped[str] = mapped_column(
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
            "TrainingCategory("
            f"id={self.training_category_id}, "
            f"code='{self.training_category_code}', "
            f"name='{self.training_category_name}'"
            ")"
        )