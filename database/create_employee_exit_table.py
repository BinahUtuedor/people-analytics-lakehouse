"""
Create the EmployeeExit table in an existing database.

Run from project root:

    python -m database.create_employee_exit_table

This creates only the missing employee_exits table.
"""

from __future__ import annotations

from config.logger import logger
from database.base import Base
from database.connection import engine

# Register the model with Base.metadata.
from database.models.employee_exit import EmployeeExit  # noqa: F401


def create_employee_exit_table() -> None:
    """Create employee_exits if it does not already exist."""

    logger.info("Creating employee_exits table if missing...")

    Base.metadata.create_all(
        bind=engine,
        tables=[Base.metadata.tables["employee_exits"]],
    )

    logger.info("employee_exits table is ready.")


if __name__ == "__main__":
    create_employee_exit_table()