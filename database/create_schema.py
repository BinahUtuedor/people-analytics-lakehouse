"""
Create database schema.

This script creates all PostgreSQL tables defined in the SQLAlchemy ORM models.

Run from the project root:

    python database/create_schema.py
"""

from database.base import Base
from database.connection import engine
from config.logger import logger

# Important:
# Importing database.models registers all ORM models with Base.metadata.
import database.models  # noqa: F401


def create_schema() -> None:
    """
    Create all tables defined by SQLAlchemy models.
    """

    logger.info("Creating database schema...")

    
    logger.info(
    f"Registered tables: {list(Base.metadata.tables.keys())}"
    )

    Base.metadata.create_all(bind=engine)

    logger.info("Database schema created successfully.")


if __name__ == "__main__":
    create_schema()