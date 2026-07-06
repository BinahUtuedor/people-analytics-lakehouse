"""
Base class for all SQLAlchemy ORM models.

Every database table inherits from Base.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base ORM model."""
    pass