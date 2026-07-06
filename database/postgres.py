"""
PostgreSQL connectivity tests.
"""

from sqlalchemy import text

from database.connection import engine
from config.logger import logger


def test_connection() -> bool:
    """
    Test connectivity to PostgreSQL.
    """

    try:

        with engine.connect() as connection:

            result = connection.execute(
                text("SELECT version();")
            )

            version = result.scalar()

            logger.info("Connected successfully.")

            logger.info(version)

            return True

    except Exception as error:

        logger.error(error)

        return False