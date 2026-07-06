from config.logger import logger
from config.settings import settings
from database.postgres import test_connection


def main() -> None:

    logger.info("=" * 60)
    logger.info(settings.PROJECT_NAME)
    logger.info("=" * 60)

    logger.info(
        f"Environment: {settings.ENVIRONMENT}"
    )

    if test_connection():

        logger.info(
            "Database connection successful."
        )

    else:

        logger.error(
            "Database connection failed."
        )


if __name__ == "__main__":
    main()