"""
Project entry point.

Currently verifies that:

1. Configuration loads.
2. Logging works.
3. The application starts correctly.
"""

from config.logger import logger
from config.settings import settings


def main() -> None:
    """
    Application entry point.
    """

    logger.info("=" * 60)

    logger.info(settings.PROJECT_NAME)

    logger.info("=" * 60)

    logger.info(
        f"Environment: {settings.ENVIRONMENT}"
    )

    logger.info(
        "Phase 1 configuration loaded successfully."
    )


if __name__ == "__main__":
    main()