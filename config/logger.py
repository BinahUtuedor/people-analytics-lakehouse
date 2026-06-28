"""
Central logging configuration.

All project modules should import
this logger.

Example:

from config.logger import logger
"""

import sys

from loguru import logger

logger.remove()

logger.add(
    sys.stdout,
    level="INFO",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss}"
        " | "
        "{level}"
        " | "
        "{message}"
    ),
)

logger.add(
    "logs/application.log",
    rotation="10 MB",
    retention="30 days",
    level="INFO",
)