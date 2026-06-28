"""
Application configuration.

Loads all environment variables
from the .env file.

Every module imports the singleton
'settings' instead of reading
environment variables directly.
"""

from dotenv import load_dotenv
import os

load_dotenv()


class Settings:
    """
    Central application settings.
    """

    # ======================================================
    # Project
    # ======================================================

    PROJECT_NAME = os.getenv(
        "PROJECT_NAME"
    )

    ENVIRONMENT = os.getenv(
        "ENVIRONMENT"
    )

    # ======================================================
    # PostgreSQL
    # ======================================================

    POSTGRES_HOST = os.getenv(
        "POSTGRES_HOST"
    )

    POSTGRES_PORT = int(
        os.getenv("POSTGRES_PORT")
    )

    POSTGRES_DATABASE = os.getenv(
        "POSTGRES_DATABASE"
    )

    POSTGRES_USER = os.getenv(
        "POSTGRES_USER"
    )

    POSTGRES_PASSWORD = os.getenv(
        "POSTGRES_PASSWORD"
    )

    # ======================================================
    # AWS
    # ======================================================

    AWS_REGION = os.getenv(
        "AWS_REGION"
    )

    S3_BUCKET = os.getenv(
        "S3_BUCKET"
    )

    # ======================================================
    # Databricks
    # ======================================================

    DATABRICKS_HOST = os.getenv(
        "DATABRICKS_HOST"
    )

    DATABRICKS_HTTP_PATH = os.getenv(
        "DATABRICKS_HTTP_PATH"
    )

    DATABRICKS_TOKEN = os.getenv(
        "DATABRICKS_TOKEN"
    )

    CATALOG = os.getenv(
        "CATALOG"
    )

    SCHEMA = os.getenv(
        "SCHEMA"
    )


settings = Settings()