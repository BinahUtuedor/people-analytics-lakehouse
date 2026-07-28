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
        os.getenv(
            "POSTGRES_PORT",
            "5432",
        )
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
        "AWS_REGION",
        "eu-west-2",
    )

    AWS_S3_BUCKET = os.getenv(
        "AWS_S3_BUCKET"
    )

    AWS_S3_RAW_PREFIX = os.getenv(
        "AWS_S3_RAW_PREFIX",
        "raw/postgresql",
    )

    AWS_S3_BRONZE_PREFIX = os.getenv(
        "AWS_S3_BRONZE_PREFIX",
        "bronze",
    )

    AWS_S3_SILVER_PREFIX = os.getenv(
        "AWS_S3_SILVER_PREFIX",
        "silver",
    )

    AWS_S3_GOLD_PREFIX = os.getenv(
        "AWS_S3_GOLD_PREFIX",
        "gold",
    )

    # AWS credentials.
    #
    # Boto3 and Hadoop S3A can consume these standard
    # environment variables.
    AWS_ACCESS_KEY_ID = os.getenv(
        "AWS_ACCESS_KEY_ID"
    )

    AWS_SECRET_ACCESS_KEY = os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )

    AWS_SESSION_TOKEN = os.getenv(
        "AWS_SESSION_TOKEN"
    )

    # ======================================================
    # Spark
    # ======================================================

    SPARK_APP_NAME = os.getenv(
        "SPARK_APP_NAME",
        "people-analytics-lakehouse",
    )

    SPARK_MASTER = os.getenv(
        "SPARK_MASTER",
        "local[*]",
    )

    SPARK_LOG_LEVEL = os.getenv(
        "SPARK_LOG_LEVEL",
        "WARN",
    )

    # Optional Maven packages supplied when the Spark installation
    # does not already contain the Hadoop S3A connector.
    #
    # Example:
    #
    # org.apache.hadoop:hadoop-aws:<matching-hadoop-version>
    #
    # Do not hard-code the version here. It must be compatible with
    # the Hadoop version bundled with the Spark installation.
    SPARK_JARS_PACKAGES = os.getenv(
        "SPARK_JARS_PACKAGES"
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