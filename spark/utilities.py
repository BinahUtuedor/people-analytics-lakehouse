"""
Shared Apache Spark utilities.

Responsible for:

- creating the SparkSession;
- applying common Spark configuration;
- configuring Amazon S3A access;
- validating that the S3A filesystem implementation is available;
- stopping Spark cleanly.

Spark jobs should use this module rather than constructing independent
SparkSession objects.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.logger import logger
from config.settings import settings


class SparkConfigurationError(RuntimeError):
    """
    Raised when Spark cannot be configured correctly.
    """


def build_spark_session(
    app_name: str | None = None,
) -> SparkSession:
    """
    Create and configure the platform SparkSession.

    Args:
        app_name:
            Optional Spark application name.

    Returns:
        Configured SparkSession.
    """

    application_name = (
        app_name
        or settings.SPARK_APP_NAME
    )

    logger.info(
        "Creating Spark session | "
        f"app={application_name} | "
        f"master={settings.SPARK_MASTER or 'runtime-provided'}"
    )

    builder = (
        SparkSession.builder
        .appName(
            application_name
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.parquet.mergeSchema",
            "false",
        )
        .config(
            "spark.sql.parquet.filterPushdown",
            "true",
        )
    )

    if settings.SPARK_MASTER:
        builder = builder.master(
            settings.SPARK_MASTER
        )

    # ---------------------------------------------------------
    # Optional external Spark packages
    # ---------------------------------------------------------

    if settings.SPARK_JARS_PACKAGES:
        builder = builder.config(
            "spark.jars.packages",
            settings.SPARK_JARS_PACKAGES,
        )

        logger.info(
            "Spark Maven packages configured."
        )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(
        settings.SPARK_LOG_LEVEL
    )

    configure_s3a(
        spark
    )

    logger.info(
        "Spark session created successfully | "
        f"version={spark.version}"
    )

    return spark


def configure_s3a(
    spark: SparkSession,
) -> None:
    """
    Apply S3A configuration to the Spark Hadoop configuration.

    AWS credentials are intentionally not copied into Spark config.

    They remain supplied through the standard AWS environment-variable
    credential chain:

        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
        AWS_SESSION_TOKEN

    This avoids embedding secrets in Spark configuration or logs.
    """

    if not settings.AWS_REGION:
        raise SparkConfigurationError(
            "AWS_REGION is not configured."
        )

    hadoop_configuration = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    hadoop_configuration.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    hadoop_configuration.set(
        "fs.s3a.endpoint",
        (
            f"s3."
            f"{settings.AWS_REGION}"
            ".amazonaws.com"
        ),
    )

    hadoop_configuration.set(
        "fs.s3a.endpoint.region",
        settings.AWS_REGION,
    )

    hadoop_configuration.set(
        "fs.s3a.connection.ssl.enabled",
        "true",
    )

    logger.info(
        "Spark S3A configuration applied | "
        f"region={settings.AWS_REGION}"
    )


def validate_s3a_available(
    spark: SparkSession,
) -> None:
    """
    Confirm that the Hadoop S3A filesystem implementation is available.

    Raises:
        SparkConfigurationError:
            If hadoop-aws / S3A classes are unavailable.
    """

    try:
        java_class = (
            spark.sparkContext
            ._jvm
            .java.lang.Class
        )

        java_class.forName(
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

    except Exception as error:
        raise SparkConfigurationError(
            "Spark cannot load the Hadoop S3A filesystem. "
            "The hadoop-aws connector may be missing or incompatible "
            "with the Hadoop version bundled with Spark. "
            "Configure SPARK_JARS_PACKAGES with a compatible "
            "org.apache.hadoop:hadoop-aws package."
        ) from error

    logger.info(
        "Hadoop S3A filesystem is available."
    )


def stop_spark(
    spark: SparkSession | None,
) -> None:
    """
    Stop a SparkSession safely.
    """

    if spark is None:
        return

    logger.info(
        "Stopping Spark session..."
    )

    spark.stop()

    logger.info(
        "Spark session stopped."
    )
