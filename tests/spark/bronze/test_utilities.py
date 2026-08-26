"""Tests for runtime-controlled Spark master selection."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import MagicMock, patch

from spark.utilities import build_spark_session, validate_s3a_available


class SparkUtilityTests(TestCase):
    def _build_with_master(self, master: str | None) -> MagicMock:
        builder = MagicMock()
        builder.appName.return_value = builder
        builder.master.return_value = builder
        builder.config.return_value = builder
        spark = MagicMock()
        builder.getOrCreate.return_value = spark

        with (
            patch("spark.utilities.SparkSession") as spark_session,
            patch("spark.utilities.settings.SPARK_MASTER", master),
            patch("spark.utilities.settings.SPARK_JARS_PACKAGES", None),
            patch("spark.utilities.configure_s3a"),
        ):
            spark_session.builder = builder
            build_spark_session("unit-test")
        return builder

    def test_runtime_master_is_inherited_when_unset(self) -> None:
        builder = self._build_with_master(None)
        builder.master.assert_not_called()

    def test_explicit_local_master_is_applied(self) -> None:
        builder = self._build_with_master("local[1]")
        builder.master.assert_called_once_with("local[1]")

    def test_s3a_validation_uses_hadoop_filesystem_resolution(self) -> None:
        spark = MagicMock()

        validate_s3a_available(spark)

        filesystem = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
        filesystem.getFileSystemClass.assert_called_once_with(
            "s3a",
            spark.sparkContext._jsc.hadoopConfiguration.return_value,
        )
