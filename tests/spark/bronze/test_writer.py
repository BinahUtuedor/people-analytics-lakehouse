"""Tests for duplicate-safe Bronze batch publication."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock

from spark.bronze.writer import (
    BronzeWriteError,
    bronze_output_exists,
    build_bronze_output_path,
    write_bronze,
)
from tests.spark.bronze.spark_test_case import SparkTestCase
from tests.spark.bronze.test_transform import make_batch


class BronzeWriterContractTests(TestCase):
    def test_checks_output_existence_with_its_hadoop_filesystem(self) -> None:
        spark = MagicMock()
        hadoop_path = (
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path.return_value
        )
        hadoop_path.getFileSystem.return_value.exists.return_value = True

        self.assertTrue(bronze_output_exists(spark, "s3a://bucket/bronze/"))

        hadoop_path.getFileSystem.return_value.exists.assert_called_once_with(
            hadoop_path
        )

    def test_builds_batch_specific_s3a_path(self) -> None:
        path = build_bronze_output_path(
            make_batch(),
            bucket="unit-test-bucket",
            bronze_prefix="/bronze/",
        )
        self.assertEqual(
            path,
            "s3a://unit-test-bucket/bronze/postgresql/business_units/"
            "extraction_date=2026-08-23/batch_id=batch-1/",
        )

    def test_writer_configures_duplicate_safe_parquet_publication(self) -> None:
        dataframe = MagicMock()
        dataframe.write.mode.return_value = dataframe.write
        dataframe.write.option.return_value = dataframe.write

        write_bronze(dataframe, "s3a://unit-test-bucket/bronze/batch/")

        dataframe.write.mode.assert_called_once_with("errorifexists")
        dataframe.write.option.assert_called_once_with("compression", "snappy")
        dataframe.write.parquet.assert_called_once_with(
            "s3a://unit-test-bucket/bronze/batch/"
        )


class BronzeWriterTests(SparkTestCase):

    def test_repeated_publication_fails_without_appending(self) -> None:
        dataframe = self.spark.createDataFrame([(1,)], ["id"])
        with TemporaryDirectory() as directory:
            output_path = str(Path(directory) / "bronze-batch")
            write_bronze(dataframe, output_path)
            with self.assertRaises(BronzeWriteError):
                write_bronze(dataframe, output_path)
            self.assertEqual(self.spark.read.parquet(output_path).count(), 1)
