from __future__ import annotations
from unittest import TestCase
from unittest.mock import MagicMock
from pyspark.sql.types import DecimalType, LongType
from spark.silver.reader import (
    BronzeBatch,
    SilverReadError,
    build_bronze_table_prefix,
    discover_bronze_batch,
)
from spark.silver.transform import transform_to_silver
from spark.silver.validate import (
    SilverValidationError,
    validate_employee_references,
    validate_silver,
)
from spark.silver.writer import build_silver_output_path
from tests.spark.bronze.spark_test_case import SparkTestCase


class SilverReaderTests(TestCase):
    def test_prefix_uses_bronze_convention(self):
        self.assertEqual(
            build_bronze_table_prefix("employees", "bronze"),
            "bronze/postgresql/employees/",
        )

    def test_missing_batch_fails(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        with self.assertRaises(SilverReadError):
            discover_bronze_batch("employees", "batch", bucket="lake", s3_client=client)


class SilverTransformTests(SparkTestCase):
    def setUp(self):
        super().setUp()
        self.batch = BronzeBatch(
            "employees",
            "lake",
            "bronze",
            "bronze/postgresql/employees/extraction_date=2026-01-01/batch_id=b/",
            "2026-01-01",
            "b",
        )

    def test_types_cleanup_metadata_and_deduplication(self):
        bronze = self.spark.createDataFrame(
            [
                (
                    1,
                    " Alice ",
                    " ",
                    "10.5",
                    "hash",
                    "b",
                    "2026-01-01",
                    "file",
                    "2026-01-01 00:00:00",
                ),
                (
                    1,
                    " Alice ",
                    " ",
                    "10.5",
                    "hash",
                    "b",
                    "2026-01-01",
                    "file2",
                    "2026-01-02 00:00:00",
                ),
            ],
            [
                "employee_id",
                "name",
                "optional_text",
                "salary",
                "_record_hash",
                "_batch_id",
                "_extraction_date",
                "_source_file",
                "_bronze_ingested_at",
            ],
        )
        silver = transform_to_silver(bronze, self.batch)
        self.assertEqual(silver.count(), 1)
        row = silver.first()
        self.assertEqual(row.name, "Alice")
        self.assertIsNone(row.optional_text)
        self.assertIsInstance(silver.schema["employee_id"].dataType, LongType)
        self.assertIsInstance(silver.schema["salary"].dataType, DecimalType)
        self.assertIn("_silver_transformed_at", silver.columns)

    def test_orphans_fail(self):
        child = self.spark.createDataFrame([(2,)], ["employee_id"])
        parents = self.spark.createDataFrame([(1,)], ["employee_id"])
        with self.assertRaises(SilverValidationError):
            validate_employee_references(child, parents, "attendance")

    def test_validation_rejects_duplicate_hash(self):
        frame = self.spark.createDataFrame(
            [
                ("h", "b", "x", "2026-01-01", "2026-01-01"),
                ("h", "b", "x", "2026-01-01", "2026-01-01"),
            ],
            [
                "_record_hash",
                "_batch_id",
                "_source_file",
                "_extraction_date",
                "_silver_transformed_at",
            ],
        )
        with self.assertRaises(SilverValidationError):
            validate_silver(frame, frame, "employees", "b")

    def test_output_path(self):
        self.assertEqual(
            build_silver_output_path(self.batch, bucket="lake", silver_prefix="silver"),
            "s3a://lake/silver/postgresql/employees/extraction_date=2026-01-01/batch_id=b/",
        )
