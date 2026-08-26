"""Tests for Bronze validation and Raw-to-Bronze reconciliation."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from spark.bronze.transform import transform_to_bronze
from spark.bronze.validate import BronzeValidationError, validate_bronze
from tests.spark.bronze.spark_test_case import SparkTestCase
from tests.spark.bronze.test_transform import RAW_SCHEMA, make_batch, raw_row


class BronzeValidationTests(SparkTestCase):
    def setUp(self) -> None:
        self.batch = make_batch()
        self.raw = self.spark.createDataFrame(
            [raw_row(), raw_row()],
            RAW_SCHEMA,
        )
        self.bronze = transform_to_bronze(
            self.raw,
            self.batch,
            ingested_at=datetime(2026, 8, 23, 12, 0),
        )

    def test_valid_bronze_reconciles_counts(self) -> None:
        result = validate_bronze(self.raw, self.bronze, self.batch)
        self.assertTrue(result.passed)
        self.assertEqual(result.raw_count, 2)
        self.assertEqual(result.bronze_count, 2)

    def test_zero_row_dataframe_retains_schema_and_passes(self) -> None:
        empty_raw = self.raw.limit(0)
        empty_bronze = transform_to_bronze(empty_raw, self.batch)
        result = validate_bronze(empty_raw, empty_bronze, self.batch)
        self.assertEqual((result.raw_count, result.bronze_count), (0, 0))

    def test_missing_metadata_fails(self) -> None:
        with self.assertRaisesRegex(BronzeValidationError, "missing"):
            validate_bronze(
                self.raw,
                self.bronze.drop("_record_hash"),
                self.batch,
            )

    def test_null_metadata_fails(self) -> None:
        invalid = self.bronze.withColumn(
            "_source_file",
            F.lit(None).cast("string"),
        )
        with self.assertRaisesRegex(BronzeValidationError, "null"):
            validate_bronze(self.raw, invalid, self.batch)

    def test_invalid_hash_fails(self) -> None:
        invalid = self.bronze.withColumn("_record_hash", F.lit("invalid"))
        with self.assertRaisesRegex(BronzeValidationError, "SHA-256"):
            validate_bronze(self.raw, invalid, self.batch)

    def test_well_formed_but_incorrect_hash_fails(self) -> None:
        invalid = self.bronze.withColumn("_record_hash", F.lit("0" * 64))
        with self.assertRaisesRegex(BronzeValidationError, "inconsistent"):
            validate_bronze(self.raw, invalid, self.batch)

    def test_lineage_mismatch_fails(self) -> None:
        invalid = self.bronze.withColumn("_batch_id", F.lit("wrong-batch"))
        with self.assertRaisesRegex(BronzeValidationError, "_batch_id"):
            validate_bronze(self.raw, invalid, self.batch)

    def test_source_column_loss_fails(self) -> None:
        invalid = self.bronze.drop("name")
        with self.assertRaisesRegex(BronzeValidationError, "Raw columns"):
            validate_bronze(self.raw, invalid, self.batch)

    def test_count_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(BronzeValidationError, "row-count"):
            validate_bronze(self.raw, self.bronze.limit(1), self.batch)
