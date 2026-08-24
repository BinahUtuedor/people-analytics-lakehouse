"""Tests for deterministic Bronze transformation and hashing."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.bronze.reader import RawBatch
from spark.bronze.transform import BronzeTransformError, transform_to_bronze
from tests.spark.bronze.spark_test_case import SparkTestCase


def make_batch(batch_id: str = "batch-1") -> RawBatch:
    return RawBatch(
        table_name="business_units",
        bucket="unit-test-bucket",
        raw_prefix="raw/postgresql",
        batch_prefix=(
            "raw/postgresql/business_units/extraction_date=2026-08-23/"
            f"batch_id={batch_id}/extraction_id=extract-1/"
        ),
        parquet_keys=("part-00000.parquet",),
        extraction_date="2026-08-23",
        batch_id=batch_id,
        extraction_id="extract-1",
        last_modified=datetime(2026, 8, 23, tzinfo=timezone.utc),
    )


RAW_SCHEMA = StructType(
    [
        StructField("business_unit_id", IntegerType(), False),
        StructField("amount", DecimalType(10, 2), True),
        StructField("effective_date", DateType(), True),
        StructField("name", StringType(), False),
        StructField("description", StringType(), True),
        StructField("updated_at", TimestampType(), False),
        StructField("_source_system", StringType(), False),
        StructField("_source_schema", StringType(), False),
        StructField("_source_table", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("_extraction_id", StringType(), False),
        StructField("_extracted_at_utc", TimestampType(), False),
        StructField("_source_file", StringType(), False),
    ]
)


def raw_row(batch_id: str = "batch-1") -> tuple:
    return (
        1,
        Decimal("10.50"),
        date(2026, 8, 23),
        "München 人事",
        None,
        datetime(2026, 8, 23, 9, 30),
        "postgresql",
        "public",
        "business_units",
        batch_id,
        "extract-1",
        datetime(2026, 8, 23, 10, 0),
        "s3a://unit-test-bucket/raw/part-00000.parquet",
    )


class BronzeTransformTests(SparkTestCase):
    def test_transform_preserves_raw_columns_and_adds_metadata(self) -> None:
        raw = self.spark.createDataFrame([raw_row()], RAW_SCHEMA)
        bronze = transform_to_bronze(
            raw,
            make_batch(),
            ingested_at=datetime(2026, 8, 23, 12, 0),
        )
        self.assertEqual(bronze.columns[: len(raw.columns)], raw.columns)
        result = bronze.first().asDict()
        self.assertEqual(
            result["_source_file"],
            "s3a://unit-test-bucket/raw/part-00000.parquet",
        )
        self.assertEqual(result["_extraction_date"], date(2026, 8, 23))
        self.assertRegex(result["_record_hash"], "^[0-9a-f]{64}$")

    def test_hash_is_stable_when_column_order_or_batch_metadata_changes(self) -> None:
        first = self.spark.createDataFrame([raw_row("batch-1")], RAW_SCHEMA)
        second = self.spark.createDataFrame([raw_row("batch-2")], RAW_SCHEMA)
        reordered = second.select(*reversed(second.columns))
        first_hash = transform_to_bronze(first, make_batch("batch-1")).first()[
            "_record_hash"
        ]
        second_hash = transform_to_bronze(
            reordered,
            make_batch("batch-2"),
        ).first()["_record_hash"]
        self.assertEqual(first_hash, second_hash)

    def test_hash_changes_when_business_value_changes(self) -> None:
        raw = self.spark.createDataFrame([raw_row()], RAW_SCHEMA)
        changed = raw.withColumn("name", F.lit("Operations"))
        original_hash = transform_to_bronze(raw, make_batch()).first()[
            "_record_hash"
        ]
        changed_hash = transform_to_bronze(changed, make_batch()).first()[
            "_record_hash"
        ]
        self.assertNotEqual(original_hash, changed_hash)

    def test_null_is_retained_in_canonical_hash_payload(self) -> None:
        raw = self.spark.createDataFrame([raw_row()], RAW_SCHEMA)
        null_hash = transform_to_bronze(raw, make_batch()).first()["_record_hash"]
        non_null_hash = transform_to_bronze(
            raw.fillna({"description": ""}),
            make_batch(),
        ).first()["_record_hash"]
        self.assertNotEqual(null_hash, non_null_hash)

    def test_source_file_is_required_from_reader(self) -> None:
        raw = self.spark.createDataFrame([(1,)], ["business_unit_id"])
        with self.assertRaises(BronzeTransformError):
            transform_to_bronze(raw, make_batch())

    def test_at_least_one_business_column_is_required(self) -> None:
        raw = self.spark.createDataFrame(
            [("file.parquet",)],
            ["_source_file"],
        )
        with self.assertRaises(BronzeTransformError):
            transform_to_bronze(raw, make_batch())
