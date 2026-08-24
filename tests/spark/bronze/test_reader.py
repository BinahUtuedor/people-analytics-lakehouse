"""Tests for Raw S3 batch discovery and physical file lineage."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock

from spark.bronze.reader import (
    BronzeReadError,
    build_table_prefix,
    discover_raw_batch,
    list_raw_parquet_objects,
    parse_partition_metadata,
    read_parquet_with_lineage,
)
from tests.spark.bronze.spark_test_case import SparkTestCase


def raw_object(
    batch_id: str,
    extraction_id: str,
    part: str = "part-00000.parquet",
) -> dict:
    return {
        "Key": (
            "raw/postgresql/business_units/"
            "extraction_date=2026-08-23/"
            f"batch_id={batch_id}/"
            f"extraction_id={extraction_id}/{part}"
        ),
        "LastModified": datetime(2026, 8, 23, tzinfo=timezone.utc),
    }


class ReaderDiscoveryTests(TestCase):
    def test_partition_identities_are_parsed_independently(self) -> None:
        key = raw_object("shared-batch", "table-extraction")["Key"]
        self.assertEqual(
            parse_partition_metadata(key),
            ("2026-08-23", "shared-batch", "table-extraction"),
        )

    def test_explicit_batch_groups_all_parquet_parts(self) -> None:
        objects = [
            raw_object("wanted", "extract-1", "part-00001.parquet"),
            raw_object("other", "extract-2"),
            raw_object("wanted", "extract-1", "part-00000.parquet"),
        ]
        batch = discover_raw_batch(
            "business_units",
            "wanted",
            bucket="unit-test-bucket",
            raw_prefix="/raw/postgresql/",
            objects=objects,
        )
        self.assertEqual(batch.batch_id, "wanted")
        self.assertEqual(batch.extraction_id, "extract-1")
        self.assertEqual(len(batch.parquet_keys), 2)
        self.assertEqual(batch.raw_prefix, "raw/postgresql")

    def test_ambiguous_shared_batch_is_rejected(self) -> None:
        objects = [
            raw_object("batch-1", "extract-1"),
            raw_object("batch-1", "extract-2"),
        ]
        with self.assertRaisesRegex(BronzeReadError, "ambiguous"):
            discover_raw_batch(
                "business_units",
                "batch-1",
                bucket="unit-test-bucket",
                objects=objects,
            )

    def test_missing_partition_metadata_is_rejected(self) -> None:
        malformed = {
            "Key": "raw/postgresql/business_units/part-00000.parquet",
            "LastModified": datetime.now(timezone.utc),
        }
        with self.assertRaisesRegex(BronzeReadError, "incomplete"):
            discover_raw_batch(
                "business_units",
                "batch-1",
                bucket="unit-test-bucket",
                objects=[malformed],
            )

    def test_object_outside_requested_table_is_rejected(self) -> None:
        item = raw_object("batch-1", "extract-1")
        item["Key"] = item["Key"].replace(
            "/business_units/",
            "/departments/",
        )
        with self.assertRaisesRegex(BronzeReadError, "outside"):
            discover_raw_batch(
                "business_units",
                "batch-1",
                bucket="unit-test-bucket",
                objects=[item],
            )

    def test_s3_listing_is_paginated_and_mocked(self) -> None:
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [raw_object("batch-1", "extract-1")]},
            {"Contents": [{"Key": "raw/postgresql/ignored.txt"}]},
        ]
        client = MagicMock()
        client.get_paginator.return_value = paginator
        objects = list_raw_parquet_objects(
            "business_units",
            bucket="unit-test-bucket",
            s3_client=client,
        )
        self.assertEqual(len(objects), 1)
        paginator.paginate.assert_called_once_with(
            Bucket="unit-test-bucket",
            Prefix="raw/postgresql/business_units/",
        )

    def test_table_prefix_preserves_raw_layout(self) -> None:
        self.assertEqual(
            build_table_prefix("business_units", "/raw/postgresql/"),
            "raw/postgresql/business_units/",
        )


class ReaderLineageTests(SparkTestCase):
    def test_file_lineage_is_captured_during_read(self) -> None:
        with TemporaryDirectory() as directory:
            raw_path = Path(directory) / "raw"
            self.spark.createDataFrame([(1, "Operations")], ["id", "name"]).write.parquet(
                str(raw_path)
            )
            dataframe = read_parquet_with_lineage(self.spark, str(raw_path))
            source_file = dataframe.select("_source_file").first()[0]
            self.assertIn("part-", source_file)
            self.assertTrue(source_file.endswith(".parquet"))
