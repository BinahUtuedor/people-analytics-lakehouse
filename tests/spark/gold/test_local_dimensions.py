"""Local Parquet proof; native Windows physical IO requires the Docker suite."""

from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import os
from unittest import TestCase, skipIf
from unittest.mock import patch

from pyspark.sql import functions as F
from spark.gold.contracts import get_gold_schema
from spark.gold.dimension_validation import DimensionBuild
from spark.gold.manifest import ExistingOutputError
from spark.gold.validate import GoldValidationError
from spark.gold.writer import (
    local_dimension_path,
    write_local_dimension,
    verify_local_dimension,
)
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase, core_dimensions
from tests.spark.gold.test_manifest import spec, STAMP


class LocalPathTests(TestCase):
    def test_native_layout_and_forbidden_storage_schemes(self):
        build = DimensionBuild(spec(), STAMP)
        with TemporaryDirectory() as root:
            self.assertEqual(
                local_dimension_path(root, "dim_date", build),
                Path(root).resolve()
                / "gold"
                / "dim_date"
                / ("build_id=" + build.spec.build_id),
            )
        for root in (
            "s3://bucket/gold",
            "s3a://bucket/gold",
            "https://host/gold",
            "file:///tmp/gold",
            "s3:bucket",
            "//server/share",
            "",
            "*/gold",
        ):
            with self.subTest(root=root), self.assertRaises(ValueError):
                local_dimension_path(root, "dim_date", build)
        with self.assertRaises(ValueError):
            local_dimension_path("local", "fact_attendance", build)

    def test_existing_destination_fails_before_dataframe_use(self):
        build = DimensionBuild(spec(), STAMP)
        with TemporaryDirectory() as root:
            path = local_dimension_path(root, "dim_date", build)
            path.mkdir(parents=True)
            with self.assertRaises(ExistingOutputError):
                write_local_dimension(None, root, "dim_date", build, {})
            self.assertEqual(list(path.iterdir()), [])

    def test_missing_verification_does_not_create_output(self):
        with TemporaryDirectory() as root:
            with self.assertRaises(ExistingOutputError):
                verify_local_dimension(
                    None, root, "dim_date", DimensionBuild(spec(), STAMP), {}
                )
            self.assertEqual(list(Path(root).iterdir()), [])


@skipIf(
    os.name == "nt",
    "Physical Parquet proof requires the repository Linux Docker Spark runtime",
)
class PhysicalDimensionTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.frames = core_dimensions(cls.spark, cls.sources, cls.build)
        for frame in cls.frames.values():
            frame.cache().count()

    @classmethod
    def tearDownClass(cls):
        for frame in cls.frames.values():
            frame.unpersist()
        super().tearDownClass()

    def test_all_five_roundtrip_schema_counts_hashes_metadata_and_unknown(self):
        # Docker mounts the repository at /workspace; a host temp path is not
        # visible inside the Linux container. Keep the disposable root on the
        # mounted filesystem so Spark and pathlib inspect the same directory.
        with TemporaryDirectory(dir="/workspace") as root:
            for model, expected in self.frames.items():
                with self.subTest(model=model):
                    actual, report = write_local_dimension(
                        expected,
                        root,
                        model,
                        self.build,
                        self.sources,
                        dim_date=self.frames["dim_date"],
                    )
                    self.assertTrue(report.passed)
                    self.assertEqual(actual.schema, get_gold_schema(model))
                    self.assertEqual(actual.count(), 95 if model == "dim_date" else 3)
                    self.assertEqual(actual.filter("is_unknown").count(), 1)
                    self.assertEqual(actual.exceptAll(expected).count(), 0)
                    before = {
                        str(p.relative_to(root)): p.read_bytes()
                        for p in Path(root).rglob("*")
                        if p.is_file()
                    }
                    with self.assertRaises(ExistingOutputError):
                        write_local_dimension(
                            expected,
                            root,
                            model,
                            self.build,
                            self.sources,
                            dim_date=self.frames["dim_date"],
                        )
                    verify_local_dimension(
                        expected,
                        root,
                        model,
                        self.build,
                        self.sources,
                        dim_date=self.frames["dim_date"],
                    )
                    after = {
                        str(p.relative_to(root)): p.read_bytes()
                        for p in Path(root).rglob("*")
                        if p.is_file()
                    }
                    self.assertEqual(before, after)

    def test_invalid_output_prevents_write_and_changed_output_fails_verification(self):
        expected = self.frames["dim_job_role"]
        invalid = expected.withColumn("_record_hash", F.lit("invalid"))
        with TemporaryDirectory(dir="/workspace") as root:
            with self.assertRaises(GoldValidationError):
                write_local_dimension(
                    invalid, root, "dim_job_role", self.build, self.sources
                )
            self.assertFalse(
                local_dimension_path(root, "dim_job_role", self.build).exists()
            )
            # Deliberately malformed disposable fixture, never a production writer.
            invalid.write.mode("errorifexists").parquet(
                str(local_dimension_path(root, "dim_job_role", self.build))
            )
            with self.assertRaises(GoldValidationError):
                verify_local_dimension(
                    expected, root, "dim_job_role", self.build, self.sources
                )

    def test_unapproved_physical_column_rejected(self):
        with TemporaryDirectory(dir="/workspace") as root:
            expected = self.frames["dim_job_role"]
            expected.withColumn("unapproved", F.lit("extra")).write.mode(
                "errorifexists"
            ).parquet(str(local_dimension_path(root, "dim_job_role", self.build)))
            with self.assertRaises(ValueError):
                verify_local_dimension(
                    expected, root, "dim_job_role", self.build, self.sources
                )
