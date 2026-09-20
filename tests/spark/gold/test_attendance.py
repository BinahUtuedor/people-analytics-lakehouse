"""Attendance Gold contract, reconciliation and immutable physical proof."""

from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from spark.gold.attendance import (
    ATTENDANCE_MODEL,
    attendance_key_reconciliation,
    build_fact_attendance,
    normalize_attendance_source,
    validate_attendance,
)
from spark.gold.contracts import get_gold_schema
from spark.gold.dimension_validation import enforce_schema
from spark.gold.hashing import record_hash, spark_record_hash
from spark.gold.manifest import ExistingOutputError
from spark.gold.validate import GoldValidationError
from spark.gold.writer import (
    local_attendance_partition_path,
    local_dimension_path,
    verify_local_dimension,
    write_local_dimension,
)
from tests.spark.gold.attendance_fixtures import attendance_fixture
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase


class AttendanceTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.build, cls.sources = attendance_fixture(cls.spark, cls.sources, cls.build)
        cls.output = build_fact_attendance(cls.sources, cls.build).cache()
        cls.rows = {row.attendance_id: row for row in cls.output.collect()}

    @classmethod
    def tearDownClass(cls):
        cls.spark.catalog.clearCache()
        super().tearDownClass()

    def source_build(self, source):
        return build_fact_attendance(dict(self.sources, attendance=source), self.build)

    def changed(self, name, value, frame=None):
        frame = (self.output if frame is None else frame).withColumn(name, value)
        if not name.startswith("_"):
            frame = frame.withColumn(
                "_record_hash", spark_record_hash(frame, ATTENDANCE_MODEL)
            )
        return enforce_schema(frame, ATTENDANCE_MODEL)

    def test_contract_status_absence_governance_and_hash(self):
        self.assertEqual(self.output.schema, get_gold_schema(ATTENDANCE_MODEL))
        self.assertTrue(
            all(
                not field.nullable
                for field in self.output.schema
                if field.name != "absence_reason"
            )
        )
        self.assertEqual(
            {row.attendance_status for row in self.rows.values()},
            {"Present", "Remote", "Hybrid", "Training", "Business Travel", "Absent"},
        )
        for row in self.rows.values():
            self.assertEqual(
                row._record_hash, record_hash(ATTENDANCE_MODEL, row.asDict())
            )
            self.assertEqual(row.recorded_day_count, 1)
            self.assertEqual(
                row.absent_day_count, int(row.attendance_status == "Absent")
            )
        for name in (
            "name",
            "email",
            "date_of_birth",
            "gender",
            "bank_account",
            "tax_identifier",
            "national_identifier",
            "home_address",
            "notes",
        ):
            self.assertNotIn(name, self.output.columns)

    def test_grain_and_historical_assignment(self):
        self.assertEqual(self.output.count(), 7)
        self.assertEqual(len(self.rows), 7)
        self.assertEqual(self.rows[1003].work_date_key, 20240229)
        self.assertEqual(self.rows[1005].work_date_key, 20240415)
        self.assertNotEqual(
            self.rows[1002].assignment_key, self.rows[1004].assignment_key
        )
        current = self.sources["employees"].filter("employee_id = 1").first()
        self.assertEqual(current.department_id, 2)
        self.assertEqual(self.rows[1002].department_key, 1)
        self.assertEqual(self.rows[1004].department_key, 2)

    def test_cutoff_employment_and_assignment_boundaries(self):
        source = self.sources["attendance"].filter("attendance_id = 1005")
        for value in ("2024-04-16", "2024-04-30"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(GoldValidationError, "source_cutoff"),
            ):
                self.source_build(
                    source.withColumn("work_date", F.lit(value).cast("date"))
                )
        before_hire = (
            self.sources["attendance"]
            .filter("attendance_id = 3001")
            .withColumn("work_date", F.lit("2024-01-01").cast("date"))
        )
        with self.assertRaisesRegex(GoldValidationError, "employment_window"):
            self.source_build(before_hire)
        after_termination = (
            self.sources["attendance"]
            .filter("attendance_id = 3001")
            .withColumn("work_date", F.lit("2024-04-16").cast("date"))
        )
        with self.assertRaisesRegex(GoldValidationError, "source_cutoff"):
            self.source_build(after_termination)

    def test_missing_employee_assignment_and_duplicate_keys_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "employment_window"):
            self.source_build(
                self.sources["attendance"]
                .filter("attendance_id = 1001")
                .withColumn("employee_id", F.lit(999).cast("long"))
            )
        with self.assertRaisesRegex(GoldValidationError, "required_assignment"):
            build_fact_attendance(
                dict(
                    self.sources,
                    dim_employee_assignment=self.sources[
                        "dim_employee_assignment"
                    ].filter("employee_key != 1"),
                ),
                self.build,
            )
        duplicate_id = (
            self.sources["attendance"]
            .filter("attendance_id = 1001")
            .withColumn("attendance_id", F.lit(9999).cast("long"))
        )
        with self.assertRaisesRegex(GoldValidationError, "daily_unique"):
            self.source_build(self.sources["attendance"].unionByName(duplicate_id))
        duplicate = self.sources["attendance"].filter("attendance_id = 1001")
        with self.assertRaisesRegex(GoldValidationError, "id_unique"):
            self.source_build(self.sources["attendance"].unionByName(duplicate))

    def test_numeric_precision_and_status_reason_rules(self):
        source = self.sources["attendance"].limit(1)
        for value in ("1.23456", "-0.0001", "100000000000000000.0000"):
            with self.subTest(value=value), self.assertRaises(GoldValidationError):
                self.source_build(
                    source.withColumn(
                        "hours_worked", F.lit(value).cast("decimal(38,18)")
                    )
                )
        for status, reason in (
            ("Absent", None),
            ("Present", "Unplanned Absence"),
            ("Unknown", None),
        ):
            with self.subTest(status=status), self.assertRaises(GoldValidationError):
                self.source_build(
                    source.withColumn("status", F.lit(status)).withColumn(
                        "absence_reason", F.lit(reason)
                    )
                )
        zeros = source.withColumn("hours_worked", F.lit(Decimal("0.0000"))).withColumn(
            "overtime_hours", F.lit(Decimal("0.0000"))
        )
        self.assertEqual(
            normalize_attendance_source(zeros, self.build).first().hours_worked,
            Decimal("0.0000"),
        )

    def test_date_and_timestamp_inputs_reorder_and_reconciliation(self):
        dated = self.sources["attendance"].withColumn(
            "work_date", F.col("work_date").cast("date")
        )
        self.assertEqual(normalize_attendance_source(dated, self.build).count(), 7)
        reordered = (
            self.sources["attendance"].orderBy(F.desc("attendance_id")).repartition(2)
        )
        rebuilt = self.source_build(reordered)
        self.assertEqual(self.output.exceptAll(rebuilt).count(), 0)
        metrics = attendance_key_reconciliation(
            self.output,
            normalize_attendance_source(self.sources["attendance"], self.build),
        )
        self.assertEqual(metrics["source_rows"], 7)
        self.assertEqual(metrics["gold_rows"], 7)
        self.assertEqual(metrics["missing_keys"], 0)
        self.assertEqual(metrics["unexpected_keys"], 0)

    @unittest.skipIf(
        __import__("os").name == "nt", "Physical Parquet requires Linux Docker Spark"
    )
    def test_physical_roundtrip_immutable_and_corruption(self):
        with TemporaryDirectory(dir="/workspace") as root:
            actual, report = write_local_dimension(
                self.output, root, ATTENDANCE_MODEL, self.build, self.sources
            )
            self.assertTrue(report.passed)
            self.assertEqual(actual.schema, get_gold_schema(ATTENDANCE_MODEL))
            self.assertEqual(actual.count(), 7)
            for year in (2023, 2024):
                self.assertTrue(
                    local_attendance_partition_path(root, self.build, year).is_dir()
                )
            before = {
                str(path): path.read_bytes()
                for path in Path(root).rglob("*")
                if path.is_file()
            }
            with self.assertRaises(ExistingOutputError):
                write_local_dimension(
                    self.output, root, ATTENDANCE_MODEL, self.build, self.sources
                )
            verify_local_dimension(
                self.output, root, ATTENDANCE_MODEL, self.build, self.sources
            )
            self.assertEqual(
                before,
                {
                    str(path): path.read_bytes()
                    for path in Path(root).rglob("*")
                    if path.is_file()
                },
            )

    @unittest.skipIf(
        __import__("os").name == "nt", "Physical Parquet requires Linux Docker Spark"
    )
    def test_physical_wrong_partition_and_invalid_prewrite(self):
        with TemporaryDirectory(dir="/workspace") as root:
            with self.assertRaises(GoldValidationError):
                write_local_dimension(
                    self.changed("_record_hash", F.lit("bad")),
                    root,
                    ATTENDANCE_MODEL,
                    self.build,
                    self.sources,
                )
            claim = local_dimension_path(root, ATTENDANCE_MODEL, self.build)
            claim.mkdir(parents=True)
            self.output.filter("reporting_year = 2023").write.mode(
                "errorifexists"
            ).parquet(str(local_attendance_partition_path(root, self.build, 2023)))
            with self.assertRaises(ExistingOutputError):
                verify_local_dimension(
                    self.output, root, ATTENDANCE_MODEL, self.build, self.sources
                )
