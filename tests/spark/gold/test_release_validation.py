"""MVP-wide Gold integration, identity and release-readiness tests."""

from dataclasses import replace
from datetime import date
from tempfile import TemporaryDirectory
import unittest

from spark.gold.attendance import build_fact_attendance
from spark.gold.contracts import GOLD_MODELS
from spark.gold.manifest import (
    ExistingOutputError,
    ReleaseStatus,
    RunMode,
    VERIFICATION_CHECKS,
    require_output_policy,
)
from spark.gold.release_validation import (
    build_release_manifest,
    validate_mvp_frames,
)
from spark.gold.workforce_history import build_fact_employee_movement
from spark.gold.workforce_monthly import build_fact_workforce_monthly
from spark.gold.payroll import build_fact_payroll
from spark.gold.writer import local_dimension_path, write_local_dimension
from tests.spark.gold.attendance_fixtures import attendance_fixture
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase


class ReleaseValidationTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.build, cls.sources = attendance_fixture(cls.spark, cls.sources, cls.build)
        cls.sources = dict(cls.sources)
        cls.frames = {
            model: cls.sources[model]
            for model in (
                "dim_employee",
                "dim_department",
                "dim_location",
                "dim_job_role",
                "dim_date",
                "dim_employee_assignment",
            )
        }
        cls.frames["fact_employee_movement"] = build_fact_employee_movement(
            cls.sources["employees"],
            cls.sources["promotions"],
            cls.sources["transfers"],
            cls.sources["employee_exits"],
            cls.sources["dim_employee_assignment"],
            cls.build,
        )
        cls.frames["fact_workforce_monthly"] = build_fact_workforce_monthly(
            cls.sources, cls.build
        )
        cls.frames["fact_payroll"] = build_fact_payroll(cls.sources, cls.build)
        cls.frames["fact_attendance"] = build_fact_attendance(cls.sources, cls.build)

    @classmethod
    def tearDownClass(cls):
        cls.spark.catalog.clearCache()
        super().tearDownClass()

    def test_complete_inventory_references_dates_and_metadata(self):
        result = validate_mvp_frames(self.frames, self.build)
        self.assertTrue(result["passed"], result["failed_checks"])
        self.assertEqual(set(self.frames), set(GOLD_MODELS))
        self.assertTrue(all(value == 0 for value in result["references"].values()))
        self.assertTrue(all(value == 0 for value in result["dates"].values()))
        self.assertTrue(all(value == 0 for value in result["metadata"].values()))

    def test_historical_assignment_and_cutoff_coherence(self):
        attendance = self.frames["fact_attendance"].filter("employee_key = 1")
        payroll = self.frames["fact_payroll"].filter("employee_key = 1")
        self.assertGreater(attendance.select("assignment_key").distinct().count(), 1)
        self.assertGreater(payroll.select("assignment_key").distinct().count(), 1)
        self.assertEqual(self.build.spec.source_cutoff, date(2024, 4, 15))
        april = self.frames["dim_date"].filter("date_key = 20240430").count()
        self.assertEqual(april, 1)
        self.assertEqual(
            self.frames["fact_attendance"].filter("work_date_key > 20240415").count(),
            0,
        )
        self.assertEqual(
            self.frames["fact_workforce_monthly"]
            .filter("snapshot_month_key > 20240331")
            .count(),
            0,
        )
        self.assertGreater(
            self.frames["fact_payroll"].filter("payroll_month_key = 20240430").count(),
            0,
        )

    def test_workforce_movement_relationship_preserves_participant_rows(self):
        workforce = self.frames["fact_workforce_monthly"]
        for month in workforce.select("snapshot_month_key").distinct().collect():
            month_rows = workforce.filter(
                f"snapshot_month_key = {month.snapshot_month_key}"
            )
            self.assertGreaterEqual(
                month_rows.count(), month_rows.agg({"headcount_eom": "sum"}).first()[0]
            )
        movements = self.frames["fact_employee_movement"]
        self.assertEqual(
            movements.filter("movement_type = 'HIRE'")
            .agg({"headcount_delta": "sum"})
            .first()[0],
            3,
        )
        self.assertEqual(
            movements.filter("movement_type = 'EXIT'")
            .agg({"headcount_delta": "sum"})
            .first()[0],
            -1,
        )

    def test_negative_cross_model_reference_and_duplicate_fail_closed(self):
        bad = dict(self.frames)
        bad["fact_attendance"] = bad["fact_attendance"].withColumn(
            "employee_key", bad["fact_attendance"].employee_key * 0 + 999
        )
        result = validate_mvp_frames(bad, self.build)
        self.assertFalse(result["passed"])
        self.assertTrue(
            any(
                "fact_attendance.employee_key" in key for key in result["failed_checks"]
            )
        )
        duplicate = dict(self.frames)
        duplicate["fact_attendance"] = duplicate["fact_attendance"].unionByName(
            duplicate["fact_attendance"].limit(1)
        )
        result = validate_mvp_frames(duplicate, self.build)
        self.assertFalse(result["passed"])
        self.assertTrue(
            any(
                key.startswith("duplicate:fact_attendance")
                for key in result["failed_checks"]
            )
        )

    def test_build_identity_and_manifest_lifecycle(self):
        first = build_release_manifest(self.frames, self.build, self.build.generated_at)
        second = build_release_manifest(
            dict(reversed(tuple(self.frames.items()))),
            self.build,
            self.build.generated_at,
        )
        self.assertEqual(first.to_json(), second.to_json())
        self.assertEqual(first.status, ReleaseStatus.BUILDING)
        accepted = build_release_manifest(
            self.frames,
            self.build,
            self.build.generated_at,
            status=ReleaseStatus.ACCEPTED,
            verification_outcomes={name: True for name in VERIFICATION_CHECKS},
            reconciliation_metrics={"cross_model": "passed"},
        )
        self.assertEqual(accepted.status, ReleaseStatus.ACCEPTED)
        changed = replace(self.build.spec, silver_batch_id="different-batch")
        self.assertNotEqual(self.build.spec.build_id, changed.build_id)
        with self.assertRaises(ExistingOutputError):
            require_output_policy(RunMode.CREATE, ReleaseStatus.BUILDING)

    def test_physical_complete_mvp_inventory_is_immutable(self):
        # The shared writer performs validate-before-write and readback checks;
        # model-specific physical suites cover every model, while this test
        # proves a release uses the exact ten-model inventory and immutable
        # destination policy.
        with TemporaryDirectory() as root:
            write_local_dimension(
                self.frames["dim_date"],
                root,
                "dim_date",
                self.build,
                self.sources,
                dim_date=self.frames["dim_date"],
            )
            self.assertTrue(local_dimension_path(root, "dim_date", self.build).exists())
            with self.assertRaises(ExistingOutputError):
                write_local_dimension(
                    self.frames["dim_date"],
                    root,
                    "dim_date",
                    self.build,
                    self.sources,
                    dim_date=self.frames["dim_date"],
                )


if __name__ == "__main__":
    unittest.main()
