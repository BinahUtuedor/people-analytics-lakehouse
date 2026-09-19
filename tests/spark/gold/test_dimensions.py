"""Executable Phase 1 dimensions, rejection gates and reference hash equivalence."""

from dataclasses import replace
from datetime import date, timedelta
from unittest import TestCase

from pyspark.sql import functions as F
from spark.gold.contracts import GOLD_MODELS, get_gold_schema, unknown_business_row
from spark.gold.hashing import record_hash, spark_record_hash
from spark.gold.dimension_validation import (
    DimensionBuild,
    enforce_schema,
    validate_core_dimension,
)
from spark.gold.transform import (
    build_dim_date,
    build_dim_employee,
    build_dim_department,
    build_dim_location,
    build_dim_job_role,
)
from spark.gold.validate import GoldValidationError
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase, core_dimensions


class CoreDimensionTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.frames = core_dimensions(cls.spark, cls.sources, cls.build)
        for frame in cls.frames.values():
            frame.cache().count()

    @classmethod
    def tearDownClass(cls):
        for frame in getattr(cls, "frames", {}).values():
            frame.unpersist()
        super().tearDownClass()

    def test_exact_schemas_unknowns_metadata_and_reference_hashes(self):
        for model, frame in self.frames.items():
            with self.subTest(model=model):
                self.assertEqual(frame.schema, get_gold_schema(model))
                rows = frame.collect()  # At most 95 declared fixture rows.
                unknown = [r for r in rows if r.is_unknown]
                self.assertEqual(len(unknown), 1)
                self.assertEqual(
                    {
                        f.name: unknown[0][f.name]
                        for f in GOLD_MODELS[model].business_fields
                    },
                    unknown_business_row(model),
                )
                for row in rows:
                    self.assertEqual(row._source_batch_id, "batch-a")
                    self.assertEqual(row._gold_build_id, self.build.spec.build_id)
                    self.assertEqual(
                        row._gold_generated_at,
                        self.build.generated_at.replace(tzinfo=None),
                    )
                    self.assertEqual(row._record_hash, record_hash(model, row.asDict()))

    def test_calendar_against_independent_python_calendar(self):
        rows = (
            self.frames["dim_date"]
            .filter(~F.col("is_unknown"))
            .orderBy("calendar_date")
            .collect()
        )
        self.assertEqual(len(rows), 94)
        for index, row in enumerate(rows):
            day = self.build.spec.reporting_start + timedelta(days=index)
            self.assertEqual(row.calendar_date, day)
            self.assertEqual(
                (row.iso_week_year, row.iso_week_number, row.iso_day_of_week),
                tuple(day.isocalendar()),
            )
            self.assertEqual(row.date_key, day.year * 10000 + day.month * 100 + day.day)
            self.assertEqual(row.is_weekend, day.weekday() >= 5)
        leap = next(r for r in rows if r.calendar_date == date(2024, 2, 29))
        self.assertEqual(leap.month_start_date, date(2024, 2, 1))
        self.assertEqual(leap.month_end_date, date(2024, 2, 29))
        self.assertEqual(leap.day_name, "Thursday")
        self.assertEqual(leap.month_name, "February")

    def test_iso_week_year_differs_from_calendar_year(self):
        build = replace(
            self.build,
            spec=replace(
                self.build.spec,
                reporting_start=date(2020, 12, 28),
                reporting_end=date(2021, 1, 4),
                source_cutoff=date(2021, 1, 4),
            ),
        )
        rows = (
            build_dim_date(self.spark, build)
            .filter(~F.col("is_unknown"))
            .orderBy("calendar_date")
            .collect()
        )
        self.assertEqual(len(rows), 35)
        for row in rows:
            self.assertEqual(
                (row.iso_week_year, row.iso_week_number, row.iso_day_of_week),
                tuple(row.calendar_date.isocalendar()),
            )
        self.assertEqual(rows[4].calendar_year, 2021)
        self.assertEqual(rows[4].iso_week_year, 2020)

    def test_employee_active_and_terminated_values(self):
        rows = {r.employee_key: r for r in self.frames["dim_employee"].collect()}
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1].termination_date_key, 0)
        self.assertEqual(rows[1].hire_date_key, 20231231)
        self.assertEqual(rows[2].termination_date_key, 20240229)
        self.assertEqual(rows[2].current_employment_status, "Terminated")
        self.assertEqual(rows[2].current_employment_type, "Fixed-term")
        for name in (
            "email",
            "annual_salary",
            "date_of_birth",
            "gender",
            "contract_type",
            "department_key",
            "assignment_key",
        ):
            self.assertNotIn(name, self.frames["dim_employee"].columns)

    def test_department_location_role_values_and_reconciliation(self):
        department = (
            self.frames["dim_department"].filter(~F.col("is_unknown")).collect()
        )
        self.assertEqual({r.business_unit_name for r in department}, {"Technology"})
        self.assertEqual({r.business_unit_id for r in department}, {10})
        role = self.frames["dim_job_role"].filter(F.col("job_role_key") == 1).first()
        self.assertEqual(role.grade, "G07")
        self.assertNotIn("salary_band_min", self.frames["dim_job_role"].columns)
        for model in self.frames:
            report = validate_core_dimension(
                self.frames[model],
                model,
                self.build,
                self.sources,
                dim_date=self.frames["dim_date"],
            )
            metric = next(
                r.metrics
                for r in report.results
                if r.check_name == "source_reconciliation"
            )
            self.assertEqual(metric["real_rows"], "94" if model == "dim_date" else "2")
            self.assertEqual(metric["unknown_rows"], "1")

    def test_lifecycle_invalid_ids_duplicate_numbers_and_dates_outside_coverage_fail(
        self,
    ):
        source = self.sources["employees"]
        invalid = [
            source.withColumn("employee_id", F.lit(0).cast("long")),
            source.withColumn("employee_number", F.lit("duplicate")),
            source.withColumn("hire_date", F.lit(date(2024, 3, 1))),
            source.withColumn("hire_date", F.lit(date(2020, 1, 1))),
            source.withColumn("hire_date", F.lit(None).cast("date")),
        ]
        for frame in invalid:
            with (
                self.subTest(plan=frame.schema.simpleString()),
                self.assertRaises(GoldValidationError),
            ):
                build_dim_employee(frame, self.frames["dim_date"], self.build)

    def test_department_orphan_and_duplicate_parent_fail(self):
        departments, units = self.sources["departments"], self.sources["business_units"]
        with self.assertRaises(GoldValidationError):
            build_dim_department(
                departments.withColumn("business_unit_id", F.lit(999).cast("long")),
                units,
                self.build,
            )
        with self.assertRaises(GoldValidationError):
            build_dim_department(departments, units.unionByName(units), self.build)
        with self.assertRaises(GoldValidationError):
            build_dim_department(
                departments.withColumn("cost_center", F.lit("same")), units, self.build
            )

    def test_required_location_role_values_and_batch_rejected(self):
        for source, field, builder in (
            ("locations", "timezone", build_dim_location),
            ("locations", "city", build_dim_location),
            ("job_roles", "grade", build_dim_job_role),
            ("job_roles", "role_name", build_dim_job_role),
        ):
            with self.subTest(field=field), self.assertRaises(GoldValidationError):
                builder(
                    self.sources[source].withColumn(field, F.lit(None).cast("string")),
                    self.build,
                )
        for batch in (None, "other"):
            with self.assertRaises(GoldValidationError):
                build_dim_job_role(
                    self.sources["job_roles"].withColumn(
                        "_batch_id", F.lit(batch).cast("string")
                    ),
                    self.build,
                )

    def test_source_type_and_duplicate_keys_are_not_coerced_or_deduplicated(self):
        roles = self.sources["job_roles"]
        with self.assertRaises(GoldValidationError):
            build_dim_job_role(
                roles.withColumn("role_id", F.col("role_id").cast("string")), self.build
            )
        with self.assertRaises(GoldValidationError):
            build_dim_job_role(roles.unionByName(roles), self.build)

    def test_business_hashes_ignore_order_partitions_and_metadata(self):
        roles = self.sources["job_roles"]
        build = replace(
            self.build,
            generated_at=self.build.generated_at + timedelta(days=2),
            spec=replace(self.build.spec, code_revision="different"),
        )
        frame = build_dim_job_role(
            roles.orderBy(F.desc("role_id")).repartition(2), build
        )
        old = {
            r.job_role_key: r._record_hash
            for r in self.frames["dim_job_role"].collect()
        }
        self.assertEqual(old, {r.job_role_key: r._record_hash for r in frame.collect()})
        changed = build_dim_job_role(
            roles.withColumn("grade", F.lit("G11")), self.build
        )
        self.assertNotEqual(
            old[1], changed.filter(F.col("job_role_key") == 1).first()._record_hash
        )

    def test_corrupted_hash_unknown_and_coverage_fail_validation(self):
        model = "dim_job_role"
        source = self.frames[model]
        cases = [
            source.withColumn("_record_hash", F.lit("bad")),
            source.filter(~F.col("is_unknown")),
            source.filter(F.col("job_role_key") != 1),
        ]
        for frame in cases:
            with self.assertRaises(GoldValidationError):
                validate_core_dimension(
                    enforce_schema(frame, model), model, self.build, self.sources
                )

    def test_missing_date_parent_wrong_build_and_calendar_corruption_fail(self):
        wrong = self.frames["dim_date"].withColumn("_gold_build_id", F.lit("f" * 64))
        with self.assertRaises(GoldValidationError):
            build_dim_employee(
                self.sources["employees"], enforce_schema(wrong, "dim_date"), self.build
            )
        wrong = self.frames["dim_date"].filter(F.col("date_key") != 20240229)
        with self.assertRaises(GoldValidationError):
            validate_core_dimension(wrong, "dim_date", self.build, {})

    def test_phase2_and_runtime_mismatch_rejected(self):
        with self.assertRaises(ValueError):
            self.build.context("dim_employee_assignment")
        self.spark.conf.set("spark.sql.session.timeZone", "Europe/London")
        try:
            with self.assertRaises(ValueError):
                build_dim_date(self.spark, self.build)
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def test_empty_source_still_produces_exactly_one_unknown(self):
        roles = self.sources["job_roles"].limit(0)
        result = build_dim_job_role(roles, self.build)
        self.assertEqual(result.count(), 1)
        self.assertTrue(result.first().is_unknown)

    def test_one_date_range_and_metadata_only_hash_changes(self):
        build = replace(
            self.build,
            spec=replace(
                self.build.spec,
                reporting_start=date(2024, 2, 29),
                reporting_end=date(2024, 2, 29),
                source_cutoff=date(2024, 2, 29),
            ),
        )
        result = build_dim_date(self.spark, build)
        self.assertEqual(result.count(), 2)
        original = self.frames["dim_job_role"]
        altered = original.withColumn("_source_batch_id", F.lit("different-batch"))
        altered = altered.withColumn(
            "recomputed", spark_record_hash(altered, "dim_job_role")
        )
        self.assertEqual(
            altered.filter(F.col("recomputed") != F.col("_record_hash")).count(), 0
        )
