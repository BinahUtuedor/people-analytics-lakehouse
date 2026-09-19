"""Workforce participation, closing population and immutable physical proof."""

from dataclasses import replace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import unittest

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, BooleanType

from spark.gold.contracts import get_gold_schema
from spark.gold.dimension_validation import enforce_schema
from spark.gold.hashing import record_hash, spark_record_hash
from spark.gold.manifest import ExistingOutputError
from spark.gold.workforce_history import (
    build_dim_employee_assignment,
    build_fact_employee_movement,
)
from spark.gold.workforce_monthly import (
    WORKFORCE_MODEL,
    build_fact_workforce_monthly,
    closed_months,
    monthly_reconciliation,
    validate_workforce_monthly,
)
from spark.gold.transform import build_dim_date
from spark.gold.validate import GoldValidationError
from spark.gold.writer import (
    write_local_dimension,
    verify_local_dimension,
    local_dimension_path,
)
from tests.spark.gold.dimension_fixtures import (
    CoreDimensionTestCase,
    literal_frame,
    core_dimensions,
)


class WorkforceTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.build = replace(
            cls.build,
            spec=replace(
                cls.build.spec,
                reporting_start=date(2024, 1, 1),
                reporting_end=date(2024, 4, 15),
                source_cutoff=date(2024, 4, 15),
            ),
        )
        number, day, text = LongType(), DateType(), StringType()
        cls.employee_dates = [
            (1, date(2024, 1, 1), None),
            (2, date(2024, 1, 1), None),
            (3, date(2024, 1, 15), None),
            (4, date(2024, 1, 31), None),
            (5, date(2024, 2, 1), None),
            (6, date(2024, 1, 1), date(2024, 1, 15)),
            (7, date(2024, 1, 1), date(2024, 2, 29)),
            (8, date(2024, 1, 1), date(2024, 3, 1)),
            (9, date(2024, 2, 29), date(2024, 2, 29)),
            (10, date(2024, 4, 1), None),
        ]
        employees = literal_frame(
            cls.spark,
            [
                ("employee_id", number),
                ("hire_date", day),
                ("termination_date", day),
            ],
            cls.employee_dates,
        )
        employees = (
            employees.withColumn(
                "employee_number", F.concat(F.lit("E"), F.col("employee_id"))
            )
            .withColumn(
                "employment_status",
                F.when(F.col("termination_date").isNull(), "Active").otherwise(
                    "Terminated"
                ),
            )
            .withColumn("employment_type", F.lit("Permanent"))
            .withColumn("department_id", F.lit(1).cast("long"))
            .withColumn("role_id", F.lit(1).cast("long"))
            .withColumn("location_id", F.lit(1).cast("long"))
            .withColumn("manager_id", F.lit(None).cast("long"))
            .withColumn("email", F.lit("excluded@example.invalid"))
            .withColumn("salary", F.lit("123.45"))
        )
        promotions = literal_frame(
            cls.spark,
            [
                ("promotion_id", number),
                ("employee_id", number),
                ("promotion_date", day),
                ("new_role_id", number),
            ],
            [(11, 1, date(2024, 2, 10), 2), (12, 1, date(2024, 3, 20), 1)],
        )
        transfers = literal_frame(
            cls.spark,
            [
                ("transfer_id", number),
                ("employee_id", number),
                ("transfer_date", day),
                ("new_department_id", number),
                ("new_location_id", number),
                ("new_manager_id", number),
            ],
            [(21, 1, date(2024, 3, 10), 2, 2, 2)],
        )
        exits = literal_frame(
            cls.spark,
            [
                ("exit_event_id", number),
                ("employee_id", number),
                ("exit_date", day),
                ("exit_type", text),
                ("voluntary_flag", BooleanType()),
                ("regrettable_flag", BooleanType()),
            ],
            [
                (100 + i, i, end, "Voluntary", True, False)
                for i, _, end in cls.employee_dates
                if end
            ],
        )
        cls.sources = dict(
            cls.sources,
            employees=employees,
            promotions=promotions,
            transfers=transfers,
            employee_exits=exits,
        )
        # Materialize small Silver fixtures before reusing their expression
        # plans across dimension/history construction and negative tests.
        for frame in cls.sources.values():
            frame.cache().count()
        cls.sources.update(core_dimensions(cls.spark, cls.sources, cls.build))
        cls.sources["dim_employee_assignment"] = build_dim_employee_assignment(
            employees, promotions, transfers, exits, cls.build
        )
        for frame in cls.sources.values():
            frame.cache().count()
        cls.output = build_fact_workforce_monthly(cls.sources, cls.build).cache()
        cls.rows = {
            (r.employee_key, r.snapshot_month_key): r for r in cls.output.collect()
        }

    @classmethod
    def tearDownClass(cls):
        cls.output.unpersist()
        for frame in cls.sources.values():
            frame.unpersist()
        super().tearDownClass()

    def validate(self, output=None, sources=None):
        return validate_workforce_monthly(
            self.output if output is None else output,
            self.build,
            self.sources if sources is None else sources,
        )

    def changed(self, field, value):
        frame = self.output.withColumn(field, value)
        if not field.startswith("_"):
            frame = frame.withColumn(
                "_record_hash", spark_record_hash(frame, WORKFORCE_MODEL)
            )
        return enforce_schema(frame, WORKFORCE_MODEL)

    def test_balanced_wrong_headcount_flags_rejected_per_employee(self):
        # Swapping two flags preserves monthly totals but violates closing
        # membership. Valid hashes ensure the business gate detects this.
        january = F.col("snapshot_month_key") == 20240131
        altered = self.output.withColumn(
            "headcount_eom",
            F.when(january & (F.col("employee_key") == 1), 0)
            .when(january & (F.col("employee_key") == 6), 1)
            .otherwise(F.col("headcount_eom")),
        )
        altered = altered.withColumn(
            "tenure_days_eom",
            F.when(january & (F.col("employee_key") == 1), F.lit(None).cast("int"))
            .when(january & (F.col("employee_key") == 6), 30)
            .otherwise(F.col("tenure_days_eom")),
        )
        altered = enforce_schema(
            altered.withColumn(
                "_record_hash", spark_record_hash(altered, WORKFORCE_MODEL)
            ),
            WORKFORCE_MODEL,
        )
        with self.assertRaisesRegex(GoldValidationError, "source_reconstruction"):
            self.validate(altered)

    def test_valid_but_wrong_organisation_reference_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "source_reconstruction"):
            self.validate(self.changed("department_key", F.lit(2).cast("long")))

    def test_empty_population_reconciles_all_closed_months(self):
        sources = dict(
            self.sources,
            employees=self.sources["employees"].limit(0),
            employee_exits=self.sources["employee_exits"].limit(0),
        )
        output = build_fact_workforce_monthly(sources, self.build)
        self.assertEqual(output.count(), 0)
        months = monthly_reconciliation(output, sources, self.build).collect()
        self.assertEqual(len(months), 3)
        for month in months:
            self.assertEqual(
                (
                    month.expected_participants,
                    month.expected_closing,
                    month.fact_rows,
                    month.headcount_sum,
                ),
                (0, 0, 0, 0),
            )

    def test_calendar_closed_leap_months_and_midmonth_cutoff(self):
        self.assertEqual(
            self.sources["dim_date"].filter(F.col("date_key") == 20240430).count(), 1
        )
        self.assertEqual({k[1] for k in self.rows}, {20240131, 20240229, 20240331})
        self.assertTrue(
            all(
                r.snapshot_date.day in (29, 31)
                for r in closed_months(self.sources["dim_date"], self.build).collect()
            )
        )

    def test_reference_calendar_does_not_extend_assignment_or_events(self):
        future = (
            self.sources["promotions"]
            .limit(1)
            .withColumn("promotion_id", F.lit(999).cast("long"))
            .withColumn("promotion_date", F.lit(date(2024, 4, 20)))
        )
        promotions = self.sources["promotions"].unionByName(future)
        assignments = build_dim_employee_assignment(
            self.sources["employees"],
            promotions,
            self.sources["transfers"],
            self.sources["employee_exits"],
            self.build,
        )
        self.assertEqual(
            assignments.filter(
                F.col("valid_to_exclusive") > F.lit(date(2024, 4, 16))
            ).count(),
            0,
        )
        self.assertEqual(
            assignments.filter(
                F.col("valid_from_date") > F.lit(date(2024, 4, 15))
            ).count(),
            0,
        )
        movement = build_fact_employee_movement(
            self.sources["employees"],
            promotions,
            self.sources["transfers"],
            self.sources["employee_exits"],
            assignments,
            self.build,
        )
        self.assertEqual(movement.filter(F.col("event_date_key") > 20240415).count(), 0)

    def test_calendar_exact_eom_and_before_first_eom(self):
        for cutoff, expected in [
            (date(2024, 2, 29), [20240131, 20240229]),
            (date(2024, 2, 28), [20240131]),
            (date(2024, 1, 30), []),
        ]:
            with self.subTest(cutoff=cutoff):
                build = replace(
                    self.build,
                    spec=replace(
                        self.build.spec, source_cutoff=cutoff, reporting_end=cutoff
                    ),
                )
                self.assertEqual(
                    sorted(
                        r.snapshot_month_key
                        for r in closed_months(
                            self.sources["dim_date"], build
                        ).collect()
                    ),
                    expected,
                )

    def test_reporting_end_bounds_coverage(self):
        build = replace(
            self.build, spec=replace(self.build.spec, reporting_end=date(2024, 2, 15))
        )
        self.assertEqual(
            [
                r.snapshot_month_key
                for r in closed_months(self.sources["dim_date"], build).collect()
            ],
            [20240131],
        )

    def test_hire_boundaries_and_active_population(self):
        for employee in (1, 2, 3, 4):
            self.assertEqual(self.rows[employee, 20240131].headcount_eom, 1)
        self.assertNotIn((5, 20240131), self.rows)
        self.assertEqual(self.rows[5, 20240229].headcount_eom, 1)
        self.assertFalse(any(employee == 10 for employee, _ in self.rows))

    def test_exit_before_eom_retained_but_not_next_month(self):
        row = self.rows[6, 20240131]
        self.assertEqual(
            (row.headcount_eom, row.assignment_date_key, row.tenure_days_eom),
            (0, 20240115, None),
        )
        self.assertNotIn((6, 20240229), self.rows)

    def test_exit_on_eom_and_same_day_hire_exit(self):
        for employee in (7, 9):
            row = self.rows[employee, 20240229]
            self.assertEqual(
                (row.headcount_eom, row.assignment_date_key, row.tenure_days_eom),
                (0, 20240229, None),
            )
            self.assertNotIn((employee, 20240331), self.rows)

    def test_exit_on_month_start_retains_month(self):
        row = self.rows[8, 20240331]
        self.assertEqual((row.headcount_eom, row.assignment_date_key), (0, 20240301))

    def test_future_termination_does_not_extend_cutoff(self):
        employees = self.sources["employees"].withColumn(
            "termination_date",
            F.when(F.col("employee_id") == 2, F.lit(date(2024, 5, 1))).otherwise(
                F.col("termination_date")
            ),
        )
        actual = build_fact_workforce_monthly(
            dict(self.sources, employees=employees), self.build
        )
        self.assertEqual(actual.exceptAll(self.output).count(), 0)
        self.assertEqual(actual.count(), len(self.rows))

    def test_assignment_promotion_transfer_sequential_and_manager(self):
        expected = {
            20240131: (1, 1, 1, 0),
            20240229: (1, 2, 1, 0),
            20240331: (2, 1, 2, 2),
        }
        for month, state in expected.items():
            row = self.rows[1, month]
            self.assertEqual(
                (
                    row.department_key,
                    row.job_role_key,
                    row.location_key,
                    row.manager_employee_key,
                ),
                state,
            )
            self.assertEqual(row.assignment_date_key, month)
        self.assertEqual(
            len({self.rows[1, month].assignment_key for month in expected}), 3
        )

    def test_history_wins_over_current_organisation(self):
        employees = (
            self.sources["employees"]
            .withColumn("department_id", F.lit(999).cast("long"))
            .withColumn("is_active", F.lit(False))
        )
        actual = build_fact_workforce_monthly(
            dict(self.sources, employees=employees), self.build
        )
        self.assertEqual(actual.exceptAll(self.output).count(), 0)

    def test_movement_count_does_not_multiply_months(self):
        movements = build_fact_employee_movement(
            *[
                self.sources[n]
                for n in (
                    "employees",
                    "promotions",
                    "transfers",
                    "employee_exits",
                    "dim_employee_assignment",
                )
            ],
            self.build,
        )
        self.assertEqual(movements.filter("employee_key = 1").count(), 4)
        self.assertEqual(self.output.filter("employee_key = 1").count(), 3)
        self.assertEqual(
            movements.filter("employee_key = 1 AND event_date_key >= 20240301").count(),
            2,
        )
        self.assertEqual(
            self.output.filter(
                "employee_key = 1 AND snapshot_month_key = 20240331"
            ).count(),
            1,
        )

    def test_tenure_new_hire_and_leap_days(self):
        self.assertEqual(self.rows[4, 20240131].tenure_days_eom, 0)
        self.assertEqual(self.rows[4, 20240229].tenure_days_eom, 29)
        for row in self.rows.values():
            if row.headcount_eom:
                self.assertGreaterEqual(row.tenure_days_eom, 0)
            else:
                self.assertIsNone(row.tenure_days_eom)

    def test_tenure_exact_year_and_leap_anniversary(self):
        employees = self.sources["employees"].withColumn(
            "hire_date",
            F.when(F.col("employee_id") == 1, F.lit(date(2023, 1, 31)))
            .when(F.col("employee_id") == 2, F.lit(date(2020, 2, 29)))
            .otherwise(F.col("hire_date")),
        )
        actual = build_fact_workforce_monthly(
            dict(self.sources, employees=employees), self.build
        )
        rows = {(r.employee_key, r.snapshot_month_key): r for r in actual.collect()}
        self.assertEqual(rows[1, 20240131].tenure_days_eom, 365)
        self.assertEqual(rows[2, 20240229].tenure_days_eom, 1461)

    def test_monthly_reconciliation_against_hand_counted_population(self):
        expected = {20240131: (7, 6), 20240229: (8, 6), 20240331: (6, 5)}
        actual = monthly_reconciliation(self.output, self.sources, self.build).collect()
        self.assertEqual(len(actual), 3)
        for row in actual:
            participants, closing = expected[row.snapshot_month_key]
            self.assertEqual(
                (row.expected_participants, row.fact_rows, row.distinct_employees),
                (participants,) * 3,
            )
            self.assertEqual((row.expected_closing, row.headcount_sum), (closing,) * 2)
            self.assertEqual(row.non_closing_participant_rows, participants - closing)
        self.assertEqual(
            {r.employee_key for r in self.rows.values() if r.headcount_eom == 0},
            {6, 7, 8, 9},
        )

    def test_exact_schema_governance_hash_oracle_and_metadata(self):
        self.assertEqual(self.output.schema, get_gold_schema(WORKFORCE_MODEL))
        self.assertFalse(
            {"name", "email", "dob", "date_of_birth", "gender", "salary", "free_text"}
            & set(self.output.columns)
        )
        for row in self.rows.values():
            self.assertEqual(
                row._record_hash, record_hash(WORKFORCE_MODEL, row.asDict())
            )
            self.assertEqual(row._gold_build_id, self.build.spec.build_id)
            self.assertEqual(row._source_batch_id, self.build.spec.silver_batch_id)
            self.assertEqual(
                row._gold_generated_at, self.build.generated_at.replace(tzinfo=None)
            )
        self.assertEqual(
            len({r._record_hash for r in self.rows.values()}), len(self.rows)
        )

    def test_determinism_rebuild_reorder_repartition(self):
        reordered = {
            name: frame.orderBy(F.col(frame.columns[0]).desc()).repartition(2)
            for name, frame in self.sources.items()
        }
        actual = build_fact_workforce_monthly(reordered, self.build)
        self.assertEqual(actual.exceptAll(self.output).count(), 0)
        self.assertEqual(self.output.exceptAll(actual).count(), 0)

    def test_duplicate_grain_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "employee_month_grain"):
            self.validate(self.output.unionByName(self.output.limit(1)))

    def test_missing_or_overlapping_assignment_fails_closed(self):
        original = self.sources["dim_employee_assignment"]
        for assignment in (
            original.filter("employee_key != 1"),
            original.unionByName(
                original.filter("employee_key = 1")
                .limit(1)
                .withColumn("assignment_key", F.lit("f" * 64))
            ),
        ):
            with (
                self.subTest(),
                self.assertRaisesRegex(
                    GoldValidationError,
                    "required_assignment|assignment_unique_at_reference",
                ),
            ):
                build_fact_workforce_monthly(
                    dict(self.sources, dim_employee_assignment=assignment), self.build
                )

    def test_missing_employee_and_organisation_parents(self):
        for name in ("dim_employee", "dim_department", "dim_job_role", "dim_location"):
            with (
                self.subTest(parent=name),
                self.assertRaisesRegex(GoldValidationError, "foreign_key"),
            ):
                self.validate(
                    sources=dict(
                        self.sources, **{name: self.sources[name].filter("is_unknown")}
                    )
                )

    def test_unknown_attributes_preserved(self):
        assignment = self.sources["dim_employee_assignment"]
        for key in (
            "department_key",
            "job_role_key",
            "location_key",
            "manager_employee_key",
        ):
            assignment = assignment.withColumn(key, F.lit(0).cast("long"))
        assignment = assignment.withColumn(
            "_record_hash", spark_record_hash(assignment, "dim_employee_assignment")
        )
        assignment = enforce_schema(assignment, "dim_employee_assignment")
        actual = build_fact_workforce_monthly(
            dict(self.sources, dim_employee_assignment=assignment), self.build
        )
        self.assertEqual(actual.count(), len(self.rows))
        self.assertEqual({r.department_key for r in actual.collect()}, {0})

    def test_missing_calendar_fails(self):
        dates = self.sources["dim_date"].filter("date_key != 20240229")
        with self.assertRaises(GoldValidationError):
            self.validate(sources=dict(self.sources, dim_date=dates))

    def test_date_and_manager_foreign_keys_rejected(self):
        for field, value, gate in (
            ("snapshot_month_key", F.lit(19990131), "snapshot_month_key_foreign_key"),
            ("assignment_date_key", F.lit(19990101), "assignment_date_key_foreign_key"),
            (
                "manager_employee_key",
                F.lit(999).cast("long"),
                "manager_employee_key_foreign_key",
            ),
        ):
            # One row avoids a separate duplicate-grain failure hiding the FK.
            altered = self.changed(field, value).limit(1)
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(GoldValidationError, gate),
            ):
                self.validate(altered)

    def test_wrong_date_reference_assignment_and_tenure_rejected(self):
        for field, value in (
            ("snapshot_month_key", F.lit(20240130)),
            ("assignment_date_key", F.lit(20240101)),
            ("assignment_key", F.lit("0")),
            (
                "tenure_days_eom",
                F.when(F.col("headcount_eom") == 1, F.lit(999)).cast("int"),
            ),
        ):
            altered = self.changed(field, value)
            altered = altered.withColumn(
                "_record_hash", spark_record_hash(altered, WORKFORCE_MODEL)
            )
            altered = enforce_schema(altered, WORKFORCE_MODEL)
            with self.subTest(field=field), self.assertRaises(GoldValidationError):
                self.validate(altered)

    def test_headcount_domain_and_null_tenure_rules_rejected(self):
        for field, value in (
            ("headcount_eom", F.lit(2)),
            ("tenure_days_eom", F.lit(-1)),
            ("tenure_days_eom", F.lit(None).cast("int")),
        ):
            with self.subTest(field=field), self.assertRaises(GoldValidationError):
                self.validate(self.changed(field, value))

    def test_wrong_hash_metadata_and_schema_rejected(self):
        for field, value in (
            ("_record_hash", F.lit("wrong")),
            ("_gold_build_id", F.lit("0" * 64)),
            ("_source_batch_id", F.lit("other")),
            ("_gold_generated_at", F.lit("2000-01-01").cast("timestamp")),
        ):
            with self.subTest(field=field), self.assertRaises(GoldValidationError):
                self.validate(self.changed(field, value))
        with self.assertRaisesRegex(GoldValidationError, "exact_schema"):
            self.validate(self.output.withColumn("email", F.lit("excluded")))

    def test_missing_or_inconsistent_exit_evidence(self):
        for exits in (
            self.sources["employee_exits"].limit(0),
            self.sources["employee_exits"].withColumn(
                "exit_date", F.lit(date(2024, 1, 1))
            ),
        ):
            with (
                self.subTest(),
                self.assertRaisesRegex(GoldValidationError, "exit_reconciliation"),
            ):
                build_fact_workforce_monthly(
                    dict(self.sources, employee_exits=exits), self.build
                )

    def test_duplicate_and_wrong_batch_source_rejected(self):
        employees = self.sources["employees"]
        for bad in (
            employees.unionByName(employees.limit(1)),
            employees.withColumn("_batch_id", F.lit("other")),
        ):
            with self.subTest(), self.assertRaises(GoldValidationError):
                build_fact_workforce_monthly(
                    dict(self.sources, employees=bad), self.build
                )

    def test_missing_and_extra_population_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "monthly_reconciliation"):
            self.validate(self.output.filter("employee_key != 6"))
        extra = self.output.filter("employee_key = 6").withColumn(
            "snapshot_month_key", F.lit(20240331)
        )
        extra = extra.withColumn(
            "_record_hash", spark_record_hash(extra, WORKFORCE_MODEL)
        )
        extra = enforce_schema(extra, WORKFORCE_MODEL)
        with self.assertRaisesRegex(GoldValidationError, "monthly_reconciliation"):
            self.validate(self.output.unionByName(extra))

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_local_physical_roundtrip_and_immutable_output(self):
        with TemporaryDirectory(dir="/workspace") as root:
            actual, report = write_local_dimension(
                self.output, root, WORKFORCE_MODEL, self.build, self.sources
            )
            self.assertTrue(report.passed)
            self.assertTrue(
                (
                    local_dimension_path(root, WORKFORCE_MODEL, self.build)
                    / "reporting_year=2024"
                ).is_dir()
            )
            self.assertEqual(actual.schema, get_gold_schema(WORKFORCE_MODEL))
            self.assertEqual(actual.count(), 21)
            self.assertEqual(actual.exceptAll(self.output).count(), 0)
            self.assertEqual({r.headcount_eom for r in actual.collect()}, {0, 1})
            before = {
                str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()
            }
            with self.assertRaises(ExistingOutputError):
                write_local_dimension(
                    self.output, root, WORKFORCE_MODEL, self.build, self.sources
                )
            for mode in ("append", "overwrite"):
                with self.assertRaises(TypeError):
                    write_local_dimension(
                        self.output,
                        root,
                        WORKFORCE_MODEL,
                        self.build,
                        self.sources,
                        mode=mode,
                    )
            verify_local_dimension(
                self.output, root, WORKFORCE_MODEL, self.build, self.sources
            )
            self.assertEqual(
                before,
                {str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()},
            )

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_invalid_output_prevents_write_and_corruption_fails_readback(self):
        bad = self.changed("_record_hash", F.lit("wrong"))
        with TemporaryDirectory(dir="/workspace") as root:
            with self.assertRaises(GoldValidationError):
                write_local_dimension(
                    bad, root, WORKFORCE_MODEL, self.build, self.sources
                )
            path = local_dimension_path(root, WORKFORCE_MODEL, self.build)
            self.assertFalse(path.exists())
            bad.write.mode("errorifexists").parquet(str(path))
            with self.assertRaises(GoldValidationError):
                verify_local_dimension(
                    self.output, root, WORKFORCE_MODEL, self.build, self.sources
                )
