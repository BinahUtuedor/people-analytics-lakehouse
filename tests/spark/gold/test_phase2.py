from datetime import date, timedelta
import unittest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, BooleanType
from tempfile import TemporaryDirectory
from pathlib import Path
import os
from spark.gold.contracts import get_gold_schema
from spark.gold.validate import GoldValidationError
from spark.gold.writer import write_local_dimension, local_dimension_path
from spark.gold.manifest import ExistingOutputError
from tests.spark.gold.dimension_fixtures import (
    CoreDimensionTestCase,
    literal_frame,
)
from spark.gold.phase2 import (
    build_dim_employee_assignment,
    build_fact_employee_movement,
    validate_assignment_intervals,
)


class Phase2SmokeTests(CoreDimensionTestCase):
    def setUp(self):
        self.employees = (
            self.sources["employees"]
            .withColumn("department_id", F.col("employee_id"))
            .withColumn("role_id", F.col("employee_id"))
            .withColumn("location_id", F.col("employee_id"))
            .withColumn("manager_id", F.lit(None).cast("long"))
        )
        text, number, day = StringType(), LongType(), DateType()
        self.promotions = literal_frame(
            self.spark,
            [
                ("promotion_id", number),
                ("employee_id", number),
                ("promotion_date", day),
                ("new_role_id", number),
            ],
            [(10, 1, date(2024, 1, 15), 2)],
        )
        self.transfers = literal_frame(
            self.spark,
            [
                ("transfer_id", number),
                ("employee_id", number),
                ("transfer_date", day),
                ("new_department_id", number),
                ("new_location_id", number),
                ("new_manager_id", number),
            ],
            [(20, 1, date(2024, 2, 1), 2, 2, None)],
        )
        self.exits = literal_frame(
            self.spark,
            [
                ("exit_event_id", number),
                ("employee_id", number),
                ("exit_date", day),
                ("exit_type", text),
                ("voluntary_flag", BooleanType()),
                ("regrettable_flag", BooleanType()),
            ],
            [(30, 2, date(2024, 2, 29), "Resignation", True, False)],
        )

    def test_assignment_and_movement(self):
        a = build_dim_employee_assignment(
            self.employees, self.promotions, self.transfers, self.exits, self.build
        )
        self.assertEqual(a.filter(~F.col("is_unknown")).count(), 4)
        self.assertEqual(a.filter(F.col("is_unknown")).count(), 1)
        m = build_fact_employee_movement(
            self.employees, self.promotions, self.transfers, self.exits, a, self.build
        )
        self.assertEqual(m.count(), 5)
        self.assertNotIn("is_unknown", m.columns)

    def test_before_after_states_and_deltas(self):
        assignments = build_dim_employee_assignment(
            self.employees, self.promotions, self.transfers, self.exits, self.build
        )
        movements = build_fact_employee_movement(
            self.employees,
            self.promotions,
            self.transfers,
            self.exits,
            assignments,
            self.build,
        )
        rows = {row.movement_type: row for row in movements.collect()}
        self.assertEqual(rows["HIRE"].before_assignment_key, "0")
        self.assertNotEqual(rows["HIRE"].after_assignment_key, "0")
        self.assertNotEqual(rows["EXIT"].before_assignment_key, "0")
        self.assertEqual(rows["EXIT"].after_assignment_key, "0")
        self.assertEqual(rows["HIRE"].headcount_delta, 1)
        self.assertEqual(rows["EXIT"].headcount_delta, -1)
        self.assertEqual(rows["PROMOTION"].headcount_delta, 0)
        self.assertEqual(rows["TRANSFER"].headcount_delta, 0)

    @unittest.skipIf(os.name == "nt", "Native Windows Parquet safeguard")
    def test_phase2_local_parquet_roundtrip(self):
        a = build_dim_employee_assignment(
            self.employees, self.promotions, self.transfers, self.exits, self.build
        )
        m = build_fact_employee_movement(
            self.employees, self.promotions, self.transfers, self.exits, a, self.build
        )
        with TemporaryDirectory(dir="/workspace") as root:
            for model, frame in (
                ("dim_employee_assignment", a),
                ("fact_employee_movement", m),
            ):
                sources = dict(
                    self.sources,
                    employees=self.employees,
                    promotions=self.promotions,
                    transfers=self.transfers,
                    employee_exits=self.exits,
                )
                actual, report = write_local_dimension(
                    frame, root, model, self.build, sources
                )
                self.assertTrue(report.passed)
                self.assertEqual(actual.schema, get_gold_schema(model))
                self.assertEqual(actual.count(), frame.count())
                self.assertEqual(actual.exceptAll(frame).count(), 0)
                before = {
                    str(p.relative_to(root)): p.read_bytes()
                    for p in Path(root).rglob("*")
                    if p.is_file()
                }
                with self.assertRaises(ExistingOutputError):
                    write_local_dimension(frame, root, model, self.build, sources)
                # The public writer offers no mode override.
                for mode in ("append", "overwrite"):
                    with (
                        self.subTest(model=model, mode=mode),
                        self.assertRaises(TypeError),
                    ):
                        write_local_dimension(
                            frame, root, model, self.build, sources, mode=mode
                        )
                after = {
                    str(p.relative_to(root)): p.read_bytes()
                    for p in Path(root).rglob("*")
                    if p.is_file()
                }
                self.assertEqual(before, after)

    def build_pair(self):
        assignments = build_dim_employee_assignment(
            self.employees, self.promotions, self.transfers, self.exits, self.build
        )
        movements = build_fact_employee_movement(
            self.employees,
            self.promotions,
            self.transfers,
            self.exits,
            assignments,
            self.build,
        )
        return assignments, movements

    def test_no_event_employee(self):
        self.promotions = self.promotions.limit(0)
        self.transfers = self.transfers.limit(0)
        a, m = self.build_pair()
        rows = {r.employee_key: r for r in a.filter(~F.col("is_unknown")).collect()}
        self.assertEqual(len(rows), 2)
        for employee in self.employees.collect():
            row = rows[employee.employee_id]
            self.assertEqual(row.valid_from_date, employee.hire_date)
            self.assertEqual(
                row.valid_to_exclusive,
                (employee.termination_date or self.build.spec.source_cutoff)
                + timedelta(days=1),
            )
        events = {(r.employee_key, r.movement_type) for r in m.collect()}
        self.assertEqual(events, {(1, "HIRE"), (2, "HIRE"), (2, "EXIT")})

    def assert_history(self, expected, event_types):
        a, m = self.build_pair()
        rows = a.filter("employee_key = 1").orderBy("valid_from_date").collect()
        self.assertEqual(
            [
                (
                    r.department_key,
                    r.job_role_key,
                    r.location_key,
                    r.manager_employee_key,
                )
                for r in rows
            ],
            expected,
        )
        self.assertEqual(rows[0].valid_from_date, date(2023, 12, 31))
        self.assertEqual(rows[-1].valid_to_exclusive, date(2024, 3, 2))
        for previous, current in zip(rows, rows[1:]):
            self.assertLess(previous.valid_from_date, previous.valid_to_exclusive)
            self.assertEqual(previous.valid_to_exclusive, current.valid_from_date)
        events = m.filter("employee_key = 1").orderBy("event_date_key").collect()
        self.assertEqual([r.movement_type for r in events], ["HIRE", *event_types])
        for i, event in enumerate(events):
            self.assertEqual(
                event.before_assignment_key,
                "0" if i == 0 else rows[i - 1].assignment_key,
            )
            self.assertEqual(event.after_assignment_key, rows[i].assignment_key)
            for name in (
                "department_key",
                "job_role_key",
                "location_key",
                "manager_employee_key",
            ):
                self.assertEqual(event["after_" + name], rows[i][name])
                self.assertEqual(
                    event["before_" + name], 0 if i == 0 else rows[i - 1][name]
                )
        return rows

    def test_promotion_then_transfer(self):
        self.assert_history(
            [(1, 1, 1, 0), (1, 2, 1, 0), (2, 2, 2, 0)], ["PROMOTION", "TRANSFER"]
        )

    def test_transfer_then_promotion(self):
        self.transfers = self.transfers.withColumn(
            "transfer_date", F.lit(date(2024, 1, 10))
        )
        self.assert_history(
            [(1, 1, 1, 0), (2, 1, 2, 0), (2, 2, 2, 0)], ["TRANSFER", "PROMOTION"]
        )

    def test_multiple_sequential_changes(self):
        later = (
            self.promotions.withColumn("promotion_id", F.lit(11).cast("long"))
            .withColumn("promotion_date", F.lit(date(2024, 2, 15)))
            .withColumn("new_role_id", F.lit(1).cast("long"))
        )
        self.promotions = self.promotions.unionByName(later)
        self.assert_history(
            [(1, 1, 1, 0), (1, 2, 1, 0), (2, 2, 2, 0), (2, 1, 2, 0)],
            ["PROMOTION", "TRANSFER", "PROMOTION"],
        )

    def test_promotion_carries_organisation_and_manager(self):
        self.employees = self.employees.withColumn("manager_id", F.lit(2).cast("long"))
        self.transfers = self.transfers.limit(0)
        self.assert_history([(1, 1, 1, 2), (1, 2, 1, 2)], ["PROMOTION"])

    def test_transfer_carries_role_and_null_manager_means_no_manager(self):
        # Silver transfer department/location are mandatory. Null new manager
        # means no manager, not unchanged; transfer does not expose a role field.
        self.employees = self.employees.withColumn("manager_id", F.lit(2).cast("long"))
        self.promotions = self.promotions.limit(0)
        self.assert_history([(1, 1, 1, 2), (2, 1, 2, 0)], ["TRANSFER"])

    def test_null_manager(self):
        a, _ = self.build_pair()
        self.assertEqual({r.manager_employee_key for r in a.collect()}, {0})

    def test_valid_manager(self):
        self.employees = self.employees.withColumn("manager_id", F.lit(2).cast("long"))
        self.transfers = self.transfers.limit(0)
        a, _ = self.build_pair()
        self.assertEqual(
            {r.manager_employee_key for r in a.filter("employee_key = 1").collect()},
            {2},
        )

    def test_unresolved_manager(self):
        self.employees = self.employees.withColumn(
            "manager_id", F.lit(999).cast("long")
        )
        a, m = self.build_pair()
        self.assertEqual({r.manager_employee_key for r in a.collect()}, {0})
        self.assertEqual({r.employee_key for r in a.collect()}, {0, 1, 2})
        self.assertEqual({r.after_manager_employee_key for r in m.collect()}, {0})

    def test_schema_nullability_and_governance(self):
        restricted = {
            "name",
            "first_name",
            "last_name",
            "email",
            "date_of_birth",
            "dob",
            "gender",
            "salary",
            "free_text",
        }
        for model, output in zip(
            ("dim_employee_assignment", "fact_employee_movement"), self.build_pair()
        ):
            with self.subTest(model=model):
                self.assertEqual(output.schema, get_gold_schema(model))
                self.assertFalse(restricted.intersection(output.columns))
                for field in get_gold_schema(model):
                    if not field.nullable:
                        self.assertEqual(
                            output.filter(F.col(field.name).isNull()).count(), 0
                        )

    def test_compatible_same_day(self):
        self.transfers = self.transfers.withColumn(
            "transfer_date", F.lit(date(2024, 1, 15))
        )
        a, m = self.build_pair()
        rows = a.filter("employee_key = 1").orderBy("valid_from_date").collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].valid_to_exclusive, rows[1].valid_from_date)
        self.assertEqual(
            (rows[1].department_key, rows[1].job_role_key, rows[1].location_key),
            (2, 2, 2),
        )
        events = m.filter("movement_type IN ('PROMOTION', 'TRANSFER')").collect()
        self.assertEqual(len(events), 2)
        for event in events:
            self.assertEqual(event.before_assignment_key, rows[0].assignment_key)
            self.assertEqual(event.after_assignment_key, rows[1].assignment_key)

    def test_conflicting_same_day(self):
        conflict = self.transfers.withColumn(
            "transfer_id", F.lit(21).cast("long")
        ).withColumn("new_department_id", F.lit(1).cast("long"))
        self.transfers = self.transfers.unionByName(conflict)
        with self.assertRaisesRegex(
            GoldValidationError, "same_day_conflict_department_key"
        ):
            self.build_pair()

    def assert_duplicate_event(self, attribute, check):
        assignments, _ = self.build_pair()
        original = getattr(self, attribute)
        setattr(self, attribute, original.unionByName(original))
        with self.assertRaisesRegex(GoldValidationError, check):
            self.build_pair()
        with self.assertRaisesRegex(GoldValidationError, check):
            build_fact_employee_movement(
                self.employees,
                self.promotions,
                self.transfers,
                self.exits,
                assignments,
                self.build,
            )

    def test_duplicate_promotion(self):
        self.assert_duplicate_event("promotions", "promotion_id_unique")

    def test_duplicate_transfer(self):
        self.assert_duplicate_event("transfers", "transfer_id_unique")

    def test_duplicate_exit(self):
        self.assert_duplicate_event("exits", "exit_event_id_unique")

    def test_duplicate_movement(self):
        a, _ = self.build_pair()
        with self.assertRaisesRegex(GoldValidationError, "movement_key_unique"):
            build_fact_employee_movement(
                self.employees.unionByName(self.employees),
                self.promotions,
                self.transfers,
                self.exits,
                a,
                self.build,
            )

    def interval_frame(self, rows):
        return literal_frame(
            self.spark,
            [
                ("assignment_key", StringType()),
                ("employee_key", LongType()),
                ("valid_from_date", DateType()),
                ("valid_to_exclusive", DateType()),
                ("is_unknown", BooleanType()),
            ],
            rows,
        )

    def assert_invalid_intervals(self, rows, check):
        with self.assertRaisesRegex(GoldValidationError, check):
            validate_assignment_intervals(
                self.interval_frame(rows), self.employees, self.build
            )

    def test_zero_length_interval(self):
        self.assert_invalid_intervals(
            [("a", 1, date(2024, 1, 1), date(2024, 1, 1), False)],
            "assignment_positive_intervals",
        )

    def test_negative_interval(self):
        self.assert_invalid_intervals(
            [("a", 1, date(2024, 1, 2), date(2024, 1, 1), False)],
            "assignment_positive_intervals",
        )

    def test_overlap(self):
        self.assert_invalid_intervals(
            [
                ("a", 1, date(2024, 1, 1), date(2024, 2, 1), False),
                ("b", 1, date(2024, 1, 15), date(2024, 3, 1), False),
            ],
            "assignment_no_overlap",
        )

    def test_assignment_before_hire(self):
        self.assert_invalid_intervals(
            [("a", 1, date(2023, 12, 30), date(2024, 1, 1), False)],
            "assignment_employment_bounds",
        )

    def test_assignment_beyond_termination(self):
        self.assert_invalid_intervals(
            [("a", 2, date(2024, 2, 1), date(2024, 3, 2), False)],
            "assignment_employment_bounds",
        )

    def test_assignment_beyond_cutoff(self):
        self.assert_invalid_intervals(
            [("a", 1, date(2024, 2, 1), date(2024, 3, 3), False)],
            "assignment_employment_bounds",
        )

    def test_duplicate_assignment_grain(self):
        self.assert_invalid_intervals(
            [
                ("a", 1, date(2024, 1, 1), date(2024, 2, 1), False),
                ("b", 1, date(2024, 1, 1), date(2024, 2, 1), False),
            ],
            "assignment_grain_unique",
        )

    def test_duplicate_assignment_key(self):
        self.assert_invalid_intervals(
            [
                ("a", 1, date(2024, 1, 1), date(2024, 2, 1), False),
                ("a", 1, date(2024, 2, 1), date(2024, 3, 1), False),
            ],
            "assignment_key_unique",
        )

    def test_rebuild_reorder_repartition_identity_and_hashes(self):
        def identity(pair):
            return [
                sorted(
                    tuple(row) for row in frame.select(key, "_record_hash").collect()
                )
                for frame, key in zip(pair, ("assignment_key", "movement_key"))
            ]

        expected = identity(self.build_pair())
        self.assertEqual(identity(self.build_pair()), expected)
        for name in ("employees", "promotions", "transfers", "exits"):
            frame = getattr(self, name)
            setattr(
                self, name, frame.orderBy(F.col(frame.columns[0]).desc()).repartition(2)
            )
        self.assertEqual(identity(self.build_pair()), expected)

    def test_unknown_dimension_attributes(self):
        # Legitimately unknown baseline values use zero and an unknown basis;
        # a positive reference to a missing required parent must instead fail.
        for source, key, basis in (
            ("department_id", "department_key", "department_history_basis"),
            ("role_id", "job_role_key", "role_history_basis"),
            ("location_id", "location_key", "location_history_basis"),
        ):
            with self.subTest(attribute=source):
                original = self.employees
                self.employees = original.withColumn(source, F.lit(None).cast("long"))
                a, m = self.build_pair()
                baseline = (
                    a.filter("employee_key = 1").orderBy("valid_from_date").first()
                )
                self.assertEqual(baseline[key], 0)
                self.assertEqual(baseline[basis], "unknown")
                hire = m.filter("employee_key = 1 AND movement_type = 'HIRE'").first()
                self.assertEqual(hire["after_" + key], 0)
                self.employees = original

    def test_unresolved_required_dimension_references_fail_before_write(self):
        for dataset, gate in (
            ("departments", "department_key_foreign_key"),
            ("locations", "location_key_foreign_key"),
            ("job_roles", "job_role_key_foreign_key"),
        ):
            with self.subTest(dataset=dataset):
                a, _ = self.build_pair()
                sources = dict(
                    self.sources,
                    employees=self.employees,
                    promotions=self.promotions,
                    transfers=self.transfers,
                    employee_exits=self.exits,
                )
                sources[dataset] = sources[dataset].limit(0)
                with TemporaryDirectory() as root:
                    with self.assertRaisesRegex(GoldValidationError, gate):
                        write_local_dimension(
                            a, root, "dim_employee_assignment", self.build, sources
                        )
                    self.assertFalse(
                        local_dimension_path(
                            root, "dim_employee_assignment", self.build
                        ).exists()
                    )

    def test_fixture_reconciliation(self):
        a, m = self.build_pair()
        self.assertEqual(self.employees.count(), 2)
        self.assertEqual(a.filter(~F.col("is_unknown")).count(), 4)
        self.assertEqual(a.filter("is_unknown").count(), 1)
        self.assertEqual(a.count(), 5)
        self.assertEqual(
            {
                r.movement_type: r["count"]
                for r in m.groupBy("movement_type").count().collect()
            },
            {"HIRE": 2, "EXIT": 1, "PROMOTION": 1, "TRANSFER": 1},
        )
        self.assertEqual(m.count(), 5)

    def test_future_termination_does_not_extend_source_cutoff(self):
        self.employees = self.employees.withColumn(
            "termination_date", F.lit(date(2024, 4, 1))
        )
        with self.assertRaisesRegex(
            GoldValidationError, "assignment_employment_bounds"
        ):
            validate_assignment_intervals(
                self.interval_frame(
                    [("a", 1, date(2024, 2, 1), date(2024, 3, 3), False)]
                ),
                self.employees,
                self.build,
            )
        a, _ = self.build_pair()
        self.assertEqual(
            a.filter("employee_key = 1")
            .orderBy(F.col("valid_from_date").desc())
            .first()
            .valid_to_exclusive,
            self.build.spec.source_cutoff + timedelta(days=1),
        )
