"""Executable payroll contract, source reconciliation and immutable Linux proof."""

from dataclasses import replace
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import unittest

from pyspark.sql import functions as F
from spark.gold.contracts import get_gold_schema, GOLD_MODELS
from spark.gold.dimension_validation import enforce_schema
from spark.gold.hashing import record_hash, spark_record_hash
from spark.gold.manifest import ExistingOutputError
from spark.gold.payroll import (
    PAYROLL_MODEL,
    MONEY_FIELDS,
    build_fact_payroll,
    validate_payroll,
    normalize_payroll_source,
    payroll_key_reconciliation,
    payroll_monetary_reconciliation,
)
from spark.gold.validate import GoldValidationError
from spark.gold.writer import (
    write_local_dimension,
    verify_local_dimension,
    local_dimension_path,
    local_payroll_partition_path,
)
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase
from tests.spark.gold.payroll_fixtures import payroll_fixture, source_amounts


class PayrollTests(CoreDimensionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.build, cls.sources = payroll_fixture(cls.spark, cls.sources, cls.build)
        cls.output = build_fact_payroll(cls.sources, cls.build).cache()
        cls.rows = {r.payroll_id: r for r in cls.output.collect()}

    @classmethod
    def tearDownClass(cls):
        cls.spark.catalog.clearCache()
        super().tearDownClass()

    def source_build(self, source):
        return build_fact_payroll(dict(self.sources, payroll=source), self.build)

    def validate(self, output=None, sources=None):
        return validate_payroll(
            self.output if output is None else output,
            self.build,
            self.sources if sources is None else sources,
        )

    def changed(self, name, value, frame=None, model=PAYROLL_MODEL):
        frame = (self.output if frame is None else frame).withColumn(name, value)
        if not name.startswith("_"):
            frame = frame.withColumn("_record_hash", spark_record_hash(frame, model))
        return enforce_schema(frame, model)

    def test_exact_contract_hash_metadata_and_governance(self):
        self.assertEqual(self.output.schema, get_gold_schema(PAYROLL_MODEL))
        self.assertEqual(len(self.output.columns), 26)
        self.assertTrue(all(not f.nullable for f in self.output.schema))
        for name in (
            "name",
            "first_name",
            "email",
            "date_of_birth",
            "gender",
            "candidate_name",
            "notes",
            "bank_account",
            "tax_identifier",
            "national_identifier",
            "home_address",
        ):
            self.assertNotIn(name, self.output.columns)
        for r in self.rows.values():
            self.assertEqual(r._record_hash, record_hash(PAYROLL_MODEL, r.asDict()))
            self.assertEqual(r._source_batch_id, "batch-a")
            self.assertEqual(r._gold_build_id, self.build.spec.build_id)
            self.assertEqual(
                r._gold_generated_at, self.build.generated_at.replace(tzinfo=None)
            )
            for name in MONEY_FIELDS:
                self.assertIsInstance(r[name], Decimal)
                self.assertEqual(r[name].as_tuple().exponent, -2)

    def test_grain_and_year_boundary(self):
        self.assertEqual(set(self.rows), {101, 102, 103, 104, 105, 201, 301})
        self.assertEqual(self.output.count(), 7)
        self.assertEqual(self.rows[101].reporting_year, 2023)
        self.assertEqual(self.rows[102].reporting_year, 2024)
        self.assertEqual(self.rows[103].pay_period_days, 29)

    def test_incomplete_month_retained_actual_end_assignment(self):
        r = self.rows[105]
        self.assertEqual(
            (r.pay_period_start_key, r.pay_period_end_key, r.payroll_month_key),
            (20240401, 20240415, 20240430),
        )
        self.assertEqual(r.pay_period_days, 15)
        assignment = (
            self.sources["dim_employee_assignment"]
            .filter(F.col("assignment_key") == r.assignment_key)
            .first()
        )
        self.assertEqual(assignment.valid_to_exclusive, date(2024, 4, 16))
        self.assertLess(date(2024, 4, 15), assignment.valid_to_exclusive)
        self.assertGreater(date(2024, 4, 30), assignment.valid_to_exclusive)

    def test_promotion_transfer_manager_and_historical_state(self):
        self.assertEqual(self.rows[102].job_role_key, 1)
        self.assertEqual(self.rows[103].job_role_key, 2)
        self.assertEqual(
            (self.rows[103].department_key, self.rows[103].location_key), (1, 1)
        )
        self.assertEqual(
            (
                self.rows[104].department_key,
                self.rows[104].location_key,
                self.rows[104].manager_employee_key,
            ),
            (2, 2, 2),
        )
        current = self.sources["employees"].filter("employee_id = 1").first()
        self.assertEqual(current.department_id, 2)
        self.assertEqual(self.rows[102].department_key, 1)
        self.assertNotEqual(
            self.rows[102].assignment_key, self.rows[104].assignment_key
        )

    def test_final_pay_included_post_employment_rejected(self):
        self.assertEqual(self.rows[301].pay_period_end_key, 20240415)
        # Earlier termination makes an otherwise within-cutoff payroll period invalid.
        employees = self.sources["employees"].withColumn(
            "termination_date",
            F.when(F.col("employee_id") == 3, F.lit(date(2024, 4, 14))).otherwise(
                F.col("termination_date")
            ),
        )
        # Source gate is also tested directly to isolate employment from parent bounds.
        from spark.gold.payroll import _validate_employment

        normalized = normalize_payroll_source(self.sources["payroll"], self.build)
        with self.assertRaisesRegex(GoldValidationError, "employment_window"):
            _validate_employment(
                normalized, dict(self.sources, employees=employees), self.build
            )

    def test_duplicate_payroll_id_rejected(self):
        source = self.sources["payroll"]
        with self.assertRaisesRegex(GoldValidationError, "id_unique"):
            self.source_build(source.unionByName(source.limit(1)))
        with self.assertRaisesRegex(GoldValidationError, "payroll_unique"):
            self.validate(self.output.unionByName(self.output.limit(1)))

    def test_duplicate_employee_actual_period_with_different_id_rejected(self):
        source = self.sources["payroll"]
        extra = source.limit(1).withColumn("payroll_id", F.lit(999).cast("long"))
        with self.assertRaisesRegex(GoldValidationError, "period_unique"):
            self.source_build(source.unionByName(extra))

    def test_reversed_cross_month_and_post_cutoff_periods_rejected(self):
        source = self.sources["payroll"].filter("payroll_id = 105")
        for start, end in (
            ("2024-04-15", "2024-04-01"),
            ("2024-03-31", "2024-04-01"),
            ("2024-04-01", "2024-04-16"),
        ):
            with (
                self.subTest(start=start, end=end),
                self.assertRaisesRegex(GoldValidationError, "actual_period"),
            ):
                self.source_build(
                    source.withColumn(
                        "pay_period_start", F.lit(start).cast("timestamp")
                    ).withColumn("pay_period_end", F.lit(end).cast("timestamp"))
                )

    def test_timestamp_and_date_inputs_and_nonmidnight_rejection(self):
        source = self.sources["payroll"]
        normalized = normalize_payroll_source(source, self.build)
        dated = source.withColumn(
            "pay_period_start", F.col("pay_period_start").cast("date")
        ).withColumn("pay_period_end", F.col("pay_period_end").cast("date"))
        self.assertEqual(
            normalized.exceptAll(normalize_payroll_source(dated, self.build)).count(), 0
        )
        for value in (None, "2024-04-15 01:00:00"):
            with self.subTest(value=value), self.assertRaises(GoldValidationError):
                normalize_payroll_source(
                    source.withColumn("pay_period_end", F.lit(value).cast("timestamp")),
                    self.build,
                )

    def test_period_before_hire_rejected(self):
        source = self.sources["payroll"].filter("payroll_id = 301")
        source = source.withColumn(
            "pay_period_start", F.lit("2024-01-01").cast("timestamp")
        ).withColumn("pay_period_end", F.lit("2024-01-31").cast("timestamp"))
        with self.assertRaisesRegex(GoldValidationError, "employment_window"):
            self.source_build(source)

    def test_arithmetic_exact_cent_threshold(self):
        source = self.sources["payroll"].limit(1)
        for name in MONEY_FIELDS:
            value = (
                Decimal("1.00")
                if name in ("base_salary", "gross_pay", "net_pay")
                else Decimal("0.00")
            )
            source = source.withColumn(name, F.lit(value).cast("decimal(18,2)"))
        normalize_payroll_source(
            source.withColumn("net_pay", F.lit(Decimal("1.01"))), self.build
        )
        for name, value in (
            ("net_pay", "1.02"),
            ("deductions", "0.02"),
            ("gross_pay", "1.01"),
        ):
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(GoldValidationError, "arithmetic"),
            ):
                normalize_payroll_source(
                    source.withColumn(name, F.lit(Decimal(value))), self.build
                )

    def test_valid_but_wrong_reporting_date_and_logical_nullability(self):
        bad = self.changed(
            "payroll_month_key",
            F.when(F.col("payroll_id") == 105, 20240331).otherwise(
                F.col("payroll_month_key")
            ),
        )
        with self.assertRaisesRegex(GoldValidationError, "source_reconstruction"):
            self.validate(bad)
        nullable = self.output.withColumn(
            "currency", F.when(F.col("payroll_id") > 0, F.col("currency"))
        )
        self.assertTrue(nullable.schema["currency"].nullable)
        with self.assertRaisesRegex(GoldValidationError, "exact_schema"):
            self.validate(nullable)

    def test_missing_source_employee_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "employment_window"):
            self.source_build(
                self.sources["payroll"]
                .filter("payroll_id = 101")
                .withColumn("employee_id", F.lit(999).cast("long"))
            )

    def test_required_employee_parent_rejected(self):
        with self.assertRaisesRegex(GoldValidationError, "employee_key_foreign_key"):
            self.validate(
                sources=dict(
                    self.sources,
                    dim_employee=self.sources["dim_employee"].filter(
                        "employee_key != 1"
                    ),
                )
            )

    def test_missing_and_overlapping_assignment_rejected(self):
        assignment = self.sources["dim_employee_assignment"]
        with self.assertRaisesRegex(GoldValidationError, "required_assignment"):
            build_fact_payroll(
                dict(
                    self.sources,
                    dim_employee_assignment=assignment.filter("employee_key != 1"),
                ),
                self.build,
            )
        duplicate = assignment.filter("employee_key = 1").limit(1)
        duplicate = self.changed(
            "assignment_key", F.lit("f" * 64), duplicate, "dim_employee_assignment"
        )
        with self.assertRaisesRegex(GoldValidationError, "assignment_grain_unique"):
            build_fact_payroll(
                dict(
                    self.sources,
                    dim_employee_assignment=assignment.unionByName(duplicate),
                ),
                self.build,
            )

    def test_broken_assignment_org_manager_and_date_references(self):
        for name, value in (
            ("assignment_key", "absent"),
            ("department_key", 999),
            ("job_role_key", 999),
            ("location_key", 999),
            ("manager_employee_key", 999),
            ("pay_period_end_key", 20240230),
            ("payroll_month_key", 0),
        ):
            with self.subTest(name=name), self.assertRaises(GoldValidationError):
                self.validate(
                    self.changed(
                        name, F.lit(value).cast(self.output.schema[name].dataType)
                    )
                )

    def test_valid_but_wrong_assignment_date_and_org_rejected(self):
        for name, value in (
            ("assignment_key", self.rows[104].assignment_key),
            ("department_key", 2),
            ("pay_period_days", 99),
            ("reporting_year", 2025),
        ):
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(GoldValidationError, "source_reconstruction"),
            ):
                self.validate(
                    self.changed(
                        name,
                        F.when(F.col("payroll_id") == 102, F.lit(value))
                        .otherwise(F.col(name))
                        .cast(self.output.schema[name].dataType),
                    )
                )

    def test_legitimate_unknown_organisation_and_manager_preserved(self):
        a = self.sources["dim_employee_assignment"]
        for name in (
            "department_key",
            "job_role_key",
            "location_key",
            "manager_employee_key",
        ):
            a = a.withColumn(name, F.lit(0).cast("long"))
        for name in (
            "department_history_basis",
            "role_history_basis",
            "location_history_basis",
            "manager_history_basis",
        ):
            a = a.withColumn(name, F.lit("unknown"))
        a = a.withColumn(
            "_record_hash", spark_record_hash(a, "dim_employee_assignment")
        )
        result = build_fact_payroll(
            dict(
                self.sources,
                dim_employee_assignment=enforce_schema(a, "dim_employee_assignment"),
            ),
            self.build,
        )
        self.assertEqual(
            result.filter("department_key != 0 or manager_employee_key != 0").count(), 0
        )
        self.assertEqual(result.count(), 7)

    def test_exact_monetary_preservation_and_no_float_drift(self):
        for pid, base, ot in (
            (101, "100.02", "10.01"),
            (103, "1100.10", "12.34"),
            (105, "600.02", "1.01"),
        ):
            self.assertEqual(
                tuple(self.rows[pid][n] for n in MONEY_FIELDS), source_amounts(base, ot)
            )
        self.assertEqual(self.rows[103].gross_pay, Decimal("1145.44"))
        self.assertEqual(self.rows[103].net_pay, Decimal("870.42"))

    def test_lossy_nonfinite_null_and_overflow_rejected(self):
        source = self.sources["payroll"].limit(1)
        for value in (0.001, 1e-19, float("nan"), float("inf"), None):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(GoldValidationError, "precision"),
            ):
                normalize_payroll_source(
                    source.withColumn("bonus", F.lit(value).cast("double")), self.build
                )
        for value in ("10000000000000000.00", "1.001"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(GoldValidationError, "precision"),
            ):
                normalize_payroll_source(
                    source.withColumn(
                        "base_salary", F.lit(value).cast("decimal(21,3)")
                    ),
                    self.build,
                )

    def test_decimal_maximum_boundary_and_zero_domain(self):
        source = self.sources["payroll"].limit(1)
        for name in MONEY_FIELDS:
            source = source.withColumn(
                name, F.lit(Decimal("0.00")).cast("decimal(18,2)")
            )
        zeros = self.source_build(source)
        self.assertEqual(zeros.first().gross_pay, Decimal("0.00"))
        for name in ("base_salary", "gross_pay", "net_pay"):
            source = source.withColumn(name, F.lit(Decimal("9999999999999999.99")))
        normalized = normalize_payroll_source(source, self.build)
        self.assertEqual(normalized.first().net_pay, Decimal("9999999999999999.99"))

    def test_negative_components_rejected(self):
        for name in MONEY_FIELDS:
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(GoldValidationError, "precision"),
            ):
                normalize_payroll_source(
                    self.sources["payroll"]
                    .limit(1)
                    .withColumn(name, F.lit(Decimal("-0.01"))),
                    self.build,
                )

    def test_rounding_tolerance_is_source_evidenced(self):
        maxima = [Decimal(0)] * 3
        for cents in range(1, 10001):
            b, o, bonus, ded, pension, tax, gross, net = source_amounts(
                str(Decimal(cents) / 100), "0.01"
            )
            for i, residual in enumerate(
                (
                    abs(gross - b - o - bonus),
                    abs(ded - pension - tax),
                    abs(net - gross + ded),
                )
            ):
                maxima[i] = max(maxima[i], residual)
        self.assertEqual(maxima, [Decimal(0), Decimal(".01"), Decimal(".01")])
        self.assertEqual(
            self.rows[101].deductions
            - self.rows[101].pension_contribution
            - self.rows[101].tax_amount,
            Decimal(".01"),
        )
        self.assertEqual(
            self.rows[101].net_pay
            - self.rows[101].gross_pay
            + self.rows[101].deductions,
            Decimal(".01"),
        )

    def test_invalid_arithmetic_rejected_without_double_counting(self):
        source = self.sources["payroll"]
        for name in ("gross_pay", "deductions", "net_pay"):
            bad = source.withColumn(
                name,
                (F.col(name).cast("decimal(18,2)") + F.lit(Decimal("1.00"))).cast(
                    "decimal(18,2)"
                ),
            )
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(GoldValidationError, "arithmetic"),
            ):
                normalize_payroll_source(bad, self.build)
        # Gross - total deductions - pension - tax would wrongly reject this.
        self.assertNotEqual(
            self.rows[103].net_pay,
            self.rows[103].gross_pay
            - self.rows[103].deductions
            - self.rows[103].pension_contribution
            - self.rows[103].tax_amount,
        )

    def test_currency_status_preserved_with_open_source_domain(self):
        self.assertEqual({r.currency for r in self.rows.values()}, {"GBP", "EUR"})
        self.assertEqual({r.payroll_status for r in self.rows.values()}, {"Processed"})
        source = (
            self.sources["payroll"]
            .limit(1)
            .withColumn("currency", F.lit("XYZ"))
            .withColumn("payroll_status", F.lit("Source-defined"))
        )
        result = self.source_build(source).first()
        self.assertEqual(
            (result.currency, result.payroll_status), ("XYZ", "Source-defined")
        )

    def test_invalid_currency_status_and_batch_rejected(self):
        for name, value in (
            ("currency", None),
            ("currency", ""),
            ("currency", " GBP"),
            ("currency", "X" * 11),
            ("payroll_status", ""),
            ("payroll_status", None),
            ("payroll_status", "X" * 31),
            ("_batch_id", "wrong"),
        ):
            with (
                self.subTest(name=name, value=value),
                self.assertRaises(GoldValidationError),
            ):
                normalize_payroll_source(
                    self.sources["payroll"].withColumn(
                        name, F.lit(value).cast("string")
                    ),
                    self.build,
                )

    def test_source_key_and_period_currency_reconciliation(self):
        metrics = payroll_key_reconciliation(self.output, self.sources["payroll"])
        self.assertEqual(
            metrics,
            dict(
                source_rows=7,
                gold_rows=7,
                source_business_keys=7,
                gold_business_keys=7,
                missing_keys=0,
                unexpected_keys=0,
                source_duplicate_rows=0,
                gold_duplicate_rows=0,
            ),
        )
        normalized = normalize_payroll_source(self.sources["payroll"], self.build)
        rows = payroll_monetary_reconciliation(self.output, normalized).collect()
        self.assertEqual(len(rows), 6)
        self.assertEqual(sum(r.source_rows for r in rows), 7)
        for r in rows:
            self.assertEqual(r.source_rows, r.gold_rows)
            for n in MONEY_FIELDS:
                self.assertEqual(r[n + "_difference"], Decimal(0))
                self.assertEqual(r["source_" + n], r["gold_" + n])
            print("PAYROLL_RECONCILIATION", r.asDict())
        april = next(r for r in rows if r.pay_period_start_key == 20240401)
        self.assertEqual(
            (april.source_rows, april.source_gross_pay, april.source_net_pay),
            (2, Decimal("1340.05"), Decimal("1015.05")),
        )

    def test_missing_extra_keys_and_balanced_component_corruption(self):
        with self.assertRaisesRegex(GoldValidationError, "source_key_reconciliation"):
            self.validate(self.output.filter("payroll_id != 101"))
        with self.assertRaisesRegex(GoldValidationError, "source_key_reconciliation"):
            self.validate(
                self.changed(
                    "payroll_id",
                    F.when(F.col("payroll_id") == 101, F.lit(999))
                    .otherwise(F.col("payroll_id"))
                    .cast("long"),
                )
            )
        # April has two GBP rows: equal/opposite changes preserve aggregates and
        # within-row arithmetic, so only per-record reconstruction detects them.
        altered = self.output
        delta = (
            F.when(F.col("payroll_id") == 105, F.lit(Decimal(".01")))
            .when(F.col("payroll_id") == 301, F.lit(Decimal("-.01")))
            .otherwise(F.lit(Decimal("0")))
        )
        for n in ("base_salary", "gross_pay", "net_pay"):
            altered = altered.withColumn(n, (F.col(n) + delta).cast("decimal(18,2)"))
        altered = altered.withColumn(
            "_record_hash", spark_record_hash(altered, PAYROLL_MODEL)
        )
        with self.assertRaisesRegex(GoldValidationError, "source_reconstruction"):
            self.validate(enforce_schema(altered, PAYROLL_MODEL))

    def test_deterministic_reorder_repartition(self):
        shuffled = {
            n: f.orderBy(F.desc(f.columns[0])).repartition(2)
            for n, f in self.sources.items()
        }
        rebuilt = build_fact_payroll(shuffled, self.build)
        self.assertEqual(self.output.exceptAll(rebuilt).count(), 0)
        self.assertEqual(rebuilt.exceptAll(self.output).count(), 0)
        self.assertEqual(
            self.build.spec.build_id,
            replace(
                self.build, generated_at=self.build.generated_at.replace(year=2025)
            ).spec.build_id,
        )

    def test_schema_and_metadata_corruption_rejected(self):
        for name, value in (
            ("_record_hash", "wrong"),
            ("_source_batch_id", "wrong"),
            ("_gold_build_id", "f" * 64),
            ("_gold_generated_at", "2025-01-01"),
        ):
            with self.subTest(name=name), self.assertRaises(GoldValidationError):
                self.validate(
                    self.changed(
                        name, F.lit(value).cast(self.output.schema[name].dataType)
                    )
                )
        for bad in (
            self.output.select(*reversed(self.output.columns)),
            self.output.withColumn("notes", F.lit("restricted")),
            self.output.withColumn("base_salary", F.col("base_salary").cast("double")),
        ):
            with self.subTest(), self.assertRaises(GoldValidationError):
                self.validate(bad)

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_local_physical_roundtrip_and_immutable_output(self):
        with TemporaryDirectory(dir="/workspace") as root:
            actual, report = write_local_dimension(
                self.output, root, PAYROLL_MODEL, self.build, self.sources
            )
            self.assertTrue(report.passed)
            self.assertEqual(actual.schema, get_gold_schema(PAYROLL_MODEL))
            self.assertEqual(actual.count(), 7)
            self.assertEqual(actual.exceptAll(self.output).count(), 0)
            for year in (2023, 2024):
                leaf = local_payroll_partition_path(root, self.build, year)
                self.assertTrue(leaf.is_dir())
                self.assertEqual(leaf.parent.name, f"reporting_year={year}")
            before = {
                str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()
            }
            with self.assertRaises(ExistingOutputError):
                write_local_dimension(
                    self.output, root, PAYROLL_MODEL, self.build, self.sources
                )
            for mode in ("append", "overwrite"):
                with self.assertRaises(TypeError):
                    write_local_dimension(
                        self.output,
                        root,
                        PAYROLL_MODEL,
                        self.build,
                        self.sources,
                        mode=mode,
                    )
            verify_local_dimension(
                self.output, root, PAYROLL_MODEL, self.build, self.sources
            )
            self.assertEqual(
                before,
                {str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()},
            )

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_invalid_prewrite_and_corrupt_readback(self):
        bad = self.changed("_record_hash", F.lit("corrupt"))
        with TemporaryDirectory(dir="/workspace") as root:
            with self.assertRaises(GoldValidationError):
                write_local_dimension(
                    bad, root, PAYROLL_MODEL, self.build, self.sources
                )
            self.assertFalse(
                local_dimension_path(root, PAYROLL_MODEL, self.build).exists()
            )
            claim = local_dimension_path(root, PAYROLL_MODEL, self.build)
            claim.mkdir(parents=True)
            for year in (2023, 2024):
                bad.filter(F.col("reporting_year") == year).write.mode(
                    "errorifexists"
                ).parquet(str(local_payroll_partition_path(root, self.build, year)))
            before = {
                str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()
            }
            with self.assertRaises(GoldValidationError):
                verify_local_dimension(
                    self.output, root, PAYROLL_MODEL, self.build, self.sources
                )
            self.assertEqual(
                before,
                {str(p): p.read_bytes() for p in Path(root).rglob("*") if p.is_file()},
            )

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_physical_wrong_partition_and_missing_inventory(self):
        with TemporaryDirectory(dir="/workspace") as root:
            local_dimension_path(root, PAYROLL_MODEL, self.build).mkdir(parents=True)
            for year in (2023, 2024):
                self.output.filter(F.col("reporting_year") != year).write.mode(
                    "errorifexists"
                ).parquet(str(local_payroll_partition_path(root, self.build, year)))
            with self.assertRaisesRegex(GoldValidationError, "physical_partition_year"):
                verify_local_dimension(
                    self.output, root, PAYROLL_MODEL, self.build, self.sources
                )
        with TemporaryDirectory(dir="/workspace") as root:
            local_dimension_path(root, PAYROLL_MODEL, self.build).mkdir(parents=True)
            with self.assertRaises(ExistingOutputError):
                verify_local_dimension(
                    self.output, root, PAYROLL_MODEL, self.build, self.sources
                )

    @unittest.skipIf(os.name == "nt", "Physical Parquet requires Linux Docker Spark")
    def test_physical_schema_corruption_in_later_partition(self):
        with TemporaryDirectory(dir="/workspace") as root:
            local_dimension_path(root, PAYROLL_MODEL, self.build).mkdir(parents=True)
            self.output.filter("reporting_year = 2023").write.mode(
                "errorifexists"
            ).parquet(str(local_payroll_partition_path(root, self.build, 2023)))
            # A later part must not be silently reordered by unionByName.
            self.output.filter("reporting_year = 2024").select(
                *reversed(self.output.columns)
            ).write.mode("errorifexists").parquet(
                str(local_payroll_partition_path(root, self.build, 2024))
            )
            with self.assertRaisesRegex(ValueError, "Physical Gold schema"):
                verify_local_dimension(
                    self.output, root, PAYROLL_MODEL, self.build, self.sources
                )

    def test_empty_source_retains_no_unknown_fact_rows(self):
        result = self.source_build(self.sources["payroll"].limit(0))
        self.assertEqual(result.count(), 0)
        self.assertEqual(result.schema, get_gold_schema(PAYROLL_MODEL))
