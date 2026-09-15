"""Compare executable contracts directly with the approved field inventory."""

from pathlib import Path
import re
from unittest import TestCase
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from config.datasets import SUPPORTED_DATASETS
from spark.gold.contracts import (
    GOLD_MODELS,
    COMMON_METADATA,
    get_gold_schema,
    unknown_business_row,
)
from spark.gold.validate import validate_member_row


class RegistryTests(TestCase):
    def test_exact_models_types_keys_grains_and_partitions(self):
        expected = {
            "dim_employee": ("dimension", ("employee_key",), ("employee_key",), ()),
            "dim_department": (
                "dimension",
                ("department_key",),
                ("department_key",),
                (),
            ),
            "dim_location": ("dimension", ("location_key",), ("location_key",), ()),
            "dim_job_role": ("dimension", ("job_role_key",), ("job_role_key",), ()),
            "dim_date": ("dimension", ("date_key",), ("calendar_date",), ()),
            "dim_employee_assignment": (
                "interval_dimension",
                ("assignment_key",),
                ("employee_key", "valid_from_date"),
                (),
            ),
            "fact_employee_movement": (
                "event_fact",
                ("movement_key",),
                ("movement_type", "source_record_id"),
                (),
            ),
            "fact_workforce_monthly": (
                "periodic_snapshot",
                ("employee_key", "snapshot_month_key"),
                ("employee_key", "snapshot_month_key"),
                ("reporting_year",),
            ),
            "fact_payroll": (
                "transaction_fact",
                ("payroll_id",),
                ("employee_key", "pay_period_start_key", "pay_period_end_key"),
                ("reporting_year",),
            ),
            "fact_attendance": (
                "transaction_fact",
                ("attendance_id",),
                ("employee_key", "work_date_key"),
                ("reporting_year",),
            ),
        }
        self.assertEqual(set(GOLD_MODELS), set(expected))
        for name, values in expected.items():
            with self.subTest(model=name):
                model = GOLD_MODELS[name]
                self.assertEqual(
                    (
                        model.model_type.value,
                        model.primary_key,
                        model.grain,
                        model.partition_fields,
                    ),
                    values,
                )
                self.assertTrue(model.mvp)
                self.assertEqual(model.unknown_member, name.startswith("dim_"))
                self.assertEqual(
                    model.additional_unique,
                    () if model.primary_key == model.grain else (model.grain,),
                )

    def test_exact_source_dependencies(self):
        assignment = {
            "employees",
            "transfers",
            "promotions",
            "recruitment",
            "employee_exits",
        }
        expected = {
            "dim_employee": {"employees"},
            "dim_department": {"departments", "business_units"},
            "dim_location": {"locations"},
            "dim_job_role": {"job_roles"},
            "dim_date": set(),
            "dim_employee_assignment": assignment,
            "fact_employee_movement": assignment,
            "fact_workforce_monthly": assignment,
            "fact_payroll": assignment | {"payroll"},
            "fact_attendance": assignment | {"attendance"},
        }
        for name, sources in expected.items():
            self.assertEqual(set(GOLD_MODELS[name].silver_sources), sources)
            self.assertTrue(sources <= set(SUPPORTED_DATASETS))

    def test_no_post_mvp_schema(self):
        for model in (
            "fact_leave_request",
            "fact_recruitment",
            "fact_training",
            "fact_performance_review",
            "fact_employee_survey",
            "fact_manager_feedback",
            "fact_exit_interview",
            "unknown",
        ):
            with self.assertRaises(KeyError):
                get_gold_schema(model)

    def test_schema_is_fresh_and_registry_readonly(self):
        schema = get_gold_schema("dim_employee")
        schema.fields[0].nullable = True
        self.assertFalse(get_gold_schema("dim_employee").fields[0].nullable)
        with self.assertRaises(TypeError):
            GOLD_MODELS["new"] = None

    def test_every_field_matches_approved_markdown(self):
        document = (
            (Path(__file__).resolve().parents[3] / "docs/architecture/gold-layer.md")
            .resolve()
            .read_text(encoding="utf-8")
        )
        types = {
            "STRING": StringType(),
            "BIGINT": LongType(),
            "INT": IntegerType(),
            "BOOLEAN": BooleanType(),
            "DATE": DateType(),
            "TIMESTAMP": TimestampType(),
            "DECIMAL(18,2)": DecimalType(18, 2),
            "DECIMAL(18,4)": DecimalType(18, 4),
        }
        found = set()
        for section in document.split("### `")[1:]:
            name = section.split("`")[0]
            rows = []
            for line in section.splitlines():
                parts = [p.strip() for p in line.split("|")]
                if len(parts) == 7 and re.fullmatch(r"`[a-z_]+`", parts[1]):
                    rows.append(
                        (
                            parts[1].strip("`"),
                            types[parts[2]],
                            parts[4].startswith("Yes"),
                        )
                    )
            expected = rows + [
                ("_source_batch_id", StringType(), False),
                ("_gold_build_id", StringType(), False),
                ("_gold_generated_at", TimestampType(), False),
                ("_record_hash", StringType(), False),
            ]
            with self.subTest(model=name):
                actual = [
                    (f.name, f.dataType, f.nullable) for f in get_gold_schema(name)
                ]
                self.assertEqual(actual, expected)
                self.assertEqual(len({f[0] for f in actual}), len(actual))
                self.assertEqual(
                    [f.name for f in COMMON_METADATA], [f[0] for f in actual[-4:]]
                )
            found.add(name)
        self.assertEqual(found, set(GOLD_MODELS))


class UnknownMemberTests(TestCase):
    def test_all_six_unknown_members(self):
        for name, contract in GOLD_MODELS.items():
            if not contract.unknown_member:
                with self.assertRaises(ValueError):
                    unknown_business_row(name)
                continue
            with self.subTest(model=name):
                row = unknown_business_row(name)
                self.assertIs(row["is_unknown"], True)
                self.assertEqual(
                    row[contract.primary_key[0]],
                    "0" if name == "dim_employee_assignment" else 0,
                )
                validate_member_row(name, row)
                if name == "dim_date":
                    self.assertTrue(
                        all(
                            v is None
                            for k, v in row.items()
                            if k not in ("date_key", "is_unknown")
                        )
                    )
                if name == "dim_employee_assignment":
                    self.assertTrue(
                        all(
                            v == "unknown"
                            for k, v in row.items()
                            if k.endswith("_history_basis")
                        )
                    )
                    self.assertTrue(
                        all(
                            v == 0
                            for k, v in row.items()
                            if k.endswith("_key") and k != "assignment_key"
                        )
                    )

    def test_unknown_labels_and_employee_dates(self):
        self.assertEqual(unknown_business_row("dim_employee")["hire_date_key"], 0)
        self.assertEqual(
            unknown_business_row("dim_employee")["termination_date_key"], 0
        )
        for model, fields in {
            "dim_department": ("department_name", "business_unit_name"),
            "dim_location": ("office_name", "city", "country"),
            "dim_job_role": ("role_name", "grade"),
        }.items():
            for field in fields:
                self.assertEqual(unknown_business_row(model)[field], "Unknown")

    def test_invalid_sentinels_and_real_member_nulls_fail(self):
        for model in (
            "dim_employee",
            "dim_department",
            "dim_location",
            "dim_job_role",
            "dim_date",
        ):
            row = unknown_business_row(model)
            row["is_unknown"] = False
            with self.assertRaises(ValueError):
                validate_member_row(model, row)
        row = unknown_business_row("dim_job_role")
        row["role_name"] = "wrong"
        with self.assertRaises(ValueError):
            validate_member_row("dim_job_role", row)


class PortabilityTests(TestCase):
    def test_production_modules_parse_as_python_311(self):
        import ast

        root = Path(__file__).resolve().parents[3]
        for path in (root / "spark/gold").glob("*.py"):
            with self.subTest(module=path.name):
                ast.parse(path.read_text(encoding="utf-8-sig"), feature_version=(3, 11))
