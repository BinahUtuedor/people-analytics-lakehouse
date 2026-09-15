"""Validation interfaces, strict schema checking and deterministic reports."""

from dataclasses import replace
from unittest import TestCase
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark.gold.contracts import get_gold_schema
from spark.gold.validate import (
    CheckKind,
    Severity,
    ValidationContext,
    ValidationRule,
    ValidationResult,
    ValidationReport,
    GoldValidationError,
    validate_schema,
)
from tests.spark.gold.test_manifest import STAMP, spec


class ValidationTests(TestCase):
    def setUp(self):
        self.context = ValidationContext(
            "fact_payroll", "batch-a", spec().build_id, STAMP
        )

    def test_success_and_failure_aggregation(self):
        good = ValidationResult("a", True, 0, Severity.CRITICAL, self.context)
        bad = ValidationResult(
            "b", False, 2, Severity.ERROR, self.context, {"missing": "2"}
        )
        ValidationReport((good,)).raise_for_failure()
        report = ValidationReport((good, bad))
        self.assertFalse(report.passed)
        with self.assertRaises(GoldValidationError) as caught:
            report.raise_for_failure()
        self.assertIs(caught.exception.report, report)
        self.assertEqual(report.to_json(), ValidationReport((bad, good)).to_json())

    def test_severity_and_invalid_results(self):
        for severity in Severity:
            result = ValidationResult("check", False, 1, severity, self.context)
            self.assertEqual(result.to_dict()["severity"], severity.value)
            self.assertFalse(ValidationReport((result,)).passed)
        for args in (
            (True, 1, Severity.ERROR),
            (False, -1, Severity.ERROR),
            (False, 0, "ERROR"),
        ):
            with self.assertRaises(ValueError):
                ValidationResult("check", *args, self.context)

    def test_report_cannot_be_empty_duplicated_or_cross_build(self):
        good = ValidationResult("a", True, 0, Severity.CRITICAL, self.context)
        for results in (
            (),
            (good, good),
            (
                good,
                replace(
                    good,
                    check_name="b",
                    context=replace(self.context, gold_build_id="f" * 64),
                ),
            ),
        ):
            with self.assertRaises(ValueError):
                ValidationReport(results)

    def test_exact_schema(self):
        schema = get_gold_schema("fact_payroll")
        self.assertTrue(validate_schema(schema, self.context).passed)
        variants = [
            StructType(schema.fields[:-1]),
            StructType(list(reversed(schema.fields))),
            StructType(schema.fields + [StructField("extra", StringType())]),
        ]
        for change in ("name", "type", "nullable"):
            fields = list(schema.fields)
            fields[14] = StructField(
                "wrong" if change == "name" else "base_salary",
                DoubleType() if change == "type" else fields[14].dataType,
                change == "nullable",
            )
            variants.append(StructType(fields))
        for actual in variants:
            self.assertFalse(validate_schema(actual, self.context).passed)

    def test_all_reusable_check_concepts_have_typed_interfaces(self):
        for kind in CheckKind:
            if kind in (CheckKind.FOREIGN_KEY, CheckKind.SOURCE_COVERAGE):
                rule = ValidationRule(
                    kind.value,
                    kind,
                    ("employee_key",),
                    "dim_employee",
                    ("employee_key",),
                )
            else:
                rule = ValidationRule(kind.value, kind, ("employee_key",))
            self.assertEqual(rule.kind, kind)
        with self.assertRaises(ValueError):
            ValidationRule("fk", CheckKind.FOREIGN_KEY, ("employee_key",))
        with self.assertRaises(ValueError):
            ValidationRule("grain", CheckKind.GRAIN, ())
