"""Reference serialization and deterministic key boundary tests."""

from datetime import date, datetime
from decimal import Decimal, localcontext
from hashlib import sha256
from unittest import TestCase
from spark.gold.contracts import FieldContract, unknown_business_row
from spark.gold.hashing import (
    frame,
    canonical_record,
    canonical_value,
    record_hash,
    canonical_json,
)
from spark.gold.keys import date_key, assignment_key, movement_key, positive_source_id


class KeyTests(TestCase):
    def test_date_boundaries(self):
        for value, expected in (
            (None, 0),
            (date(2024, 2, 29), 20240229),
            (date(2023, 12, 31), 20231231),
            (date(2024, 1, 1), 20240101),
            (date(2024, 3, 1), 20240301),
            (date.min, 10101),
            (date.max, 99991231),
        ):
            self.assertEqual(date_key(value), expected)

    def test_invalid_dates(self):
        for value in (
            "2024-02-30",
            "2024-02-29",
            datetime(2024, 1, 1),
            20240101,
            True,
            0,
        ):
            with self.assertRaises(ValueError):
                date_key(value)

    def test_positive_source_ids_cannot_collide(self):
        for value in (1, 2, 2**63 - 1):
            self.assertEqual(positive_source_id(value), value)
            self.assertNotEqual(value, 0)
        for value in (0, -1, True, 1.0, "1", None, 2**63):
            with self.assertRaises(ValueError):
                positive_source_id(value)

    def test_assignment_determinism_and_boundaries(self):
        args = ("hr", 12, date(2024, 1, 1))
        key = assignment_key(*args)
        self.assertEqual(key, assignment_key(*args))
        self.assertRegex(key, r"^[a-f0-9]{64}$")
        self.assertNotEqual(key, "0")
        self.assertNotEqual(key, assignment_key("hr1", 2, args[2]))
        self.assertNotEqual(key, assignment_key("hr", 12, date(2024, 1, 2)))
        for args in (
            ("", 1, date(2024, 1, 1)),
            ("hr", 0, date(2024, 1, 1)),
            ("hr", 1, None),
        ):
            with self.assertRaises(ValueError):
                assignment_key(*args)

    def test_movement_determinism_and_namespace_type_safety(self):
        key = movement_key("hr", "HIRE", 12)
        self.assertEqual(key, movement_key("hr", "HIRE", 12))
        self.assertNotEqual(key, movement_key("hr1", "HIRE", 2))
        self.assertNotEqual(key, movement_key("hr", "EXIT", 12))
        for kind in ("hire", "", "REHIRE", None):
            with self.assertRaises(ValueError):
                movement_key("hr", kind, 12)

    def test_length_frames_are_unambiguous_utf8(self):
        self.assertNotEqual(frame("ab") + frame("c"), frame("a") + frame("bc"))
        self.assertEqual(frame("é"), b"2:\xc3\xa9")
        self.assertNotEqual(frame("N"), frame(""))


class HashTests(TestCase):
    def test_exact_canonical_vector(self):
        row = unknown_business_row("dim_job_role")
        expected = (
            b"11:gold-record2:v112:dim_job_role5:grade6:STRING1:V7:Unknown"
            b"10:is_unknown7:BOOLEAN1:V4:true12:job_role_key6:BIGINT1:V1:0"
            b"9:role_name6:STRING1:V7:Unknown"
        )
        self.assertEqual(canonical_record("dim_job_role", row), expected)
        self.assertEqual(record_hash("dim_job_role", row), sha256(expected).hexdigest())

    def test_reordered_mapping_and_metadata_exclusion(self):
        row = unknown_business_row("dim_employee")
        expected = record_hash("dim_employee", row)
        self.assertEqual(
            expected, record_hash("dim_employee", dict(reversed(list(row.items()))))
        )
        for field in (
            "_gold_generated_at",
            "_gold_build_id",
            "_source_batch_id",
            "_record_hash",
            "_source_file",
        ):
            self.assertEqual(
                expected, record_hash("dim_employee", {**row, field: "changed"})
            )

    def test_changed_business_field_and_null_empty_are_distinct(self):
        row = unknown_business_row("dim_employee")
        self.assertNotEqual(
            record_hash("dim_employee", row),
            record_hash("dim_employee", {**row, "employee_number": ""}),
        )
        self.assertNotEqual(
            record_hash("dim_employee", row),
            record_hash("dim_employee", {**row, "hire_date_key": 20240101}),
        )

    def test_decimal_normalization_and_local_context(self):
        field = FieldContract("amount", "DECIMAL(18,2)")
        for value in ("10", "10.0", "10.00", "1E1", Decimal("10.000")):
            self.assertEqual(canonical_value(field, value), "10.00")
        self.assertEqual(canonical_value(field, "-0"), "0.00")
        with localcontext() as context:
            context.prec = 2
            self.assertEqual(canonical_value(field, "12345.67"), "12345.67")
        for value in (
            "10.001",
            "NaN",
            "Infinity",
            "10000000000000000",
            10.0,
            "invalid",
        ):
            with self.assertRaises(ValueError):
                canonical_value(field, value)
        self.assertEqual(
            canonical_value(FieldContract("hours", "DECIMAL(18,4)"), "0.1234"), "0.1234"
        )

    def test_dates_booleans_and_type_rejection(self):
        self.assertEqual(
            canonical_value(FieldContract("d", "DATE"), date(1, 1, 1)), "0001-01-01"
        )
        for value, expected in ((True, "true"), (False, "false")):
            self.assertEqual(
                canonical_value(FieldContract("b", "BOOLEAN"), value), expected
            )
        for field, value in (
            ("INT", True),
            ("BIGINT", 2**63),
            ("DATE", "2024-01-01"),
            ("BOOLEAN", 1),
        ):
            with self.assertRaises(ValueError):
                canonical_value(FieldContract("x", field), value)

    def test_unapproved_business_field_cannot_be_silently_unhashed(self):
        with self.assertRaises(ValueError):
            record_hash(
                "dim_employee", {**unknown_business_row("dim_employee"), "salary": 10}
            )

    def test_missing_and_nonnullable_fields_fail(self):
        row = unknown_business_row("dim_job_role")
        with self.assertRaises(KeyError):
            canonical_record(
                "dim_job_role", {k: v for k, v in row.items() if k != "grade"}
            )
        with self.assertRaises(ValueError):
            canonical_record("dim_job_role", {**row, "grade": None})

    def test_json_has_explicit_restricted_values(self):
        self.assertEqual(canonical_json({"b": True, "a": None}), '{"a":null,"b":true}')
        for value in (1.5, date(2024, 1, 1), {1: "bad"}, Decimal("1")):
            with self.assertRaises(TypeError):
                canonical_json(value)


class LogicalRowHashTests(TestCase):
    def test_decimal_formatting_produces_identical_row_hashes(self):
        from spark.gold.contracts import GOLD_MODELS

        row = {}
        for field in GOLD_MODELS["fact_payroll"].business_fields:
            row[field.name] = (
                "10.0"
                if field.logical_type.startswith("DECIMAL")
                else "fixture" if field.logical_type == "STRING" else 1
            )
        original = record_hash("fact_payroll", row)
        row["base_salary"] = Decimal("10.000")
        self.assertEqual(original, record_hash("fact_payroll", row))
        row["base_salary"] = Decimal("10.01")
        self.assertNotEqual(original, record_hash("fact_payroll", row))

    def test_date_boolean_and_literal_null_are_distinct(self):
        row = unknown_business_row("dim_date")
        original = record_hash("dim_date", row)
        row["calendar_date"] = date(2024, 2, 29)
        changed = record_hash("dim_date", row)
        self.assertNotEqual(original, changed)
        row["is_weekend"] = False
        self.assertNotEqual(changed, record_hash("dim_date", row))
        row = unknown_business_row("dim_employee")
        self.assertNotEqual(
            record_hash("dim_employee", row),
            record_hash("dim_employee", {**row, "employee_number": "N"}),
        )
