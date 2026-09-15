"""Reusable Gold validation contracts; model-specific analytical gates come later."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections.abc import Mapping
from types import MappingProxyType
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spark.gold.contracts import GOLD_MODELS, get_gold_schema, unknown_business_row
from spark.gold.hashing import canonical_json, canonical_record
from spark.gold.manifest import _digest, _text, _utc


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"


class CheckKind(str, Enum):
    SCHEMA = "schema"
    REQUIRED_COLUMNS = "required_columns"
    NULLABILITY = "nullability"
    PRIMARY_KEY = "primary_key"
    GRAIN = "grain"
    FOREIGN_KEY = "foreign_key"
    GOVERNED_VALUES = "governed_values"
    NUMERIC = "numeric"
    DATE = "date"
    HASH = "hash"
    SOURCE_COVERAGE = "source_coverage"
    AGGREGATE_RECONCILIATION = "aggregate_reconciliation"
    UNKNOWN_MEMBER = "unknown_member"


@dataclass(frozen=True)
class ValidationContext:
    model: str
    source_batch_id: str
    gold_build_id: str
    executed_at: datetime

    def __post_init__(self):
        if self.model not in GOLD_MODELS:
            raise ValueError("Unknown MVP model")
        _text(self.source_batch_id)
        _digest(self.gold_build_id)
        _utc(self.executed_at)

    def to_dict(self) -> dict:
        return {
            "model": self.model,
            "source_batch_id": self.source_batch_id,
            "gold_build_id": self.gold_build_id,
            "executed_at": _utc(self.executed_at),
        }


@dataclass(frozen=True)
class ValidationRule:
    """Named future check, with explicit fields and optional reference relation.

    FK and coverage columns pair positionally with reference_columns. References
    resolve within the selected source batch/build via the supplied check inputs.
    Numeric/date/governed rules implement their approved predicates in a checker,
    not in a speculative expression language or catalogue.
    """

    check_name: str
    kind: CheckKind
    columns: tuple[str, ...]
    reference: str | None = None
    reference_columns: tuple[str, ...] = ()
    severity: Severity = Severity.CRITICAL

    def __post_init__(self):
        _text(self.check_name)
        if not isinstance(self.kind, CheckKind) or not isinstance(
            self.severity, Severity
        ):
            raise ValueError("Typed check kind/severity required")
        if not isinstance(self.columns, (tuple, list)) or not isinstance(
            self.reference_columns, (tuple, list)
        ):
            raise ValueError("Columns require explicit sequences, not a string")
        columns, reference_columns = tuple(self.columns), tuple(self.reference_columns)
        if not columns or len(columns) != len(set(columns)):
            raise ValueError("Checks require distinct explicit columns")
        for col in columns + reference_columns:
            _text(col)
        if self.kind in (CheckKind.FOREIGN_KEY, CheckKind.SOURCE_COVERAGE):
            _text(self.reference)
            if len(columns) != len(reference_columns):
                raise ValueError("Reference checks require paired columns")
        elif self.reference is not None:
            _text(self.reference)
        object.__setattr__(self, "columns", columns)
        object.__setattr__(self, "reference_columns", reference_columns)


@dataclass(frozen=True)
class ValidationResult:
    """record_count is the number of violating records (zero on success)."""

    check_name: str
    passed: bool
    record_count: int
    severity: Severity
    context: ValidationContext
    metrics: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        _text(self.check_name)
        if (
            type(self.passed) is not bool
            or type(self.record_count) is not int
            or self.record_count < 0
        ):
            raise ValueError("Invalid pass/count result")
        if self.passed and self.record_count:
            raise ValueError("Successful checks cannot report violating records")
        if not isinstance(self.severity, Severity) or not isinstance(
            self.context, ValidationContext
        ):
            raise ValueError("Typed severity/context required")
        metrics = dict(self.metrics)
        for key, value in metrics.items():
            _text(key)
            _text(value)
        object.__setattr__(self, "metrics", MappingProxyType(metrics))

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "record_count": self.record_count,
            "severity": self.severity.value,
            **self.context.to_dict(),
            "metrics": dict(self.metrics),
        }


class ValidationCheck(Protocol):
    """Read-only checker interface; implementations must not publish or repair."""

    def __call__(
        self,
        dataframe: DataFrame,
        rule: ValidationRule,
        context: ValidationContext,
        references: Mapping[str, DataFrame],
    ) -> ValidationResult: ...


class GoldValidationError(RuntimeError):
    def __init__(self, report: ValidationReport):
        self.report = report
        super().__init__(
            "Gold validation failed: "
            + ", ".join(
                f"{r.context.model}/{r.check_name}"
                for r in report.results
                if not r.passed
            )
        )


@dataclass(frozen=True)
class ValidationReport:
    """Stable order and fail-closed aggregation; warnings remain visible failures."""

    results: tuple[ValidationResult, ...]

    def __post_init__(self):
        results = tuple(self.results)
        if not results or any(not isinstance(r, ValidationResult) for r in results):
            raise ValueError("A report requires explicit validation results")
        identities = [(r.context.model, r.check_name) for r in results]
        if len(set(identities)) != len(identities):
            raise ValueError("Duplicate model/check results")
        builds = {(r.context.source_batch_id, r.context.gold_build_id) for r in results}
        if len(builds) != 1:
            raise ValueError("Cannot aggregate results across releases")
        object.__setattr__(
            self,
            "results",
            tuple(sorted(results, key=lambda r: (r.context.model, r.check_name))),
        )

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def to_json(self) -> str:
        return canonical_json(
            {"passed": self.passed, "results": [r.to_dict() for r in self.results]}
        )

    def raise_for_failure(self) -> None:
        if not self.passed:
            raise GoldValidationError(self)


def validate_schema(actual: StructType, context: ValidationContext) -> ValidationResult:
    """Check order/names/types/nullability without requiring a Spark session."""
    expected = get_gold_schema(context.model)
    observed = [(f.name, f.dataType, f.nullable) for f in actual]
    approved = [(f.name, f.dataType, f.nullable) for f in expected]
    passed = observed == approved
    return ValidationResult(
        "exact_schema", passed, 0 if passed else 1, Severity.CRITICAL, context
    )


def validate_member_row(model: str, row: Mapping[str, object]) -> None:
    """Reference fixture/member rule; whole-dataset uniqueness is a separate gate.

    This is not a dimension builder or a replacement for distributed validation.
    Exactly one reserved member, FK integrity and source coverage must also be
    checked by later model implementations using the interfaces above.
    """
    contract = GOLD_MODELS[model]
    if not contract.unknown_member:
        raise ValueError("Unknown-member validation applies only to dimensions")
    canonical_record(model, row)  # Check values without coercion or dropped fields.
    if row["is_unknown"]:
        expected = unknown_business_row(model)
        if any(row[name] != value for name, value in expected.items()):
            raise ValueError("Invalid reserved unknown member")
        return
    for field in contract.business_fields:
        if field.unknown_only_null and row[field.name] is None:
            raise ValueError(f"Null only permitted on unknown member: {field.name}")
    key = row[contract.primary_key[0]]
    if model == "dim_employee_assignment":
        _digest(key)
        if row["employee_key"] <= 0:
            raise ValueError("Real assignment must resolve to a real employee")
    elif type(key) is not int or key <= 0:
        raise ValueError("Real dimension IDs must be positive")
