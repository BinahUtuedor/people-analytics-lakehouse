"""Executable gates for the five Phase 1 dimensions; no storage or cloud access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from operator import or_
from collections.abc import Mapping
import logging

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType, LongType, StringType

from spark.gold.contracts import GOLD_MODELS, get_gold_schema, unknown_business_row
from spark.gold.hashing import spark_record_hash
from spark.gold.manifest import BuildSpec, _utc
from spark.gold.validate import (
    Severity,
    ValidationContext,
    ValidationReport,
    ValidationResult,
    validate_schema,
)

CORE_DIMENSIONS = (
    "dim_date",
    "dim_employee",
    "dim_department",
    "dim_location",
    "dim_job_role",
)


@dataclass(frozen=True)
class DimensionBuild:
    """One explicit build and fixed audit instant shared by every core dimension.

    Phase 1's declared calendar is reporting_start through source_cutoff,
    inclusive. Source dates outside that interval fail, never extend it silently.
    All bounds already participate in the approved BuildSpec identity.
    """

    spec: BuildSpec
    generated_at: datetime

    def __post_init__(self) -> None:
        self.spec.require_supported()
        _utc(self.generated_at)

    def context(self, model: str) -> ValidationContext:
        if model not in CORE_DIMENSIONS:
            raise ValueError("Only the five Phase 1 core dimensions are executable")
        return ValidationContext(
            model, self.spec.silver_batch_id, self.spec.build_id, self.generated_at
        )

    def require_runtime(self, spark: SparkSession) -> None:
        expected = {
            "spark.sql.session.timeZone": self.spec.runtime.session_timezone,
            "spark.sql.ansi.enabled": str(self.spec.runtime.ansi_enabled).lower(),
            "spark.sql.caseSensitive": str(self.spec.runtime.case_sensitive).lower(),
        }
        for name, value in expected.items():
            if spark.conf.get(name) != value:
                raise ValueError(
                    f"Spark setting must match build identity: {name}={value}"
                )


def _result(context, name, violations, **metrics) -> ValidationResult:
    return ValidationResult(
        name,
        violations == 0,
        int(violations),
        Severity.CRITICAL,
        context,
        {k: str(v) for k, v in metrics.items()},
    )


def _gate(context, name, violations, **metrics) -> ValidationResult:
    result = _result(context, name, violations, **metrics)
    ValidationReport((result,)).raise_for_failure()
    return result


def _invalid(frame: DataFrame, condition) -> int:
    return frame.filter(F.coalesce(condition, F.lit(True))).count()


def _duplicates(frame: DataFrame, columns: tuple[str, ...]) -> int:
    # A single aggregate row is bounded; record_count counts affected rows,
    # rather than the number of duplicate key groups.
    duplicates = frame.groupBy(*columns).count().filter(F.col("count") > 1)
    return int(duplicates.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])


def validate_source(
    source: DataFrame, dataset: str, build: DimensionBuild, model: str
) -> ValidationReport:
    """Validate only consumed Silver fields, batch lineage and source uniqueness.

    Source schemas are checked explicitly rather than imported from the ORM.
    Unused source columns (including PII) are never copied into Gold.
    """
    build.require_runtime(source.sparkSession)
    context = build.context(model)
    definitions = {
        "employees": (
            ("employee_id",),
            ("employee_number", "employment_status", "employment_type"),
            ("hire_date", "termination_date"),
            ("employee_number",),
        ),
        "departments": (
            ("department_id", "business_unit_id"),
            ("department_name", "cost_center"),
            (),
            ("department_name", "cost_center"),
        ),
        "business_units": (("business_unit_id",), ("unit_name",), (), ("unit_name",)),
        "locations": (
            ("location_id",),
            ("office_name", "city", "country", "timezone"),
            (),
            (),
        ),
        "job_roles": (("role_id",), ("role_name", "grade"), (), ("role_name",)),
    }
    ids, labels, dates, unique = definitions[dataset]
    types = {
        **{n: LongType() for n in ids},
        **{n: StringType() for n in labels},
        **{n: DateType() for n in dates},
        "_batch_id": StringType(),
    }
    bad_schema = len(source.columns) != len(set(source.columns)) or any(
        name not in source.columns or source.schema[name].dataType != kind
        for name, kind in types.items()
    )
    results = [_gate(context, dataset + "_source_schema", int(bad_schema))]
    invalid = ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
    for name in ids:
        invalid = invalid | F.col(name).isNull() | (F.col(name) <= 0)
    for name in labels:
        invalid = (
            invalid
            | F.col(name).isNull()
            | (F.length(F.trim(F.col(name))) == 0)
            | (F.col(name) != F.trim(F.col(name)))
        )
    if dataset == "employees":
        invalid = (
            invalid
            | F.col("hire_date").isNull()
            | (
                F.col("termination_date").isNotNull()
                & (F.col("hire_date") > F.col("termination_date"))
            )
        )
    results.append(
        _gate(context, dataset + "_source_values", _invalid(source, invalid))
    )
    for name in (ids[0], *unique):
        results.append(
            _gate(context, dataset + "_unique_" + name, _duplicates(source, (name,)))
        )
    return ValidationReport(tuple(results))


def enforce_schema(frame: DataFrame, model: str) -> DataFrame:
    """Restore logical nullability after expressions/Parquet without row conversion.

    A required null raises at evaluation. The final typed literal is unreachable
    after raise_error, but lets Spark represent the field as non-nullable. No
    missing data is filled or repaired. Nullable branches retain the contract's
    unknown-member exceptions even when the current fixture has no nulls.
    """
    if model not in CORE_DIMENSIONS + (
        "dim_employee_assignment",
        "fact_employee_movement",
        "fact_workforce_monthly",
    ):
        raise ValueError("Not an implemented Gold model")
    schema = get_gold_schema(model)
    if frame.columns != schema.fieldNames() or any(
        frame.schema[f.name].dataType != f.dataType for f in schema
    ):
        raise ValueError(
            "Names, order and types must match before nullability enforcement"
        )
    columns = []
    for field in schema:
        value = F.col(field.name)
        if field.nullable:
            value = F.when(F.lit(True), value).otherwise(
                F.lit(None).cast(field.dataType)
            )
        else:
            default = (
                False
                if field.dataType.simpleString() == "boolean"
                else "" if isinstance(field.dataType, StringType) else 0
            )
            if field.dataType.simpleString() == "timestamp":
                default = datetime(1970, 1, 1, tzinfo=timezone.utc)
            value = F.coalesce(
                value,
                F.raise_error(F.lit("Required Gold field is null: " + field.name)).cast(
                    field.dataType
                ),
                F.lit(default).cast(field.dataType),
            )
        columns.append(value.alias(field.name))
    return frame.select(*columns)


def date_key_expression(column):
    return (F.year(column) * 10000 + F.month(column) * 100 + F.dayofmonth(column)).cast(
        "int"
    )


def validate_core_dimension(
    frame: DataFrame,
    model: str,
    build: DimensionBuild,
    sources: Mapping[str, DataFrame],
    *,
    dim_date: DataFrame | None = None,
) -> ValidationReport:
    """Fail closed on schema, metadata/hash, grain, source values and coverage."""
    context = build.context(model)
    build.require_runtime(frame.sparkSession)
    results = [validate_schema(frame.schema, context)]
    ValidationReport(tuple(results)).raise_for_failure()
    contract = GOLD_MODELS[model]
    key = contract.primary_key[0]
    real = frame.filter(~F.col("is_unknown"))
    unknown = frame.filter(F.col("is_unknown"))
    invalid = F.lit(False)
    for field in contract.fields:
        if not field.nullable:
            invalid = invalid | F.col(field.name).isNull()
        if field.unknown_only_null:
            invalid = invalid | (~F.col("is_unknown") & F.col(field.name).isNull())
        if field.logical_type == "STRING" and not field.name.startswith("_"):
            invalid = invalid | (
                ~F.col("is_unknown") & (F.length(F.trim(F.col(field.name))) == 0)
            )
    results.append(_gate(context, "required_values", _invalid(frame, invalid)))
    results.append(_gate(context, "primary_key", _duplicates(frame, (key,))))
    results.append(
        _gate(context, "positive_real_keys", _invalid(real, F.col(key) <= 0))
    )
    unknown_count = unknown.count()
    results.append(_gate(context, "one_unknown", abs(unknown_count - 1)))
    mismatch = reduce(
        or_,
        (
            ~F.col(name).eqNullSafe(F.lit(value))
            for name, value in unknown_business_row(model).items()
        ),
    )
    results.append(_gate(context, "unknown_values", _invalid(unknown, mismatch)))
    metadata_bad = (
        ~F.col("_source_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
        | ~F.col("_gold_build_id").eqNullSafe(F.lit(build.spec.build_id))
        | ~F.col("_gold_generated_at").eqNullSafe(
            F.lit(_utc(build.generated_at)).cast("timestamp")
        )
    )
    results.append(_gate(context, "build_metadata", _invalid(frame, metadata_bad)))
    results.append(
        _gate(
            context,
            "record_hash",
            _invalid(
                frame,
                ~F.col("_record_hash").eqNullSafe(spark_record_hash(frame, model)),
            ),
        )
    )
    if model == "dim_date":
        results.extend(_validate_calendar(real, context, build))
        expected_count = (
            build.spec.source_cutoff - build.spec.reporting_start
        ).days + 1
    else:
        source_name = contract.silver_sources[0]
        source = sources[source_name]
        for dataset in contract.silver_sources:
            results.extend(
                validate_source(sources[dataset], dataset, build, model).results
            )
        source_key = {
            "dim_employee": "employee_id",
            "dim_department": "department_id",
            "dim_location": "location_id",
            "dim_job_role": "role_id",
        }[model]
        expected_count = source.count()
        source_keys = source.select(F.col(source_key).alias(key))
        missing = source_keys.join(real.select(key), key, "left_anti").count()
        extra = real.select(key).join(source_keys, key, "left_anti").count()
        results.append(
            _gate(
                context,
                "source_key_coverage",
                missing + extra,
                missing=missing,
                extra=extra,
            )
        )
        results.extend(_validate_source_values(real, model, build, sources, dim_date))
    actual_count = real.count()
    results.append(
        _gate(
            context,
            "source_reconciliation",
            abs(actual_count - expected_count),
            source_entities=expected_count,
            real_rows=actual_count,
            unknown_rows=unknown_count,
            total_rows=actual_count + unknown_count,
        )
    )
    report = ValidationReport(tuple(results))
    logging.getLogger(__name__).info(
        "Gold validation | model=%s batch=%s build=%s real=%s unknown=%s passed=%s",
        model,
        context.source_batch_id,
        context.gold_build_id,
        actual_count,
        unknown_count,
        report.passed,
    )
    return report


def _validate_calendar(real, context, build):
    day = F.col("calendar_date")
    # ISO year is the year of the Thursday in this ISO week.
    iso_day = F.pmod(F.dayofweek(day) + 5, F.lit(7)) + 1
    expected = {
        "date_key": date_key_expression(day),
        "day_of_month": F.dayofmonth(day),
        "iso_day_of_week": iso_day,
        "day_name": F.date_format(day, "EEEE"),
        "iso_week_number": F.weekofyear(day),
        "iso_week_year": F.year(F.date_add(day, 4 - iso_day)),
        "month_number": F.month(day),
        "month_name": F.date_format(day, "MMMM"),
        "year_month": F.year(day) * 100 + F.month(day),
        "month_start_date": F.trunc(day, "month"),
        "month_end_date": F.last_day(day),
        "calendar_quarter": F.quarter(day),
        "calendar_year": F.year(day),
        "is_weekend": iso_day >= 6,
    }
    invalid = (day < F.lit(build.spec.reporting_start)) | (
        day > F.lit(build.spec.source_cutoff)
    )
    for name, value in expected.items():
        invalid = invalid | ~F.col(name).eqNullSafe(value)
    return [
        _gate(context, "calendar_attributes_and_bounds", _invalid(real, invalid)),
        _gate(context, "calendar_grain", _duplicates(real, ("calendar_date",))),
    ]


def _validate_source_values(real, model, build, sources, dim_date):
    context = build.context(model)
    source_name = GOLD_MODELS[model].silver_sources[0]
    source = sources[source_name]
    if model == "dim_employee":
        expected = source.select(
            F.col("employee_id").alias("employee_key"),
            "employee_number",
            date_key_expression(F.col("hire_date")).alias("hire_date_key"),
            F.coalesce(date_key_expression(F.col("termination_date")), F.lit(0)).alias(
                "termination_date_key"
            ),
            F.col("employment_status").alias("current_employment_status"),
            F.col("employment_type").alias("current_employment_type"),
        )
        if dim_date is None:
            raise ValueError(
                "Employee validation requires the same-build date dimension"
            )
        # Date validation includes metadata, hashes, uniqueness, coverage and unknown.
        validate_core_dimension(dim_date, "dim_date", build, {})
        missing = sum(
            real.select(F.col(role).alias("date_key"))
            .join(dim_date.select("date_key"), "date_key", "left_anti")
            .count()
            for role in ("hire_date_key", "termination_date_key")
        )
        results = [
            _gate(context, "date_foreign_keys", missing),
            _gate(
                context,
                "employee_number_unique",
                _duplicates(real, ("employee_number",)),
            ),
        ]
    elif model == "dim_department":
        units = sources["business_units"]
        orphans = source.join(
            units.select("business_unit_id"), "business_unit_id", "left_anti"
        ).count()
        results = [_gate(context, "business_unit_relationship", orphans)]
        expected = source.join(
            units.select("business_unit_id", "unit_name"), "business_unit_id"
        ).select(
            F.col("department_id").alias("department_key"),
            "department_name",
            "cost_center",
            "business_unit_id",
            F.col("unit_name").alias("business_unit_name"),
        )
    elif model == "dim_location":
        expected = source.select(
            F.col("location_id").alias("location_key"),
            "office_name",
            "city",
            "country",
            "timezone",
        )
        results = []
    else:
        expected = source.select(
            F.col("role_id").alias("job_role_key"), "role_name", "grade"
        )
        results = []
    actual = real.select(*expected.columns)
    differences = (
        actual.exceptAll(expected).count() + expected.exceptAll(actual).count()
    )
    results.append(_gate(context, "source_business_values", differences))
    return results
