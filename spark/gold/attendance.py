"""Restricted daily attendance fact at source-record grain."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from spark.gold.contracts import GOLD_MODELS
from spark.gold.dimension_validation import (
    DimensionBuild,
    _duplicates,
    _gate,
    _invalid,
    date_key_expression,
    validate_core_dimension,
    validate_source,
)
from spark.gold.hashing import spark_record_hash
from spark.gold.reference import absence_domain
from spark.gold.validate import ValidationReport, validate_schema
from spark.gold.workforce_history import (
    _ctx,
    _phase2_finish,
    validate_assignment_intervals,
)
from spark.gold.workforce_monthly import ASSIGNMENT_FIELDS, _metadata_invalid

ATTENDANCE_MODEL = "fact_attendance"
STATUS_VALUES = ("Present", "Remote", "Hybrid", "Training", "Business Travel", "Absent")
MONEY_FIELDS = ("hours_worked", "overtime_hours")
ABSENT_STATUS = "Absent"


def _decimal_invalid(frame: DataFrame, name: str):
    value = F.col(name)
    wide = F.expr(f"try_cast(cast(`{name}` as string) as decimal(38,18))")
    exact = F.expr(f"try_cast(cast(`{name}` as string) as decimal(18,4))")
    invalid = (
        value.isNull() | wide.isNull() | exact.isNull() | (wide != exact) | (wide < 0)
    )
    if isinstance(frame.schema[name].dataType, DoubleType):
        invalid = invalid | F.isnan(value) | ~wide.cast("double").eqNullSafe(value)
    else:
        invalid = invalid | ~wide.eqNullSafe(value)
    return invalid


def normalize_attendance_source(source: DataFrame, build: DimensionBuild) -> DataFrame:
    """Validate Silver attendance values before exact Gold normalization."""
    context = _ctx(build, ATTENDANCE_MODEL)
    build.spec.require_supported()
    build.require_runtime(source.sparkSession)
    required = {
        "attendance_id": LongType(),
        "employee_id": LongType(),
        "status": StringType(),
        "absence_reason": StringType(),
        "_batch_id": StringType(),
    }
    bad_schema = len(source.columns) != len(set(source.columns)) or any(
        n not in source.columns or source.schema[n].dataType != t
        for n, t in required.items()
    )
    bad_schema = (
        bad_schema
        or "work_date" not in source.columns
        or not isinstance(
            source.schema["work_date"].dataType, (DateType, TimestampType)
        )
    )
    for name in MONEY_FIELDS:
        bad_schema = (
            bad_schema
            or name not in source.columns
            or not isinstance(source.schema[name].dataType, (DecimalType, DoubleType))
        )
    if "absence_reason" not in source.columns:
        bad_schema = True
    _gate(context, "attendance_source_schema", int(bad_schema))

    invalid = ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
    for name in required.keys() - {"absence_reason"}:
        invalid = invalid | F.col(name).isNull()
    invalid = invalid | (F.col("attendance_id") <= 0) | (F.col("employee_id") <= 0)
    invalid = invalid | ~F.col("status").isin(*STATUS_VALUES)
    invalid = invalid | (
        F.col("absence_reason").isNotNull()
        & ~F.col("absence_reason").isin(*absence_domain())
    )
    invalid = invalid | F.col("absence_reason").isNotNull() & (
        F.length(F.trim("absence_reason")) == 0
    )
    work_date = F.col("work_date")
    day = work_date.cast("date")
    invalid = invalid | work_date.isNull()
    if isinstance(source.schema["work_date"].dataType, TimestampType):
        invalid = invalid | ~work_date.eqNullSafe(day.cast("timestamp"))
    invalid = (
        invalid | (F.col("status") == ABSENT_STATUS) & F.col("absence_reason").isNull()
    )
    invalid = (
        invalid
        | (F.col("status") != ABSENT_STATUS) & F.col("absence_reason").isNotNull()
    )
    for name in MONEY_FIELDS:
        invalid = invalid | _decimal_invalid(source, name)
    _gate(context, "attendance_source_values", _invalid(source, invalid))

    normalized = source.select(
        "attendance_id",
        "employee_id",
        day.alias("work_date"),
        "status",
        "absence_reason",
        *[
            F.expr(f"try_cast(cast(`{n}` as string) as decimal(18,4))").alias(n)
            for n in MONEY_FIELDS
        ],
    )
    _gate(
        context,
        "attendance_source_id_unique",
        _duplicates(normalized, ("attendance_id",)),
    )
    _gate(
        context,
        "attendance_source_daily_unique",
        _duplicates(normalized, ("employee_id", "work_date")),
    )
    invalid_date = F.col("work_date") > F.lit(build.spec.source_cutoff)
    _gate(context, "attendance_source_cutoff", _invalid(normalized, invalid_date))
    return normalized


def _validate_parents(sources, build):
    context = _ctx(build, ATTENDANCE_MODEL)
    validate_source(sources["employees"], "employees", build, "dim_employee")
    validate_core_dimension(sources["dim_date"], "dim_date", build, {})
    for model in (
        "dim_employee",
        "dim_department",
        "dim_location",
        "dim_job_role",
        "dim_employee_assignment",
    ):
        report = validate_schema(sources[model].schema, _ctx(build, model))
        ValidationReport((report,)).raise_for_failure()
        contract = GOLD_MODELS[model]
        _gate(
            context,
            model + "_unique",
            _duplicates(sources[model], contract.primary_key),
        )
        invalid = _metadata_invalid(build) | ~F.col("_record_hash").eqNullSafe(
            spark_record_hash(sources[model], model)
        )
        for field in contract.fields:
            if not field.nullable:
                invalid = invalid | F.col(field.name).isNull()
        _gate(
            context, model + "_values_hash_metadata", _invalid(sources[model], invalid)
        )
    validate_assignment_intervals(
        sources["dim_employee_assignment"], sources["employees"], build
    )


def _validate_employment(attendance, sources, build):
    joined = attendance.join(
        sources["employees"].select("employee_id", "hire_date", "termination_date"),
        "employee_id",
        "left",
    )
    invalid = (
        F.col("hire_date").isNull()
        | (F.col("work_date") < F.col("hire_date"))
        | (
            F.col("termination_date").isNotNull()
            & (F.col("work_date") > F.col("termination_date"))
        )
    )
    _gate(
        _ctx(build, ATTENDANCE_MODEL),
        "attendance_employment_window",
        _invalid(joined, invalid),
    )


def _transform(attendance, sources, build):
    assignment = (
        sources["dim_employee_assignment"].filter(~F.col("is_unknown")).alias("a")
    )
    resolved = (
        attendance.alias("s")
        .join(
            assignment,
            (F.col("s.employee_id") == F.col("a.employee_key"))
            & (F.col("a.valid_from_date") <= F.col("s.work_date"))
            & (F.col("s.work_date") < F.col("a.valid_to_exclusive")),
            "left",
        )
        .select("s.*", *[F.col("a." + n).alias(n) for n in ASSIGNMENT_FIELDS])
    )
    context = _ctx(build, ATTENDANCE_MODEL)
    _gate(
        context,
        "attendance_required_assignment",
        resolved.filter(
            F.col("assignment_key").isNull() | (F.col("assignment_key") == "0")
        ).count(),
    )
    _gate(
        context,
        "attendance_assignment_fanout",
        _duplicates(resolved, ("attendance_id",)),
    )
    business = resolved.select(
        "attendance_id",
        F.col("employee_id").alias("employee_key"),
        date_key_expression(F.col("work_date")).alias("work_date_key"),
        F.year("work_date").alias("reporting_year"),
        *ASSIGNMENT_FIELDS,
        F.col("status").alias("attendance_status"),
        "absence_reason",
        *MONEY_FIELDS,
        F.lit(1).cast("int").alias("recorded_day_count"),
        F.when(F.col("status") == ABSENT_STATUS, 1)
        .otherwise(0)
        .cast("int")
        .alias("absent_day_count"),
    )
    return _phase2_finish(business, ATTENDANCE_MODEL, build)


def attendance_key_reconciliation(
    output: DataFrame, source: DataFrame
) -> dict[str, int]:
    source_keys, gold_keys = source.select("attendance_id"), output.select(
        "attendance_id"
    )
    return {
        "source_rows": source.count(),
        "gold_rows": output.count(),
        "source_keys": source_keys.distinct().count(),
        "gold_keys": gold_keys.distinct().count(),
        "missing_keys": source_keys.join(
            gold_keys, "attendance_id", "left_anti"
        ).count(),
        "unexpected_keys": gold_keys.join(
            source_keys, "attendance_id", "left_anti"
        ).count(),
        "source_duplicates": _duplicates(source, ("attendance_id",)),
        "gold_duplicates": _duplicates(output, ("attendance_id",)),
    }


def attendance_measure_reconciliation(
    output: DataFrame, source: DataFrame
) -> DataFrame:
    source_frame = source.select(
        "*", date_key_expression(F.col("work_date")).alias("work_date_key")
    )
    gold_frame = output.select("*", F.col("attendance_status").alias("status")).drop(
        "attendance_status"
    )
    keys = ("work_date_key", "status")

    def aggregate(frame, prefix):
        return frame.groupBy(*keys).agg(
            F.count("attendance_id").alias(prefix + "rows"),
            *[F.sum(n).alias(prefix + n) for n in MONEY_FIELDS],
            (
                F.sum("recorded_day_count").alias(prefix + "recorded_day_count")
                if "recorded_day_count" in frame.columns
                else F.count("attendance_id").alias(prefix + "recorded_day_count")
            ),
            (
                F.sum("absent_day_count").alias(prefix + "absent_day_count")
                if "absent_day_count" in frame.columns
                else F.sum(
                    F.when(F.col("status") == ABSENT_STATUS, 1).otherwise(0)
                ).alias(prefix + "absent_day_count")
            ),
        )

    result = aggregate(source_frame, "source_").join(
        aggregate(gold_frame, "gold_"), list(keys), "full"
    )
    for name in ("rows", *MONEY_FIELDS, "recorded_day_count", "absent_day_count"):
        result = result.withColumn(
            name + "_difference",
            F.coalesce(F.col("gold_" + name), F.lit(0))
            - F.coalesce(F.col("source_" + name), F.lit(0)),
        )
    return result


def _validate_output(output, source, sources, build):
    context = _ctx(build, ATTENDANCE_MODEL)
    ValidationReport((validate_schema(output.schema, context),)).raise_for_failure()
    invalid = _metadata_invalid(build) | ~F.col("_record_hash").eqNullSafe(
        spark_record_hash(output, ATTENDANCE_MODEL)
    )
    for field in GOLD_MODELS[ATTENDANCE_MODEL].fields:
        if not field.nullable:
            invalid = invalid | F.col(field.name).isNull()
    invalid = (
        invalid
        | (F.col("attendance_id") <= 0)
        | (F.col("employee_key") <= 0)
        | (F.col("assignment_key") == "0")
    )
    invalid = (
        invalid
        | (F.col("recorded_day_count") != 1)
        | ~F.col("absent_day_count").isin(0, 1)
    )
    invalid = invalid | (F.col("hours_worked") < 0) | (F.col("overtime_hours") < 0)
    _gate(context, "attendance_values_hash_metadata", _invalid(output, invalid))
    for grain in (("attendance_id",), ("employee_key", "work_date_key")):
        _gate(context, "attendance_unique_" + grain[0], _duplicates(output, grain))
    for key, model, parent_key in (
        ("employee_key", "dim_employee", "employee_key"),
        ("manager_employee_key", "dim_employee", "employee_key"),
        ("department_key", "dim_department", "department_key"),
        ("job_role_key", "dim_job_role", "job_role_key"),
        ("location_key", "dim_location", "location_key"),
        ("assignment_key", "dim_employee_assignment", "assignment_key"),
        ("work_date_key", "dim_date", "date_key"),
    ):
        missing = (
            output.select(key)
            .join(sources[model].select(F.col(parent_key).alias(key)), key, "left_anti")
            .count()
        )
        _gate(context, key + "_foreign_key", missing)
    metrics = attendance_key_reconciliation(output, source)
    violations = (
        abs(metrics["source_rows"] - metrics["gold_rows"])
        + metrics["missing_keys"]
        + metrics["unexpected_keys"]
        + metrics["source_duplicates"]
        + metrics["gold_duplicates"]
    )
    _gate(context, "attendance_source_key_reconciliation", violations, **metrics)
    measures = attendance_measure_reconciliation(output, source)
    bad = F.col("source_rows").isNull() | F.col("gold_rows").isNull()
    for name in ("rows", *MONEY_FIELDS, "recorded_day_count", "absent_day_count"):
        bad = bad | (F.col(name + "_difference") != 0)
    _gate(context, "attendance_measure_reconciliation", _invalid(measures, bad))
    return metrics


def build_fact_attendance(
    sources: Mapping[str, DataFrame], build: DimensionBuild
) -> DataFrame:
    source = normalize_attendance_source(sources["attendance"], build)
    _validate_parents(sources, build)
    _validate_employment(source, sources, build)
    output = _transform(source, sources, build)
    _validate_output(output, source, sources, build)
    return output


def validate_attendance(
    output: DataFrame, build: DimensionBuild, sources: Mapping[str, DataFrame]
) -> ValidationReport:
    source = normalize_attendance_source(sources["attendance"], build)
    _validate_parents(sources, build)
    _validate_employment(source, sources, build)
    metrics = _validate_output(output, source, sources, build)
    expected = _transform(source, sources, build)
    results = [
        _gate(
            _ctx(build, ATTENDANCE_MODEL),
            "attendance_source_reconstruction",
            output.exceptAll(expected).count() + expected.exceptAll(output).count(),
            **metrics,
        )
    ]
    logging.getLogger(__name__).info(
        "Gold attendance validated | batch=%s build=%s rows=%s",
        build.spec.silver_batch_id,
        build.spec.build_id,
        metrics["gold_rows"],
    )
    return ValidationReport(tuple(results))
