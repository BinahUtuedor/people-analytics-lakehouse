"""Source-conformed restricted payroll at actual-period-end assignment.

Inputs are explicit Silver frames and validated same-build Gold dimensions.
No source discovery, business history reconstruction, wall clock or storage IO.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
)

from spark.gold.contracts import GOLD_MODELS, unknown_business_row
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
from spark.gold.validate import ValidationReport, validate_schema
from spark.gold.workforce_history import (
    _ctx,
    _phase2_finish,
    validate_assignment_intervals,
)
from spark.gold.workforce_monthly import ASSIGNMENT_FIELDS, _metadata_invalid

PAYROLL_MODEL = "fact_payroll"
MONEY_FIELDS = tuple(
    f.name
    for f in GOLD_MODELS[PAYROLL_MODEL].business_fields
    if f.logical_type == "DECIMAL(18,2)"
)
PERIOD_FIELDS = ("pay_period_start", "pay_period_end")
PERIOD_KEYS = ("pay_period_start_key", "pay_period_end_key")
CENT = Decimal("0.01")


def _arithmetic_invalid():
    """Independent persistence rounding: exact gross; one cent for deductions/net.

    Base and overtime already have cent scale before gross is rounded. Pension,
    tax, deductions and net are independently rounded in simulator/payroll.py.
    Deductions already include pension/tax: never subtract those twice.
    """
    return (
        (
            F.col("gross_pay")
            != F.col("base_salary") + F.col("overtime_pay") + F.col("bonus")
        )
        | (
            F.abs(
                F.col("deductions")
                - F.col("pension_contribution")
                - F.col("tax_amount")
            )
            > F.lit(CENT)
        )
        | (
            F.abs(F.col("net_pay") - F.col("gross_pay") + F.col("deductions"))
            > F.lit(CENT)
        )
    )


def normalize_payroll_source(source: DataFrame, build: DimensionBuild) -> DataFrame:
    """Check original precision before normalizing retained Silver numeric/date types.

    The extractor can retain DOUBLE bonus/deductions/pension; convert their
    decimal text before any arithmetic. A roundtrip check detects sub-18-scale
    loss in that intermediate representation. No binary monetary arithmetic.
    Source DATE columns may be retained as midnight TIMESTAMP by pandas/Silver.
    """
    context = _ctx(build, PAYROLL_MODEL)
    build.require_runtime(source.sparkSession)
    types = {
        "payroll_id": LongType(),
        "employee_id": LongType(),
        "currency": StringType(),
        "payroll_status": StringType(),
        "_batch_id": StringType(),
    }
    bad = len(source.columns) != len(set(source.columns)) or any(
        n not in source.columns or source.schema[n].dataType != t
        for n, t in types.items()
    )
    for name in PERIOD_FIELDS:
        bad = (
            bad
            or name not in source.columns
            or not isinstance(source.schema[name].dataType, (DateType, TimestampType))
        )
    for name in MONEY_FIELDS:
        bad = (
            bad
            or name not in source.columns
            or not isinstance(source.schema[name].dataType, (DecimalType, DoubleType))
        )
    _gate(context, "payroll_source_schema", int(bad))
    invalid = ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
    for name in types:
        invalid = invalid | F.col(name).isNull()
    for name in ("payroll_id", "employee_id"):
        invalid = invalid | (F.col(name) <= 0)
    for name, length in (("currency", 10), ("payroll_status", 30)):
        invalid = (
            invalid
            | (F.length(F.trim(F.col(name))) == 0)
            | (F.col(name) != F.trim(F.col(name)))
            | (F.length(name) > length)
        )
    expressions = [F.col(n) for n in types if n != "_batch_id"]
    for name in PERIOD_FIELDS:
        value = F.col(name)
        day = value.cast("date")
        invalid = invalid | value.isNull()
        if isinstance(source.schema[name].dataType, TimestampType):
            invalid = invalid | ~value.eqNullSafe(day.cast("timestamp"))
        expressions.append(day.alias(name))
    for name in MONEY_FIELDS:
        value = F.col(name)
        # try_cast is available in both supported Spark runtimes; defects become
        # explicit validation failures rather than rounded accepted measures.
        wide = F.expr(f"try_cast(cast(`{name}` as string) as decimal(38,18))")
        cents = F.expr(f"try_cast(cast(`{name}` as string) as decimal(18,2))")
        invalid = (
            invalid
            | value.isNull()
            | wide.isNull()
            | cents.isNull()
            | (wide != cents)
            | (wide < 0)
        )
        if isinstance(source.schema[name].dataType, DoubleType):
            invalid = invalid | F.isnan(value) | ~wide.cast("double").eqNullSafe(value)
        else:
            invalid = invalid | ~wide.eqNullSafe(value)
        expressions.append(cents.alias(name))
    _gate(context, "payroll_source_values_precision", _invalid(source, invalid))
    normalized = source.select(*expressions)
    _gate(context, "payroll_source_id_unique", _duplicates(normalized, ("payroll_id",)))
    _gate(
        context,
        "payroll_source_period_unique",
        _duplicates(normalized, ("employee_id", *PERIOD_FIELDS)),
    )
    invalid_period = (
        (F.col("pay_period_start") > F.col("pay_period_end"))
        | (F.trunc("pay_period_start", "month") != F.trunc("pay_period_end", "month"))
        | (F.col("pay_period_end") > F.lit(build.spec.source_cutoff))
    )
    _gate(context, "payroll_actual_period", _invalid(normalized, invalid_period))
    _gate(
        context,
        "payroll_source_arithmetic",
        _invalid(normalized, _arithmetic_invalid()),
    )
    return normalized


def _validate_parents(sources, build):
    """Validate supplied history bounds and required same-build parent contracts."""
    context = _ctx(build, PAYROLL_MODEL)
    validate_source(sources["employees"], "employees", build, "dim_employee")
    validate_core_dimension(sources["dim_date"], "dim_date", build, {})
    for model in (
        "dim_employee",
        "dim_department",
        "dim_location",
        "dim_job_role",
        "dim_employee_assignment",
    ):
        parent = sources[model]
        ValidationReport(
            (validate_schema(parent.schema, _ctx(build, model)),)
        ).raise_for_failure()
        contract = GOLD_MODELS[model]
        _gate(context, model + "_unique", _duplicates(parent, contract.primary_key))
        invalid = _metadata_invalid(build) | ~F.col("_record_hash").eqNullSafe(
            spark_record_hash(parent, model)
        )
        for field in contract.fields:
            if not field.nullable:
                invalid = invalid | F.col(field.name).isNull()
            elif field.unknown_only_null:
                invalid = invalid | (~F.col("is_unknown") & F.col(field.name).isNull())
        key = contract.primary_key[0]
        reserved = "0" if model == "dim_employee_assignment" else 0
        invalid = invalid | ~F.col("is_unknown").eqNullSafe(
            F.col(key) == F.lit(reserved)
        )
        for name, value in unknown_business_row(model).items():
            invalid = invalid | (
                F.col("is_unknown") & ~F.col(name).eqNullSafe(F.lit(value))
            )
        _gate(context, model + "_values_hash_metadata", _invalid(parent, invalid))
        _gate(context, model + "_unknown", abs(parent.filter("is_unknown").count() - 1))
    validate_assignment_intervals(
        sources["dim_employee_assignment"], sources["employees"], build
    )


def _validate_employment(payroll, sources, build):
    """Keep actual payroll wholly within its source employee employment window."""
    joined = payroll.join(
        sources["employees"].select("employee_id", "hire_date", "termination_date"),
        "employee_id",
        "left",
    )
    invalid = (
        F.col("hire_date").isNull()
        | (F.col("pay_period_start") < F.col("hire_date"))
        | (F.col("pay_period_end") > F.col("termination_date"))
    )
    _gate(
        _ctx(build, PAYROLL_MODEL),
        "payroll_employment_window",
        _invalid(joined, F.coalesce(invalid, F.lit(False))),
    )


def _transform(payroll, sources, build):
    """Resolve actual-end history and project only the approved business fields."""
    assignment = (
        sources["dim_employee_assignment"].filter(~F.col("is_unknown")).alias("a")
    )
    resolved = (
        payroll.alias("p")
        .join(
            assignment,
            (F.col("p.employee_id") == F.col("a.employee_key"))
            & (F.col("a.valid_from_date") <= F.col("p.pay_period_end"))
            & (F.col("p.pay_period_end") < F.col("a.valid_to_exclusive")),
            "left",
        )
        .select("p.*", *[F.col("a." + n).alias(n) for n in ASSIGNMENT_FIELDS])
    )
    context = _ctx(build, PAYROLL_MODEL)
    _gate(
        context,
        "payroll_required_assignment",
        resolved.filter(
            F.col("assignment_key").isNull() | (F.col("assignment_key") == "0")
        ).count(),
    )
    _gate(context, "payroll_assignment_fanout", _duplicates(resolved, ("payroll_id",)))
    business = resolved.select(
        "payroll_id",
        F.col("employee_id").alias("employee_key"),
        *[date_key_expression(F.col(n)).alias(n + "_key") for n in PERIOD_FIELDS],
        date_key_expression(F.last_day("pay_period_end")).alias("payroll_month_key"),
        F.year("pay_period_end").alias("reporting_year"),
        (F.datediff("pay_period_end", "pay_period_start") + 1).alias("pay_period_days"),
        *ASSIGNMENT_FIELDS,
        "currency",
        "payroll_status",
        *MONEY_FIELDS,
    )
    return _phase2_finish(business, PAYROLL_MODEL, build)


def payroll_key_reconciliation(output: DataFrame, payroll: DataFrame) -> dict[str, int]:
    """Bounded scalar metrics at source payroll ID grain, never employee count."""
    source_keys, gold_keys = payroll.select("payroll_id"), output.select("payroll_id")
    return {
        "source_rows": payroll.count(),
        "gold_rows": output.count(),
        "source_business_keys": source_keys.distinct().count(),
        "gold_business_keys": gold_keys.distinct().count(),
        "missing_keys": source_keys.join(gold_keys, "payroll_id", "left_anti").count(),
        "unexpected_keys": gold_keys.join(
            source_keys, "payroll_id", "left_anti"
        ).count(),
        "source_duplicate_rows": _duplicates(payroll, ("payroll_id",)),
        "gold_duplicate_rows": _duplicates(output, ("payroll_id",)),
    }


def payroll_monetary_reconciliation(
    output: DataFrame, normalized_source: DataFrame
) -> DataFrame:
    """Distributed actual-period/currency totals of each separate source component.

    Accept the validated frame returned by normalize_payroll_source. No currency
    conversion and no total combining overlapping payroll components.
    """
    keys = (*PERIOD_KEYS, "currency")
    source = normalized_source.select(
        "*", *[date_key_expression(F.col(n)).alias(n + "_key") for n in PERIOD_FIELDS]
    )

    def aggregate(frame, prefix):
        return frame.groupBy(*keys).agg(
            F.count("payroll_id").alias(prefix + "rows"),
            F.countDistinct("payroll_id").alias(prefix + "keys"),
            *[F.sum(n).alias(prefix + n) for n in MONEY_FIELDS],
        )

    result = aggregate(source, "source_").join(
        aggregate(output, "gold_"), list(keys), "full"
    )
    for name in ("rows", "keys", *MONEY_FIELDS):
        result = result.withColumn(
            name + "_difference",
            F.coalesce(F.col("gold_" + name), F.lit(0))
            - F.coalesce(F.col("source_" + name), F.lit(0)),
        )
    return result


def _validate_output(output, payroll, sources, build):
    """Check structure, references and independent ID/component reconciliations."""
    context = _ctx(build, PAYROLL_MODEL)
    results = [validate_schema(output.schema, context)]
    ValidationReport(tuple(results)).raise_for_failure()
    invalid = (
        _metadata_invalid(build)
        | ~F.col("_record_hash").eqNullSafe(spark_record_hash(output, PAYROLL_MODEL))
        | _arithmetic_invalid()
    )
    for field in GOLD_MODELS[PAYROLL_MODEL].fields:
        invalid = invalid | F.col(field.name).isNull()
    for name in MONEY_FIELDS:
        invalid = invalid | (F.col(name) < 0)
    invalid = (
        invalid
        | (F.col("payroll_id") <= 0)
        | (F.col("employee_key") <= 0)
        | (F.col("pay_period_days") <= 0)
        | (F.col("assignment_key") == "0")
    )
    for name in (*PERIOD_KEYS, "payroll_month_key"):
        invalid = invalid | (F.col(name) <= 0)
    results.append(
        _gate(context, "payroll_values_hash_metadata", _invalid(output, invalid))
    )
    for grain in (("payroll_id",), ("employee_key", *PERIOD_KEYS)):
        results.append(
            _gate(context, "payroll_unique_" + grain[0], _duplicates(output, grain))
        )
    for key, model, parent_key in (
        ("employee_key", "dim_employee", "employee_key"),
        ("manager_employee_key", "dim_employee", "employee_key"),
        ("department_key", "dim_department", "department_key"),
        ("job_role_key", "dim_job_role", "job_role_key"),
        ("location_key", "dim_location", "location_key"),
        ("assignment_key", "dim_employee_assignment", "assignment_key"),
        *[(n, "dim_date", "date_key") for n in (*PERIOD_KEYS, "payroll_month_key")],
    ):
        missing = (
            output.select(key)
            .join(sources[model].select(F.col(parent_key).alias(key)), key, "left_anti")
            .count()
        )
        results.append(_gate(context, key + "_foreign_key", missing))
    metrics = payroll_key_reconciliation(output, payroll)
    violations = (
        abs(metrics["source_rows"] - metrics["gold_rows"])
        + metrics["missing_keys"]
        + metrics["unexpected_keys"]
        + metrics["source_duplicate_rows"]
        + metrics["gold_duplicate_rows"]
    )
    results.append(
        _gate(context, "payroll_source_key_reconciliation", violations, **metrics)
    )
    amounts = payroll_monetary_reconciliation(output, payroll)
    bad = F.col("source_rows").isNull() | F.col("gold_rows").isNull()
    for name in ("rows", "keys", *MONEY_FIELDS):
        bad = bad | (F.col(name + "_difference") != 0)
    results.append(
        _gate(context, "payroll_period_currency_reconciliation", _invalid(amounts, bad))
    )
    logging.getLogger(__name__).info(
        "Gold payroll reconciliation | batch=%s build=%s source_rows=%s gold_rows=%s missing=%s unexpected=%s",
        build.spec.silver_batch_id,
        build.spec.build_id,
        metrics["source_rows"],
        metrics["gold_rows"],
        metrics["missing_keys"],
        metrics["unexpected_keys"],
    )
    return results


def build_fact_payroll(
    sources: Mapping[str, DataFrame], build: DimensionBuild
) -> DataFrame:
    """One row per legitimate Silver payroll record; validate before returning."""
    payroll = normalize_payroll_source(sources["payroll"], build)
    _validate_parents(sources, build)
    _validate_employment(payroll, sources, build)
    output = _transform(payroll, sources, build)
    _validate_output(output, payroll, sources, build)
    return output


def validate_payroll(
    output: DataFrame, build: DimensionBuild, sources: Mapping[str, DataFrame]
) -> ValidationReport:
    """Validate every contract field and reconstruct rows before/after publication."""
    payroll = normalize_payroll_source(sources["payroll"], build)
    _validate_parents(sources, build)
    _validate_employment(payroll, sources, build)
    results = _validate_output(output, payroll, sources, build)
    expected = _transform(payroll, sources, build)
    results.append(
        _gate(
            _ctx(build, PAYROLL_MODEL),
            "payroll_source_reconstruction",
            output.exceptAll(expected).count() + expected.exceptAll(output).count(),
        )
    )
    logging.getLogger(__name__).info(
        "Gold payroll validated | batch=%s build=%s cutoff=%s passed=True",
        build.spec.silver_batch_id,
        build.spec.build_id,
        build.spec.source_cutoff,
    )
    return ValidationReport(tuple(results))
