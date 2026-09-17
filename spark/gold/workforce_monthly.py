"""Closed employee-month participation snapshots with post-event headcount.

Inputs are explicit, already validated Phase 1/2 dimensions and Silver frames.
No history reconstruction, source discovery, clock lookup or storage access.
"""

from collections.abc import Mapping
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DateType, LongType, StringType

from spark.gold.contracts import GOLD_MODELS
from spark.gold.dimension_validation import (
    DimensionBuild,
    _duplicates,
    _gate,
    _invalid,
    validate_core_dimension,
)
from spark.gold.hashing import spark_record_hash
from spark.gold.manifest import _utc
from spark.gold.workforce_history import _ctx, _phase2_finish
from spark.gold.validate import ValidationReport, validate_schema

WORKFORCE_MODEL = "fact_workforce_monthly"
ASSIGNMENT_FIELDS = (
    "assignment_key",
    "department_key",
    "job_role_key",
    "location_key",
    "manager_employee_key",
)


def closed_months(dim_date: DataFrame, build: DimensionBuild) -> DataFrame:
    """Select actual date-dimension month ends inside the declared reporting range."""
    day = F.col("calendar_date")
    return dim_date.filter(
        ~F.col("is_unknown")
        & (day == F.last_day(day))
        & (day >= F.lit(build.spec.reporting_start))
        & (day <= F.lit(min(build.spec.reporting_end, build.spec.source_cutoff)))
    ).select(
        F.col("date_key").alias("snapshot_month_key"),
        day.alias("snapshot_date"),
        F.trunc(day, "month").alias("month_start"),
    )


def _population(employees, months):
    """Inclusive monthly participation and separate post-exit closing state."""
    return (
        employees.select(
            F.col("employee_id").alias("employee_key"),
            "hire_date",
            "termination_date",
        )
        .join(
            months,
            (F.col("hire_date") <= F.col("snapshot_date"))
            & (
                F.col("termination_date").isNull()
                | (F.col("termination_date") >= F.col("month_start"))
            ),
        )
        .withColumn(
            "headcount_eom",
            F.when(
                F.col("termination_date").isNull()
                | (F.col("termination_date") > F.col("snapshot_date")),
                1,
            ).otherwise(0),
        )
        .withColumn("assignment_date", F.least("snapshot_date", "termination_date"))
    )


def _metadata_invalid(build):
    return (
        ~F.col("_source_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
        | ~F.col("_gold_build_id").eqNullSafe(F.lit(build.spec.build_id))
        | ~F.col("_gold_generated_at").eqNullSafe(
            F.lit(_utc(build.generated_at)).cast("timestamp")
        )
    )


def _validate_inputs(sources, build):
    """Reject ambiguous identities, inconsistent lifecycle evidence and parents."""
    context = _ctx(build, WORKFORCE_MODEL)
    employees, exits = sources["employees"], sources["employee_exits"]
    build.require_runtime(employees.sparkSession)
    for name, frame, types, unique in (
        (
            "employees",
            employees,
            {
                "employee_id": LongType(),
                "hire_date": DateType(),
                "termination_date": DateType(),
                "_batch_id": StringType(),
            },
            ("employee_id",),
        ),
        (
            "employee_exits",
            exits,
            {
                "exit_event_id": LongType(),
                "employee_id": LongType(),
                "exit_date": DateType(),
                "_batch_id": StringType(),
            },
            ("exit_event_id", "employee_id"),
        ),
    ):
        _gate(
            context,
            name + "_schema",
            int(
                len(frame.columns) != len(set(frame.columns))
                or any(
                    n not in frame.columns or frame.schema[n].dataType != t
                    for n, t in types.items()
                )
            ),
        )
        invalid = ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
        for name_in in types:
            if name_in != "termination_date":
                invalid = invalid | F.col(name_in).isNull()
            if name_in.endswith("_id") and name_in != "_batch_id":
                invalid = invalid | (F.col(name_in) <= 0)
        if name == "employees":
            invalid = invalid | (F.col("termination_date") < F.col("hire_date"))
        _gate(
            context,
            name + "_values",
            _invalid(frame, F.coalesce(invalid, F.lit(False))),
        )
        for key in unique:
            _gate(context, name + "_unique_" + key, _duplicates(frame, (key,)))
    evidence = employees.select("employee_id", "hire_date", "termination_date").join(
        exits.select("employee_id", "exit_date"), "employee_id", "full"
    )
    bad = (
        F.col("hire_date").isNull()
        | (
            F.col("exit_date").isNotNull()
            & ~F.col("exit_date").eqNullSafe(F.col("termination_date"))
        )
        | (
            (F.col("termination_date") <= F.lit(build.spec.source_cutoff))
            & F.col("exit_date").isNull()
        )
    )
    _gate(context, "exit_reconciliation", evidence.filter(bad).count())
    # Core dimensions and assignment history are authoritative inputs. Check
    # their contracts/build identities; never reconstruct their business state.
    for model in (
        "dim_date",
        "dim_employee",
        "dim_department",
        "dim_job_role",
        "dim_location",
        "dim_employee_assignment",
    ):
        parent = sources[model]
        ValidationReport(
            (validate_schema(parent.schema, _ctx(build, model)),)
        ).raise_for_failure()
        _gate(context, model + "_metadata", _invalid(parent, _metadata_invalid(build)))
        _gate(
            context, model + "_key", _duplicates(parent, GOLD_MODELS[model].primary_key)
        )
    validate_core_dimension(sources["dim_date"], "dim_date", build, {})


def _transform(sources, build):
    context = _ctx(build, WORKFORCE_MODEL)
    population = _population(
        sources["employees"], closed_months(sources["dim_date"], build)
    )
    assignment = (
        sources["dim_employee_assignment"].filter(~F.col("is_unknown")).alias("a")
    )
    resolved = (
        population.alias("p")
        .join(
            assignment,
            (F.col("p.employee_key") == F.col("a.employee_key"))
            & (F.col("a.valid_from_date") <= F.col("p.assignment_date"))
            & (F.col("p.assignment_date") < F.col("a.valid_to_exclusive")),
            "left",
        )
        .select("p.*", *[F.col("a." + key).alias(key) for key in ASSIGNMENT_FIELDS])
    )
    _gate(
        context,
        "required_assignment",
        resolved.filter(
            F.col("assignment_key").isNull() | (F.col("assignment_key") == "0")
        ).count(),
    )
    _gate(
        context,
        "assignment_unique_at_reference",
        _duplicates(resolved, ("employee_key", "snapshot_month_key")),
    )
    dates = sources["dim_date"].select(
        F.col("calendar_date").alias("assignment_date"),
        F.col("date_key").alias("assignment_date_key"),
    )
    resolved = resolved.join(dates, "assignment_date", "left")
    _gate(
        context,
        "assignment_date_foreign_key",
        resolved.filter(F.col("assignment_date_key").isNull()).count(),
    )
    real = resolved.select(
        "employee_key",
        "snapshot_month_key",
        F.year("snapshot_date").alias("reporting_year"),
        "assignment_date_key",
        *ASSIGNMENT_FIELDS,
        "headcount_eom",
        F.when(F.col("headcount_eom") == 1, F.datediff("snapshot_date", "hire_date"))
        .cast("int")
        .alias("tenure_days_eom"),
    )
    return _phase2_finish(real, WORKFORCE_MODEL, build)


def build_fact_workforce_monthly(
    sources: Mapping[str, DataFrame], build: DimensionBuild
) -> DataFrame:
    """Build and validate only workforce, using supplied Phase 2 history."""
    _validate_inputs(sources, build)
    output = _transform(sources, build)
    _validate_output(output, sources, build)
    return output


def monthly_reconciliation(output, sources, build):
    """Return all eligible months, including empty populations, without collecting."""
    months = closed_months(sources["dim_date"], build)
    # Independent predicates: do not use the fact's flag or transformation to
    # derive the expected closing population.
    e = sources["employees"]
    expected = (
        months.join(e, F.col("hire_date") <= F.col("snapshot_date"), "left")
        .groupBy("snapshot_month_key")
        .agg(
            F.sum(
                F.when(
                    F.col("employee_id").isNotNull()
                    & (
                        F.col("termination_date").isNull()
                        | (F.col("termination_date") >= F.col("month_start"))
                    ),
                    1,
                ).otherwise(0)
            ).alias("expected_participants"),
            F.sum(
                F.when(
                    F.col("employee_id").isNotNull()
                    & (
                        F.col("termination_date").isNull()
                        | (F.col("termination_date") > F.col("snapshot_date"))
                    ),
                    1,
                ).otherwise(0)
            ).alias("expected_closing"),
        )
    )
    actual = output.groupBy("snapshot_month_key").agg(
        F.count("employee_key").alias("fact_rows"),
        F.countDistinct("employee_key").alias("distinct_employees"),
        F.sum("headcount_eom").alias("headcount_sum"),
    )
    return (
        expected.join(actual, "snapshot_month_key", "full")
        .fillna(0)
        .withColumn(
            "non_closing_participant_rows", F.col("fact_rows") - F.col("headcount_sum")
        )
    )


def _validate_output(output, sources, build):
    context = _ctx(build, WORKFORCE_MODEL)
    results = [validate_schema(output.schema, context)]
    ValidationReport(tuple(results)).raise_for_failure()
    results.append(
        _gate(
            context,
            "employee_month_grain",
            _duplicates(output, ("employee_key", "snapshot_month_key")),
        )
    )
    invalid = _metadata_invalid(build) | ~F.col("_record_hash").eqNullSafe(
        spark_record_hash(output, WORKFORCE_MODEL)
    )
    for field in GOLD_MODELS[WORKFORCE_MODEL].fields:
        if not field.nullable:
            invalid = invalid | F.col(field.name).isNull()
    invalid = (
        invalid | ~F.col("headcount_eom").isin(0, 1) | (F.col("employee_key") <= 0)
    )
    invalid = invalid | (
        (F.col("headcount_eom") == 0) & F.col("tenure_days_eom").isNotNull()
    )
    invalid = invalid | (
        (F.col("headcount_eom") == 1)
        & (F.col("tenure_days_eom").isNull() | (F.col("tenure_days_eom") < 0))
    )
    results.append(_gate(context, "values_hash_metadata", _invalid(output, invalid)))
    for key, model, parent_key in (
        ("employee_key", "dim_employee", "employee_key"),
        ("manager_employee_key", "dim_employee", "employee_key"),
        ("department_key", "dim_department", "department_key"),
        ("job_role_key", "dim_job_role", "job_role_key"),
        ("location_key", "dim_location", "location_key"),
        ("snapshot_month_key", "dim_date", "date_key"),
        ("assignment_date_key", "dim_date", "date_key"),
    ):
        missing = (
            output.select(key)
            .join(sources[model].select(F.col(parent_key).alias(key)), key, "left_anti")
            .count()
        )
        results.append(_gate(context, key + "_foreign_key", missing))
    reconciliation = monthly_reconciliation(output, sources, build)
    results.append(
        _gate(
            context,
            "monthly_reconciliation",
            reconciliation.filter(
                (F.col("fact_rows") != F.col("distinct_employees"))
                | (F.col("fact_rows") != F.col("expected_participants"))
                | (F.col("headcount_sum") != F.col("expected_closing"))
            ).count(),
        )
    )
    return results


def validate_workforce_monthly(output, build, sources) -> ValidationReport:
    """Fail closed before publication; compare full business/metadata multisets.

    Reconstruction from authoritative assignments verifies overlap coverage,
    closed dates, reference dates, tenure, organisation and headcount per row.
    """
    _validate_inputs(sources, build)
    results = _validate_output(output, sources, build)
    expected = _transform(sources, build)
    results.append(
        _gate(
            _ctx(build, WORKFORCE_MODEL),
            "source_reconstruction",
            output.exceptAll(expected).count() + expected.exceptAll(output).count(),
        )
    )
    logging.getLogger(__name__).info(
        "Gold workforce validation | batch=%s build=%s cutoff=%s passed=True",
        build.spec.silver_batch_id,
        build.spec.build_id,
        build.spec.source_cutoff,
    )
    return ValidationReport(tuple(results))
