"""Phase 2 assignment-history and employee-movement transformations.

The functions consume explicitly supplied Silver-shaped DataFrames and the
already-built core dimensions.  They never discover data or publish storage.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark.gold.contracts import (
    GOLD_MODELS,
    UNKNOWN_ASSIGNMENT_KEY,
    unknown_business_row,
)
from spark.gold.dimension_validation import DimensionBuild, _gate, enforce_schema
from spark.gold.hashing import spark_record_hash, frame
from spark.gold.manifest import _utc
from spark.gold.validate import ValidationContext

PHASE2_MODELS = ("dim_employee_assignment", "fact_employee_movement")
MOVEMENT_TYPES = ("HIRE", "EXIT", "PROMOTION", "TRANSFER")


def _ctx(build: DimensionBuild, model: str) -> ValidationContext:
    return ValidationContext(
        model, build.spec.silver_batch_id, build.spec.build_id, build.generated_at
    )


def _require_columns(frame: DataFrame, names: tuple[str, ...], label: str) -> None:
    missing = sorted(set(names) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required Silver fields: {missing}")


def _validate_event_source(
    frame: DataFrame, dataset: str, build: DimensionBuild, fields: tuple[str, ...]
) -> None:
    _require_columns(frame, fields + ("_batch_id",), dataset)
    build.require_runtime(frame.sparkSession)
    _gate(
        _ctx(build, "dim_employee_assignment"),
        dataset + "_batch",
        frame.filter(
            ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
        ).count(),
    )
    _gate(
        _ctx(build, "dim_employee_assignment"),
        dataset + "_event_values",
        frame.filter(F.col(fields[0]).isNull() | F.col(fields[1]).isNull()).count(),
    )


def _phase2_finish(real: DataFrame, model: str, build: DimensionBuild) -> DataFrame:
    contract = GOLD_MODELS[model]
    schema = StructType([field.spark_field() for field in contract.business_fields])
    business = real.select(*schema.fieldNames())
    if contract.unknown_member:
        unknown_values = unknown_business_row(model)
        unknown = real.sparkSession.range(1).select(
            *[
                F.lit(unknown_values[f.name]).cast(f.dataType).alias(f.name)
                for f in schema
            ]
        )
        business = business.unionByName(unknown)
    result = (
        business.withColumn("_source_batch_id", F.lit(build.spec.silver_batch_id))
        .withColumn("_gold_build_id", F.lit(build.spec.build_id))
        .withColumn(
            "_gold_generated_at", F.lit(_utc(build.generated_at)).cast("timestamp")
        )
    )
    return enforce_schema(
        result.withColumn("_record_hash", spark_record_hash(result, model)).select(
            *[field.name for field in GOLD_MODELS[model].fields]
        ),
        model,
    )


def _spark_key(namespace, kind, *values):
    parts = [
        F.lit((frame(value).decode("utf-8")))
        for value in ("gold-key", "v1", namespace, kind)
    ]
    for value in values:
        encoded = F.col(value).cast("string") if isinstance(value, str) else value
        parts.append(
            F.concat(
                F.length(F.encode(encoded, "UTF-8")).cast("string"), F.lit(":"), encoded
            )
        )
    return F.sha2(F.concat(*parts), 256)


def _same_day_conflicts(events: DataFrame, build: DimensionBuild) -> None:
    context = _ctx(build, "dim_employee_assignment")
    for column in (
        "department_key",
        "job_role_key",
        "location_key",
        "manager_employee_key",
    ):
        conflict = (
            events.filter(F.col(column).isNotNull())
            .groupBy("employee_key", "event_date")
            .agg(F.countDistinct(column).alias("n"))
            .filter(F.col("n") > 1)
            .count()
        )
        _gate(context, "same_day_conflict_" + column, conflict)


def _reject_duplicates(
    frame: DataFrame, columns: tuple[str, ...], build: DimensionBuild, name: str
) -> None:
    duplicate_rows = frame.groupBy(*columns).count().filter(F.col("count") > 1).count()
    _gate(_ctx(build, "fact_employee_movement"), name, duplicate_rows)


def validate_assignment_intervals(
    assignments: DataFrame, employees: DataFrame, build: DimensionBuild
) -> None:
    """Fail closed on interval validity, employment bounds, overlap and grain."""
    real = assignments.filter(~F.col("is_unknown"))
    context = _ctx(build, "dim_employee_assignment")
    _gate(
        context,
        "assignment_key_unique",
        real.groupBy("assignment_key").count().filter("count > 1").count(),
    )
    _gate(
        context,
        "assignment_grain_unique",
        real.groupBy("employee_key", "valid_from_date")
        .count()
        .filter("count > 1")
        .count(),
    )
    _gate(
        context,
        "assignment_positive_intervals",
        real.filter(F.col("valid_from_date") >= F.col("valid_to_exclusive")).count(),
    )
    employee_dates = employees.select(
        F.col("employee_id").alias("employee_key"), "hire_date", "termination_date"
    )
    joined = real.join(employee_dates, "employee_key", "left")
    _gate(
        context,
        "assignment_employee_fk",
        joined.filter(F.col("hire_date").isNull()).count(),
    )
    _gate(
        context,
        "assignment_employment_bounds",
        joined.filter(
            (F.col("valid_from_date") < F.col("hire_date"))
            | (
                F.col("valid_to_exclusive")
                > F.date_add(
                    F.least(F.col("termination_date"), F.lit(build.spec.source_cutoff)),
                    1,
                )
            )
        ).count(),
    )
    w = Window.partitionBy("employee_key").orderBy("valid_from_date")
    overlap = (
        joined.withColumn("previous_end", F.lag("valid_to_exclusive").over(w))
        .filter(F.col("previous_end") > F.col("valid_from_date"))
        .count()
    )
    _gate(context, "assignment_no_overlap", overlap)


def build_dim_employee_assignment(
    employees: DataFrame,
    promotions: DataFrame,
    transfers: DataFrame,
    employee_exits: DataFrame,
    build: DimensionBuild,
) -> DataFrame:
    """Reconstruct bounded, half-open assignment intervals from lifecycle evidence."""
    _require_columns(
        employees,
        (
            "employee_id",
            "hire_date",
            "termination_date",
            "department_id",
            "role_id",
            "location_id",
            "manager_id",
            "_batch_id",
        ),
        "employees",
    )
    for frame, name, fields in (
        (
            promotions,
            "promotions",
            ("promotion_id", "employee_id", "promotion_date", "new_role_id"),
        ),
        (
            transfers,
            "transfers",
            (
                "transfer_id",
                "employee_id",
                "transfer_date",
                "new_department_id",
                "new_location_id",
                "new_manager_id",
            ),
        ),
        (
            employee_exits,
            "employee_exits",
            ("exit_event_id", "employee_id", "exit_date"),
        ),
    ):
        _require_columns(frame, fields + ("_batch_id",), name)
        build.require_runtime(frame.sparkSession)
        _gate(
            _ctx(build, "dim_employee_assignment"),
            name + "_batch",
            frame.filter(
                ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
            ).count(),
        )
        if name == "promotions":
            _reject_duplicates(frame, ("promotion_id",), build, "promotion_id_unique")
        elif name == "transfers":
            _reject_duplicates(frame, ("transfer_id",), build, "transfer_id_unique")
        elif name == "employee_exits":
            _reject_duplicates(frame, ("exit_event_id",), build, "exit_event_id_unique")

    base = employees.select(
        F.col("employee_id").alias("employee_key"),
        "hire_date",
        "termination_date",
        F.col("department_id").alias("department_key"),
        F.col("role_id").alias("job_role_key"),
        F.col("location_id").alias("location_key"),
        F.coalesce(F.col("manager_id"), F.lit(0)).alias("manager_employee_key"),
    )
    _gate(
        _ctx(build, "dim_employee_assignment"),
        "employee_positive",
        base.filter(F.col("employee_key") <= 0).count(),
    )
    transfer_events = transfers.select(
        F.col("employee_id").alias("employee_key"),
        F.col("transfer_date").alias("event_date"),
        F.col("new_department_id").alias("department_key"),
        F.lit(None).cast("long").alias("job_role_key"),
        F.col("new_location_id").alias("location_key"),
        F.coalesce(F.col("new_manager_id"), F.lit(0)).alias("manager_employee_key"),
    )
    promotion_events = promotions.select(
        F.col("employee_id").alias("employee_key"),
        F.col("promotion_date").alias("event_date"),
        *[
            F.lit(None).cast("long").alias(n)
            for n in ("department_key", "location_key", "manager_employee_key")
        ],
        F.col("new_role_id").alias("job_role_key"),
    )
    events = transfer_events.unionByName(promotion_events)
    _same_day_conflicts(events, build)
    events = events.groupBy("employee_key", "event_date").agg(
        *[
            F.max(column).alias(column)
            for column in (
                "department_key",
                "job_role_key",
                "location_key",
                "manager_employee_key",
            )
        ]
    )
    bounds = (
        base.select("employee_key", F.col("hire_date").alias("valid_from_date"))
        .unionByName(
            events.join(base.select("employee_key", "hire_date"), "employee_key")
            .select("employee_key", "event_date")
            .withColumnRenamed("event_date", "valid_from_date")
        )
        .dropDuplicates(("employee_key", "valid_from_date"))
    )
    bnd, bas, ev = bounds.alias("bnd"), base.alias("bas"), events.alias("ev")
    seeded = bnd.join(bas, F.col("bnd.employee_key") == F.col("bas.employee_key")).join(
        ev,
        (F.col("bnd.employee_key") == F.col("ev.employee_key"))
        & (F.col("bnd.valid_from_date") == F.col("ev.event_date")),
        "left",
    )
    seeded = seeded.select(
        F.col("bnd.employee_key").alias("employee_key"),
        F.col("bnd.valid_from_date").alias("valid_from_date"),
        F.col("bas.hire_date").alias("hire_date"),
        F.col("bas.termination_date").alias("termination_date"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.col("bas.department_key"),
        )
        .otherwise(F.col("ev.department_key"))
        .alias("department_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.col("bas.job_role_key"),
        )
        .otherwise(F.col("ev.job_role_key"))
        .alias("role_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.col("bas.location_key"),
        )
        .otherwise(F.col("ev.location_key"))
        .alias("location_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.col("bas.manager_employee_key"),
        )
        .otherwise(F.col("ev.manager_employee_key"))
        .alias("manager_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.lit("current-state-assumed"),
        )
        .otherwise(
            F.when(F.col("ev.department_key").isNotNull(), F.lit("event-derived"))
        )
        .alias("department_basis_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.lit("current-state-assumed"),
        )
        .otherwise(F.when(F.col("ev.job_role_key").isNotNull(), F.lit("event-derived")))
        .alias("role_basis_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.lit("current-state-assumed"),
        )
        .otherwise(F.when(F.col("ev.location_key").isNotNull(), F.lit("event-derived")))
        .alias("location_basis_value"),
        F.when(
            F.col("bnd.valid_from_date") == F.col("bas.hire_date"),
            F.lit("current-state-assumed"),
        )
        .otherwise(
            F.when(F.col("ev.manager_employee_key").isNotNull(), F.lit("event-derived"))
        )
        .alias("manager_basis_value"),
    )
    window = (
        Window.partitionBy("employee_key")
        .orderBy("valid_from_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    for name in ("department", "role", "location", "manager"):
        seeded = seeded.withColumn(
            name + "_key",
            F.last("{}_value".format(name), ignorenulls=True).over(window),
        ).withColumn(
            name + "_history_basis",
            F.last("{}_basis_value".format(name), ignorenulls=True).over(window),
        )
    result = seeded.withColumn(
        "next_boundary",
        F.lead("valid_from_date").over(
            Window.partitionBy("employee_key").orderBy("valid_from_date")
        ),
    )
    result = result.withColumn(
        "valid_to_exclusive",
        F.least(
            "next_boundary",
            F.date_add(F.least("termination_date", F.lit(build.spec.source_cutoff)), 1),
        ),
    )
    result = result.filter(
        (F.col("valid_from_date") < F.col("valid_to_exclusive"))
        & (F.col("valid_from_date") >= F.col("hire_date"))
        & (F.col("valid_from_date") <= F.lit(build.spec.source_cutoff))
    )
    real = (
        result.select(
            _spark_key(
                build.spec.source_namespace,
                "assignment",
                F.col("employee_key"),
                F.col("valid_from_date"),
            ).alias("assignment_key"),
            "employee_key",
            "valid_from_date",
            "valid_to_exclusive",
            "department_key",
            "role_key",
            "location_key",
            "manager_key",
            "department_history_basis",
            "role_history_basis",
            "location_history_basis",
            "manager_history_basis",
        )
        .withColumnRenamed("role_key", "job_role_key")
        .withColumnRenamed("manager_key", "manager_employee_key")
        .withColumn("is_unknown", F.lit(False))
    )
    # Optional manager references resolve only to actual source employees.
    managers = employees.select(
        F.col("employee_id").alias("_resolved_manager")
    ).distinct()
    real = (
        real.join(
            managers, real.manager_employee_key == managers._resolved_manager, "left"
        )
        .withColumn(
            "manager_employee_key",
            F.coalesce("_resolved_manager", F.lit(0).cast("long")),
        )
        .drop("_resolved_manager")
    )
    for key, basis in (
        ("department_key", "department_history_basis"),
        ("job_role_key", "role_history_basis"),
        ("location_key", "location_history_basis"),
        ("manager_employee_key", "manager_history_basis"),
    ):
        real = real.withColumn(
            basis,
            F.when(F.col(key).isNull() | (F.col(key) == 0), F.lit("unknown")).otherwise(
                F.col(basis)
            ),
        ).withColumn(key, F.coalesce(F.col(key), F.lit(0).cast("long")))
    output = _phase2_finish(real, "dim_employee_assignment", build)
    validate_assignment_intervals(output, employees, build)
    return output


def build_fact_employee_movement(
    employees: DataFrame,
    promotions: DataFrame,
    transfers: DataFrame,
    employee_exits: DataFrame,
    assignments: DataFrame,
    build: DimensionBuild,
) -> DataFrame:
    """Construct one deterministic row per approved lifecycle event."""
    for frame, name in (
        (employees, "employees"),
        (promotions, "promotions"),
        (transfers, "transfers"),
        (employee_exits, "employee_exits"),
    ):
        _require_columns(frame, ("_batch_id",), name)
        _gate(
            _ctx(build, "fact_employee_movement"),
            name + "_batch",
            frame.filter(
                ~F.col("_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
            ).count(),
        )
    _require_columns(
        assignments,
        tuple(f.name for f in GOLD_MODELS["dim_employee_assignment"].fields),
        "assignments",
    )

    employee_events = employees.select(
        F.lit("HIRE").alias("movement_type"),
        F.lit("employees").alias("source_dataset"),
        F.col("employee_id").alias("source_record_id"),
        F.col("employee_id").alias("employee_key"),
        F.col("hire_date").alias("event_date"),
        F.lit(None).cast("string").alias("exit_type"),
        F.lit(None).cast("boolean").alias("voluntary_flag"),
        F.lit(None).cast("boolean").alias("regrettable_flag"),
    )
    exit_events = employee_exits.select(
        F.lit("EXIT").alias("movement_type"),
        F.lit("employee_exits").alias("source_dataset"),
        F.col("exit_event_id").alias("source_record_id"),
        F.col("employee_id").alias("employee_key"),
        F.col("exit_date").alias("event_date"),
        "exit_type",
        "voluntary_flag",
        "regrettable_flag",
    )
    promotion_events = (
        promotions.select(
            F.lit("PROMOTION").alias("movement_type"),
            F.lit("promotions").alias("source_dataset"),
            F.col("promotion_id").alias("source_record_id"),
            F.col("employee_id").alias("employee_key"),
            F.col("promotion_date").alias("event_date"),
        )
        .withColumn("exit_type", F.lit(None).cast("string"))
        .withColumn("voluntary_flag", F.lit(None).cast("boolean"))
        .withColumn("regrettable_flag", F.lit(None).cast("boolean"))
    )
    transfer_events = (
        transfers.select(
            F.lit("TRANSFER").alias("movement_type"),
            F.lit("transfers").alias("source_dataset"),
            F.col("transfer_id").alias("source_record_id"),
            F.col("employee_id").alias("employee_key"),
            F.col("transfer_date").alias("event_date"),
        )
        .withColumn("exit_type", F.lit(None).cast("string"))
        .withColumn("voluntary_flag", F.lit(None).cast("boolean"))
        .withColumn("regrettable_flag", F.lit(None).cast("boolean"))
    )
    events = (
        employee_events.unionByName(exit_events)
        .unionByName(promotion_events)
        .unionByName(transfer_events)
    )
    _reject_duplicates(promotions, ("promotion_id",), build, "promotion_id_unique")
    _reject_duplicates(transfers, ("transfer_id",), build, "transfer_id_unique")
    _reject_duplicates(
        employee_exits, ("exit_event_id",), build, "exit_event_id_unique"
    )
    events = events.filter(
        (F.col("event_date") >= F.lit(build.spec.reporting_start))
        & (F.col("event_date") <= F.lit(build.spec.source_cutoff))
    )
    out = events.select(
        _spark_key(
            build.spec.source_namespace,
            "movement",
            F.col("movement_type"),
            F.col("source_record_id"),
        ).alias("movement_key"),
        "movement_type",
        "source_dataset",
        "source_record_id",
        "employee_key",
        "event_date",
        (
            F.year("event_date") * 10000
            + F.month("event_date") * 100
            + F.dayofmonth("event_date")
        )
        .cast("int")
        .alias("event_date_key"),
        F.lit(UNKNOWN_ASSIGNMENT_KEY).alias("before_assignment_key"),
        F.lit(UNKNOWN_ASSIGNMENT_KEY).alias("after_assignment_key"),
        *[
            F.lit(0).cast("long").alias(n)
            for n in (
                "before_department_key",
                "after_department_key",
                "before_job_role_key",
                "after_job_role_key",
                "before_location_key",
                "after_location_key",
                "before_manager_employee_key",
                "after_manager_employee_key",
            )
        ],
        F.lit(1).alias("event_count"),
        F.when(F.col("movement_type") == "HIRE", 1)
        .when(F.col("movement_type") == "EXIT", -1)
        .otherwise(0)
        .alias("headcount_delta"),
        "exit_type",
        "voluntary_flag",
        "regrettable_flag",
    )
    # Resolve assignment keys and dimensional FKs without multiplying events.
    real_assignments = assignments.filter(~F.col("is_unknown"))
    after = real_assignments.alias("after")
    candidates = out.alias("event").join(
        after,
        (F.col("event.employee_key") == F.col("after.employee_key"))
        & (F.col("after.valid_from_date") <= F.col("event.event_date"))
        & (F.col("event.event_date") < F.col("after.valid_to_exclusive")),
        "left",
    )
    latest = candidates.groupBy("event.movement_key").agg(
        F.max(
            F.struct(
                "after.valid_from_date",
                *[
                    F.col("after." + n)
                    for n in (
                        "assignment_key",
                        "department_key",
                        "job_role_key",
                        "location_key",
                        "manager_employee_key",
                    )
                ],
            )
        ).alias("a")
    )
    before_candidates = out.alias("event").join(
        real_assignments.alias("before"),
        (F.col("event.employee_key") == F.col("before.employee_key"))
        & (F.col("before.valid_from_date") < F.col("event.event_date")),
        "left",
    )
    before_latest = before_candidates.groupBy("event.movement_key").agg(
        F.max(
            F.struct(
                "before.valid_from_date",
                *[
                    F.col("before." + n)
                    for n in (
                        "assignment_key",
                        "department_key",
                        "job_role_key",
                        "location_key",
                        "manager_employee_key",
                    )
                ],
            )
        ).alias("b")
    )
    out = (
        out.join(latest, "movement_key", "left")
        .join(before_latest, "movement_key", "left")
        .select(
            *[F.col(n) for n in out.columns],
            F.when(F.col("movement_type") == "EXIT", F.lit(UNKNOWN_ASSIGNMENT_KEY))
            .otherwise(
                F.coalesce(F.col("a.assignment_key"), F.lit(UNKNOWN_ASSIGNMENT_KEY))
            )
            .alias("_after_assignment_key"),
            F.when(F.col("movement_type") == "EXIT", F.lit(0))
            .otherwise(F.coalesce(F.col("a.department_key"), F.lit(0)))
            .alias("_after_department_key"),
            F.when(F.col("movement_type") == "EXIT", F.lit(0))
            .otherwise(F.coalesce(F.col("a.job_role_key"), F.lit(0)))
            .alias("_after_job_role_key"),
            F.when(F.col("movement_type") == "EXIT", F.lit(0))
            .otherwise(F.coalesce(F.col("a.location_key"), F.lit(0)))
            .alias("_after_location_key"),
            F.when(F.col("movement_type") == "EXIT", F.lit(0))
            .otherwise(F.coalesce(F.col("a.manager_employee_key"), F.lit(0)))
            .alias("_after_manager_employee_key"),
            F.when(F.col("movement_type") == "HIRE", F.lit(UNKNOWN_ASSIGNMENT_KEY))
            .otherwise(
                F.coalesce(F.col("b.assignment_key"), F.lit(UNKNOWN_ASSIGNMENT_KEY))
            )
            .alias("_before_assignment_key"),
            F.when(F.col("movement_type") == "HIRE", F.lit(0))
            .otherwise(F.coalesce(F.col("b.department_key"), F.lit(0)))
            .alias("_before_department_key"),
            F.when(F.col("movement_type") == "HIRE", F.lit(0))
            .otherwise(F.coalesce(F.col("b.job_role_key"), F.lit(0)))
            .alias("_before_job_role_key"),
            F.when(F.col("movement_type") == "HIRE", F.lit(0))
            .otherwise(F.coalesce(F.col("b.location_key"), F.lit(0)))
            .alias("_before_location_key"),
            F.when(F.col("movement_type") == "HIRE", F.lit(0))
            .otherwise(F.coalesce(F.col("b.manager_employee_key"), F.lit(0)))
            .alias("_before_manager_employee_key"),
        )
    )
    out = (
        out.drop(
            "before_assignment_key",
            "before_department_key",
            "before_job_role_key",
            "before_location_key",
            "before_manager_employee_key",
            "after_assignment_key",
            "after_department_key",
            "after_job_role_key",
            "after_location_key",
            "after_manager_employee_key",
        )
        .withColumnRenamed("_after_assignment_key", "after_assignment_key")
        .withColumnRenamed("_after_department_key", "after_department_key")
        .withColumnRenamed("_after_job_role_key", "after_job_role_key")
        .withColumnRenamed("_after_location_key", "after_location_key")
        .withColumnRenamed("_after_manager_employee_key", "after_manager_employee_key")
        .withColumnRenamed("_before_assignment_key", "before_assignment_key")
        .withColumnRenamed("_before_department_key", "before_department_key")
        .withColumnRenamed("_before_job_role_key", "before_job_role_key")
        .withColumnRenamed("_before_location_key", "before_location_key")
        .withColumnRenamed(
            "_before_manager_employee_key", "before_manager_employee_key"
        )
    )
    out = out.drop("event_date")
    _reject_duplicates(out, ("movement_key",), build, "movement_key_unique")
    return _phase2_finish(out, "fact_employee_movement", build)


def validate_phase2_output(output, model, build, sources):
    """Gate local publication against supplied Silver evidence and reference keys.

    Required positive parent references must exist; genuinely unknown attributes
    use the reserved zero member. Rebuild comparison also verifies source
    coverage, metadata, hashes, and before/after semantics without repairing data.
    """
    from spark.gold.validate import ValidationReport, validate_schema

    if model not in PHASE2_MODELS:
        raise ValueError("Not a Phase 2 model")
    context = _ctx(build, model)
    schema_result = validate_schema(output.schema, context)
    ValidationReport((schema_result,)).raise_for_failure()
    inputs = tuple(
        sources[n] for n in ("employees", "promotions", "transfers", "employee_exits")
    )
    assignments = build_dim_employee_assignment(*inputs, build)
    for key, dataset, source_key in (
        ("department_key", "departments", "department_id"),
        ("job_role_key", "job_roles", "role_id"),
        ("location_key", "locations", "location_id"),
        ("manager_employee_key", "employees", "employee_id"),
    ):
        missing = (
            assignments.filter(F.col(key) != 0)
            .select(key)
            .join(
                sources[dataset].select(F.col(source_key).alias(key)), key, "left_anti"
            )
            .count()
        )
        _gate(context, key + "_foreign_key", missing)
    expected = (
        assignments
        if model == "dim_employee_assignment"
        else (build_fact_employee_movement(*inputs, assignments, build))
    )
    differences = (
        output.exceptAll(expected).count() + expected.exceptAll(output).count()
    )
    equality = _gate(context, "source_reconstruction", differences)
    return ValidationReport((schema_result, equality))
