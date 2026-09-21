"""Cross-model Gold MVP validation and deterministic release evidence.

This module composes the approved model contracts. It does not build facts,
publish data, or alter the immutable writer; callers provide already-built Gold
DataFrames and receive fail-closed integration evidence.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
import json

from pyspark.sql import DataFrame, functions as F

from spark.gold.contracts import GOLD_MODELS
from spark.gold.manifest import (
    ModelInventory,
    ReleaseManifest,
    ReleaseStatus,
)

MVP_MODELS = tuple(GOLD_MODELS)
FACT_MODELS = (
    "fact_employee_movement",
    "fact_workforce_monthly",
    "fact_payroll",
    "fact_attendance",
)


def _require_inventory(frames: Mapping[str, DataFrame]) -> None:
    if set(frames) != set(MVP_MODELS):
        missing = sorted(set(MVP_MODELS) - set(frames))
        unexpected = sorted(set(frames) - set(MVP_MODELS))
        raise ValueError(
            f"Gold MVP inventory mismatch; missing={missing}, unexpected={unexpected}"
        )


def _missing_positive(
    frame: DataFrame, child: str, parent: DataFrame, parent_key: str
) -> int:
    return (
        frame.filter(F.col(child) > 0)
        .select(F.col(child).alias("_key"))
        .join(parent.select(F.col(parent_key).alias("_key")), "_key", "left_anti")
        .count()
    )


def cross_model_referential_integrity(
    frames: Mapping[str, DataFrame],
) -> dict[str, int]:
    """Validate positive employee, organisation, manager, and assignment keys."""
    _require_inventory(frames)
    employee = frames["dim_employee"]
    checks: dict[str, int] = {}
    for model in (
        "dim_employee_assignment",
        "fact_employee_movement",
        "fact_workforce_monthly",
        "fact_payroll",
        "fact_attendance",
    ):
        checks[f"{model}.employee_key"] = _missing_positive(
            frames[model], "employee_key", employee, "employee_key"
        )
    for model in ("fact_workforce_monthly", "fact_payroll", "fact_attendance"):
        for key, parent in (
            ("department_key", "dim_department"),
            ("job_role_key", "dim_job_role"),
            ("location_key", "dim_location"),
        ):
            checks[f"{model}.{key}"] = _missing_positive(
                frames[model], key, frames[parent], key
            )
        checks[f"{model}.manager_employee_key"] = _missing_positive(
            frames[model], "manager_employee_key", employee, "employee_key"
        )
        checks[f"{model}.assignment_key"] = (
            frames[model]
            .filter(F.col("assignment_key") != "0")
            .select("assignment_key")
            .join(
                frames["dim_employee_assignment"].select("assignment_key"),
                "assignment_key",
                "left_anti",
            )
            .count()
        )
    movement = frames["fact_employee_movement"]
    for key in (
        "before_manager_employee_key",
        "after_manager_employee_key",
    ):
        checks[f"fact_employee_movement.{key}"] = _missing_positive(
            movement, key, employee, "employee_key"
        )
    for key in ("before_assignment_key", "after_assignment_key"):
        checks[f"fact_employee_movement.{key}"] = (
            movement.filter(F.col(key) != "0")
            .select(F.col(key).alias("_key"))
            .join(
                frames["dim_employee_assignment"].select(
                    F.col("assignment_key").alias("_key")
                ),
                "_key",
                "left_anti",
            )
            .count()
        )
    return checks


def cross_model_date_integrity(frames: Mapping[str, DataFrame]) -> dict[str, int]:
    """Validate every fact date key against the shared date dimension."""
    _require_inventory(frames)
    dates = frames["dim_date"].select(F.col("date_key").alias("_key"))
    checks: dict[str, int] = {}
    for model, fields in (
        ("fact_employee_movement", ("event_date_key",)),
        ("fact_workforce_monthly", ("snapshot_month_key", "assignment_date_key")),
        (
            "fact_payroll",
            ("pay_period_start_key", "pay_period_end_key", "payroll_month_key"),
        ),
        ("fact_attendance", ("work_date_key",)),
    ):
        for field in fields:
            checks[f"{model}.{field}"] = (
                frames[model]
                .select(F.col(field).alias("_key"))
                .join(dates, "_key", "left_anti")
                .count()
            )
    return checks


def metadata_consistency(frames: Mapping[str, DataFrame], build) -> dict[str, int]:
    """Check shared build metadata without depending on physical row order."""
    _require_inventory(frames)
    result: dict[str, int] = {}
    for model, frame in frames.items():
        result[f"{model}.source_batch"] = frame.filter(
            ~F.col("_source_batch_id").eqNullSafe(F.lit(build.spec.silver_batch_id))
        ).count()
        result[f"{model}.build_id"] = frame.filter(
            ~F.col("_gold_build_id").eqNullSafe(F.lit(build.spec.build_id))
        ).count()
        result[f"{model}.generated_at"] = frame.filter(
            ~F.col("_gold_generated_at").eqNullSafe(F.lit(build.generated_at))
        ).count()
    return result


def validate_mvp_frames(frames: Mapping[str, DataFrame], build) -> dict[str, object]:
    """Run the shared fail-closed integration checks for one logical MVP build."""
    refs = cross_model_referential_integrity(frames)
    dates = cross_model_date_integrity(frames)
    metadata = metadata_consistency(frames, build)
    duplicates = uniqueness_checks(frames)
    failed = {
        **{f"reference:{k}": v for k, v in refs.items() if v},
        **{f"date:{k}": v for k, v in dates.items() if v},
        **{f"metadata:{k}": v for k, v in metadata.items() if v},
        **{f"duplicate:{k}": v for k, v in duplicates.items() if v},
    }
    return {
        "passed": not failed,
        "failed_checks": failed,
        "references": refs,
        "dates": dates,
        "metadata": metadata,
        "duplicates": duplicates,
        "report": release_reconciliation_report(frames),
    }


def uniqueness_checks(frames: Mapping[str, DataFrame]) -> dict[str, int]:
    """Return duplicate row counts for every approved primary/additional key."""
    _require_inventory(frames)
    result: dict[str, int] = {}
    for model, contract in GOLD_MODELS.items():
        for key in (contract.primary_key,) + contract.additional_unique:
            result[f"{model}:{','.join(key)}"] = (
                frames[model].groupBy(*key).count().filter(F.col("count") > 1).count()
            )
    return result


def _content_fingerprint(frame: DataFrame) -> str:
    """Create a deterministic bounded aggregate fingerprint without row collect."""
    hashes = frame.select(F.col("_record_hash").alias("h"))
    summary = hashes.agg(
        F.count("h").alias("rows"),
        F.sum(F.xxhash64("h").cast("decimal(38,0)")).alias("hash_sum"),
        F.min("h").alias("hash_min"),
        F.max("h").alias("hash_max"),
    ).first()
    payload = json.dumps(
        [
            str(summary.rows),
            str(summary.hash_sum),
            str(summary.hash_min),
            str(summary.hash_max),
        ],
        separators=(",", ":"),
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def release_reconciliation_report(
    frames: Mapping[str, DataFrame],
) -> tuple[dict[str, object], ...]:
    """Produce per-model counts, keys, duplicate and content evidence."""
    _require_inventory(frames)
    duplicates = uniqueness_checks(frames)
    rows = []
    for model, contract in GOLD_MODELS.items():
        frame = frames[model]
        rows.append(
            {
                "model": model,
                "row_count": frame.count(),
                "primary_key_count": frame.select(*contract.primary_key)
                .distinct()
                .count(),
                "duplicate_count": sum(
                    duplicates[f"{model}:{','.join(key)}"]
                    for key in (contract.primary_key,) + contract.additional_unique
                ),
                "content_fingerprint": _content_fingerprint(frame),
                "foreign_key_status": "pending",
                "reconciliation_status": "pending",
                "physical_status": "pending",
            }
        )
    return tuple(rows)


def build_release_manifest(
    frames: Mapping[str, DataFrame],
    build,
    generated_at: datetime,
    *,
    status: ReleaseStatus = ReleaseStatus.BUILDING,
    verification_outcomes: Mapping[str, bool] | None = None,
    reconciliation_metrics: Mapping[str, str] | None = None,
) -> ReleaseManifest:
    """Build the exact ten-model manifest from a supplied logical MVP build."""
    _require_inventory(frames)
    reports = release_reconciliation_report(frames)
    report_by_model = {r["model"]: r for r in reports}
    inventories = []
    for model, contract in GOLD_MODELS.items():
        frame = frames[model]
        if contract.partition_fields:
            partitions = tuple(
                tuple((field, int(row[field])) for field in contract.partition_fields)
                for row in frame.select(*contract.partition_fields).distinct().collect()
            )
        else:
            partitions = ((),)
        inventories.append(
            ModelInventory(
                model,
                partitions,
                report_by_model[model]["row_count"],
                report_by_model[model]["content_fingerprint"],
            )
        )
    return ReleaseManifest(
        build.spec,
        generated_at,
        tuple(inventories),
        status=status,
        verification_outcomes=dict(verification_outcomes or {}),
        reconciliation_metrics=dict(reconciliation_metrics or {}),
    )
