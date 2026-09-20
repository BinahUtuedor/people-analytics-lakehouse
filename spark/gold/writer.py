"""Local-only immutable Gold Parquet proof; no release publication or CLI."""

from __future__ import annotations

from pathlib import Path
from collections.abc import Mapping

from pyspark.sql import DataFrame

from spark.gold.dimension_validation import (
    DimensionBuild,
    enforce_schema,
    validate_core_dimension,
    _gate,
)
from spark.gold.manifest import ExistingOutputError
from spark.gold.contracts import get_gold_schema
from spark.gold.validate import ValidationReport
from spark.gold.workforce_history import PHASE2_MODELS, _ctx, validate_phase2_output
from spark.gold.workforce_monthly import WORKFORCE_MODEL, validate_workforce_monthly
from spark.gold.payroll import PAYROLL_MODEL, validate_payroll
from spark.gold.attendance import ATTENDANCE_MODEL, validate_attendance


def _context(build, model):
    return (
        _ctx(build, model)
        if model
        in PHASE2_MODELS
        + (
            WORKFORCE_MODEL,
            PAYROLL_MODEL,
            ATTENDANCE_MODEL,
        )
        else build.context(model)
    )


def _validate(frame, model, build, sources, *, dim_date=None):
    if model == PAYROLL_MODEL:
        return validate_payroll(frame, build, sources)
    if model == ATTENDANCE_MODEL:
        return validate_attendance(frame, build, sources)
    if model == WORKFORCE_MODEL:
        return validate_workforce_monthly(frame, build, sources)
    if model in PHASE2_MODELS:
        return validate_phase2_output(frame, model, build, sources)
    return validate_core_dimension(frame, model, build, sources, dim_date=dim_date)


def local_dimension_path(root: str | Path, model: str, build: DimensionBuild) -> Path:
    """Accept native local roots only; reject URI, UNC and glob input before IO."""
    _context(build, model)
    text = str(root)
    if (
        not text
        or "://" in text
        or text.startswith(("//", "\\\\"))
        or any(c in text for c in "*?[]")
    ):
        raise ValueError("An explicit native local directory is required")
    # Reject scheme-like relative strings such as s3:bucket as well as URI forms.
    path = Path(root)
    if ":" in text and not (path.drive and len(path.drive) == 2):
        raise ValueError("Storage schemes are not supported")
    base = path.resolve() / "gold" / model
    if model in (PAYROLL_MODEL, ATTENDANCE_MODEL):
        # Exclusive local claim isolates even an empty or partially written build.
        # Parquet itself follows the approved year-first layout, below.
        base = base / "_build_claims"
    return base / ("build_id=" + build.spec.build_id)


def local_payroll_partition_path(root, build, reporting_year):
    """Native local payroll leaf; reference/business dates are stored in the files."""
    if type(reporting_year) is not int or not 1 <= reporting_year <= 9999:
        raise ValueError("Invalid payroll reporting year")
    claim = local_dimension_path(root, PAYROLL_MODEL, build)
    return claim.parent.parent / f"reporting_year={reporting_year}" / claim.name


def local_attendance_partition_path(root, build, reporting_year):
    """Native local attendance leaf using the shared immutable layout."""
    if type(reporting_year) is not int or not 1 <= reporting_year <= 9999:
        raise ValueError("Invalid attendance reporting year")
    claim = local_dimension_path(root, ATTENDANCE_MODEL, build)
    return claim.parent.parent / f"reporting_year={reporting_year}" / claim.name


def _payroll_paths(frame, root, build):
    # At most 9999 calendar years, never payroll rows, reach the driver.
    return [
        (
            row.reporting_year,
            local_payroll_partition_path(root, build, row.reporting_year),
        )
        for row in frame.select("reporting_year")
        .distinct()
        .orderBy("reporting_year")
        .collect()
    ]


def _partition_paths(frame, root, model, build):
    path_builder = (
        local_payroll_partition_path
        if model == PAYROLL_MODEL
        else local_attendance_partition_path
    )
    return [
        (row.reporting_year, path_builder(root, build, row.reporting_year))
        for row in frame.select("reporting_year")
        .distinct()
        .orderBy("reporting_year")
        .collect()
    ]


def _existing_payroll_paths(root, build):
    claim = local_dimension_path(root, PAYROLL_MODEL, build)
    return set(claim.parent.parent.glob("reporting_year=*/" + claim.name))


def _existing_partition_paths(root, model, build):
    claim = local_dimension_path(root, model, build)
    return set(claim.parent.parent.glob("reporting_year=*/" + claim.name))


def verify_local_dimension(
    expected: DataFrame,
    root: str | Path,
    model: str,
    build: DimensionBuild,
    sources: Mapping[str, DataFrame],
    *,
    dim_date: DataFrame | None = None,
) -> tuple[DataFrame, ValidationReport]:
    """Read only; missing/changed output fails without creating or repairing it.

    Spark Parquet readers widen nullability. Verify names/types and required
    values, then restore the exact logical schema. Compare all rows (including
    metadata) as multisets; never infer acceptance from a _SUCCESS file.
    """
    path = local_dimension_path(root, model, build)
    if not path.is_dir():
        raise ExistingOutputError(
            "Missing dimension output; verification cannot create it"
        )
    _validate(expected, model, build, sources, dim_date=dim_date)
    if model in (PAYROLL_MODEL, ATTENDANCE_MODEL):
        paths = _partition_paths(expected, root, model, build)
        if _existing_partition_paths(root, model, build) != {p for _, p in paths}:
            raise ExistingOutputError("Partition inventory differs from expected build")
        actual = None
        from pyspark.sql import functions as F

        for year, leaf in paths:
            # Recursive lookup disables Hive inference of build_id. All logical
            # contract fields, including reporting_year, remain in Parquet.
            part = (
                expected.sparkSession.read.option("mergeSchema", "true")
                .option("recursiveFileLookup", "true")
                .parquet(str(leaf))
            )
            schema = get_gold_schema(model)
            if part.columns != schema.fieldNames() or any(
                part.schema[f.name].dataType != f.dataType for f in schema
            ):
                raise ValueError(
                    "Physical Gold schema differs from the approved contract"
                )
            _gate(
                _context(build, model),
                "physical_partition_year",
                part.filter(~F.col("reporting_year").eqNullSafe(F.lit(year))).count(),
            )
            actual = part if actual is None else actual.unionByName(part)
        if actual is None:
            actual = expected.sparkSession.createDataFrame([], get_gold_schema(model))
    else:
        actual = expected.sparkSession.read.option("mergeSchema", "true").parquet(
            str(path)
        )
    schema = get_gold_schema(model)
    # Partition columns are discovered after file columns by Parquet readers.
    # Restore the contract order only after checking the complete column set.
    if model == WORKFORCE_MODEL and set(actual.columns) == set(schema.fieldNames()):
        actual = actual.select(*schema.fieldNames())
    if actual.columns != schema.fieldNames() or any(
        actual.schema[f.name].dataType != f.dataType for f in schema
    ):
        raise ValueError("Physical Gold schema differs from the approved contract")
    # Check real data before restoring nullable flags from physical Parquet.
    from functools import reduce
    from operator import or_
    from pyspark.sql import functions as F

    required = reduce(or_, (F.col(f.name).isNull() for f in schema if not f.nullable))
    _gate(
        _context(build, model),
        "physical_required_values",
        actual.filter(required).count(),
    )
    actual = enforce_schema(actual, model)
    report = _validate(actual, model, build, sources, dim_date=dim_date)
    differences = (
        actual.exceptAll(expected).count() + expected.exceptAll(actual).count()
    )
    equality = _gate(_context(build, model), "physical_readback_equality", differences)
    return actual, ValidationReport(report.results + (equality,))


def write_local_dimension(
    frame: DataFrame,
    root: str | Path,
    model: str,
    build: DimensionBuild,
    sources: Mapping[str, DataFrame],
    *,
    dim_date: DataFrame | None = None,
) -> tuple[DataFrame, ValidationReport]:
    """Validate, write with errorifexists, and verify; never append/delete/overwrite.

    A failed/partial directory remains for explicit operator resolution. This is
    a local proof for one implemented Gold model, not an accepted ten-model release.
    """
    path = local_dimension_path(root, model, build)
    if path.exists() or (
        model in (PAYROLL_MODEL, ATTENDANCE_MODEL)
        and _existing_partition_paths(root, model, build)
    ):
        raise ExistingOutputError("Gold dimension destination already exists")
    _validate(frame, model, build, sources, dim_date=dim_date)
    if model in (PAYROLL_MODEL, ATTENDANCE_MODEL):
        paths = _partition_paths(frame, root, model, build)
        try:
            path.mkdir(parents=True, exist_ok=False)
        except FileExistsError as error:
            raise ExistingOutputError("Gold payroll build already claimed") from error
        from pyspark.sql import functions as F

        for year, leaf in paths:
            frame.filter(F.col("reporting_year") == year).write.mode(
                "errorifexists"
            ).option("compression", "snappy").parquet(str(leaf))
        return verify_local_dimension(frame, root, model, build, sources)
    try:
        writer = frame.write.mode("errorifexists").option("compression", "snappy")
        if model == WORKFORCE_MODEL:
            writer = writer.partitionBy("reporting_year")
        writer.parquet(str(path))
    except Exception as error:
        if getattr(error, "getErrorClass", lambda: None)() == "PATH_ALREADY_EXISTS":
            raise ExistingOutputError(
                "Gold dimension destination already exists"
            ) from error
        raise
    return verify_local_dimension(frame, root, model, build, sources, dim_date=dim_date)
