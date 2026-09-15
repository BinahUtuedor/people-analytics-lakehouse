"""Local-only immutable dimension Parquet proof; no release publication or CLI."""

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


def local_dimension_path(root: str | Path, model: str, build: DimensionBuild) -> Path:
    """Accept native local roots only; reject URI, UNC and glob input before IO."""
    build.context(model)
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
    return path.resolve() / "gold" / model / ("build_id=" + build.spec.build_id)


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
    validate_core_dimension(expected, model, build, sources, dim_date=dim_date)
    actual = expected.sparkSession.read.option("mergeSchema", "true").parquet(str(path))
    schema = get_gold_schema(model)
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
        build.context(model),
        "physical_required_values",
        actual.filter(required).count(),
    )
    actual = enforce_schema(actual, model)
    report = validate_core_dimension(actual, model, build, sources, dim_date=dim_date)
    differences = (
        actual.exceptAll(expected).count() + expected.exceptAll(actual).count()
    )
    equality = _gate(build.context(model), "physical_readback_equality", differences)
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
    a local proof for one core dimension, not an accepted ten-model release.
    """
    path = local_dimension_path(root, model, build)
    if path.exists():
        raise ExistingOutputError("Gold dimension destination already exists")
    validate_core_dimension(frame, model, build, sources, dim_date=dim_date)
    try:
        frame.write.mode("errorifexists").option("compression", "snappy").parquet(
            str(path)
        )
    except Exception as error:
        if getattr(error, "getErrorClass", lambda: None)() == "PATH_ALREADY_EXISTS":
            raise ExistingOutputError(
                "Gold dimension destination already exists"
            ) from error
        raise
    return verify_local_dimension(frame, root, model, build, sources, dim_date=dim_date)
