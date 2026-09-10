"""Publication-gate validation for Silver datasets."""

from __future__ import annotations
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_METADATA = {
    "_batch_id",
    "_extraction_date",
    "_source_file",
    "_record_hash",
    "_silver_transformed_at",
}


class SilverValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class SilverValidationResult:
    table_name: str
    batch_id: str
    bronze_count: int
    silver_count: int
    duplicates_removed: int
    passed: bool = True


def validate_silver(
    bronze: DataFrame, silver: DataFrame, table_name: str, batch_id: str
) -> SilverValidationResult:
    errors: list[str] = []
    missing = REQUIRED_METADATA - set(silver.columns)
    if missing:
        errors.append(f"Silver is missing metadata columns: {sorted(missing)}")
    if (
        "_batch_id" in silver.columns
        and silver.filter(F.col("_batch_id") != batch_id).limit(1).count()
    ):
        errors.append("_batch_id is inconsistent with requested batch.")
    if (
        "_record_hash" in silver.columns
        and silver.groupBy("_record_hash").count().filter("count > 1").limit(1).count()
    ):
        errors.append("Silver contains duplicate record hashes.")
    if (
        "_source_file" in silver.columns
        and silver.filter(
            F.col("_source_file").isNull() | (F.length(F.trim("_source_file")) == 0)
        )
        .limit(1)
        .count()
    ):
        errors.append("_source_file is null or empty.")
    bronze_count, silver_count = bronze.count(), silver.count()
    if silver_count > bronze_count:
        errors.append("Silver count exceeds Bronze count.")
    if errors:
        raise SilverValidationError(" | ".join(errors))
    return SilverValidationResult(
        table_name, batch_id, bronze_count, silver_count, bronze_count - silver_count
    )


def validate_employee_references(
    dataframe: DataFrame, employees: DataFrame, table_name: str
) -> None:
    if (
        table_name != "employees"
        and "employee_id" in dataframe.columns
        and dataframe.join(
            employees.select("employee_id").dropDuplicates(), "employee_id", "left_anti"
        )
        .limit(1)
        .count()
    ):
        raise SilverValidationError(f"{table_name} contains orphan employee_id values.")
