"""Exactly five core Gold dimensions from explicitly supplied Silver DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from spark.gold.contracts import GOLD_MODELS, unknown_business_row
from spark.gold.hashing import spark_record_hash
from spark.gold.manifest import _utc
from spark.gold.dimension_validation import (
    DimensionBuild,
    date_key_expression,
    enforce_schema,
    validate_core_dimension,
    validate_source,
)


def _finish(real: DataFrame, model: str, build: DimensionBuild) -> DataFrame:
    """Attach the shared reserved member and metadata using the central contract."""
    fields = GOLD_MODELS[model].business_fields
    schema = StructType([field.spark_field() for field in fields])
    values = unknown_business_row(model)
    # One literal row is independent of Python workers and source partitions.
    unknown = real.sparkSession.range(1).select(
        *[
            F.lit(values[field.name]).cast(field.dataType).alias(field.name)
            for field in schema
        ]
    )
    business = (
        real.withColumn("is_unknown", F.lit(False))
        .select(*schema.fieldNames())
        .unionByName(unknown)
    )
    result = (
        business.withColumn("_source_batch_id", F.lit(build.spec.silver_batch_id))
        .withColumn("_gold_build_id", F.lit(build.spec.build_id))
        .withColumn(
            "_gold_generated_at", F.lit(_utc(build.generated_at)).cast("timestamp")
        )
    )
    result = result.withColumn("_record_hash", spark_record_hash(result, model))
    return enforce_schema(result, model)


def build_dim_date(spark: SparkSession, build: DimensionBuild) -> DataFrame:
    """Generate the declared inclusive calendar without driver-side date loops."""
    build.require_runtime(spark)
    spec = build.spec
    count = (spec.source_cutoff - spec.reporting_start).days + 1
    real = spark.range(count).select(
        F.date_add(F.lit(spec.reporting_start), F.col("id").cast("int")).alias(
            "calendar_date"
        )
    )
    day = F.col("calendar_date")
    iso_day = F.pmod(F.dayofweek(day) + 5, F.lit(7)) + 1
    real = real.select(
        date_key_expression(day).alias("date_key"),
        day,
        F.dayofmonth(day).alias("day_of_month"),
        iso_day.alias("iso_day_of_week"),
        F.date_format(day, "EEEE").alias("day_name"),
        F.weekofyear(day).alias("iso_week_number"),
        F.year(F.date_add(day, 4 - iso_day)).alias("iso_week_year"),
        F.month(day).alias("month_number"),
        F.date_format(day, "MMMM").alias("month_name"),
        (F.year(day) * 100 + F.month(day)).alias("year_month"),
        F.trunc(day, "month").alias("month_start_date"),
        F.last_day(day).alias("month_end_date"),
        F.quarter(day).alias("calendar_quarter"),
        F.year(day).alias("calendar_year"),
        (iso_day >= 6).alias("is_weekend"),
    )
    result = _finish(real, "dim_date", build)
    validate_core_dimension(result, "dim_date", build, {})
    return result


def build_dim_employee(
    employees: DataFrame, dim_date: DataFrame, build: DimensionBuild
) -> DataFrame:
    validate_source(employees, "employees", build, "dim_employee")
    real = employees.select(
        F.col("employee_id").alias("employee_key"),
        "employee_number",
        date_key_expression(F.col("hire_date")).alias("hire_date_key"),
        F.coalesce(date_key_expression(F.col("termination_date")), F.lit(0)).alias(
            "termination_date_key"
        ),
        F.col("employment_status").alias("current_employment_status"),
        F.col("employment_type").alias("current_employment_type"),
    )
    result = _finish(real, "dim_employee", build)
    validate_core_dimension(
        result, "dim_employee", build, {"employees": employees}, dim_date=dim_date
    )
    return result


def build_dim_department(
    departments: DataFrame, business_units: DataFrame, build: DimensionBuild
) -> DataFrame:
    # Check parent uniqueness before joining: duplicates must never cause fan-out.
    validate_source(departments, "departments", build, "dim_department")
    validate_source(business_units, "business_units", build, "dim_department")
    real = departments.join(
        business_units.select("business_unit_id", "unit_name"),
        "business_unit_id",
        "left",
    ).select(
        F.col("department_id").alias("department_key"),
        "department_name",
        "cost_center",
        "business_unit_id",
        F.col("unit_name").alias("business_unit_name"),
    )
    # Orphans fail as missing required hierarchy values before hashing.
    from spark.gold.dimension_validation import _gate

    _gate(
        build.context("dim_department"),
        "business_unit_relationship",
        real.filter(F.col("business_unit_name").isNull()).count(),
    )
    result = _finish(real, "dim_department", build)
    validate_core_dimension(
        result,
        "dim_department",
        build,
        {"departments": departments, "business_units": business_units},
    )
    return result


def build_dim_location(locations: DataFrame, build: DimensionBuild) -> DataFrame:
    validate_source(locations, "locations", build, "dim_location")
    real = locations.select(
        F.col("location_id").alias("location_key"),
        "office_name",
        "city",
        "country",
        "timezone",
    )
    result = _finish(real, "dim_location", build)
    validate_core_dimension(result, "dim_location", build, {"locations": locations})
    return result


def build_dim_job_role(job_roles: DataFrame, build: DimensionBuild) -> DataFrame:
    validate_source(job_roles, "job_roles", build, "dim_job_role")
    real = job_roles.select(
        F.col("role_id").alias("job_role_key"), "role_name", "grade"
    )
    result = _finish(real, "dim_job_role", build)
    validate_core_dimension(result, "dim_job_role", build, {"job_roles": job_roles})
    return result
