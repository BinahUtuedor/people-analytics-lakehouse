"""Small Silver-shaped literals and one local Spark runtime for Gold tests."""

from dataclasses import replace
from datetime import date, datetime, timezone
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType

from spark.gold.dimension_validation import DimensionBuild
from spark.gold.transform import (
    build_dim_date,
    build_dim_employee,
    build_dim_department,
    build_dim_location,
    build_dim_job_role,
)
from tests.spark.bronze.spark_test_case import SparkTestCase
from tests.spark.gold.test_manifest import spec


class CoreDimensionTestCase(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        cls.previous_python = {
            name: os.environ.get(name)
            for name in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")
        }
        for name in cls.previous_python:
            os.environ[name] = sys.executable
        super().setUpClass()
        cls.spark.conf.set("spark.sql.ansi.enabled", "true")
        cls.spark.conf.set("spark.sql.caseSensitive", "true")
        cls.build = DimensionBuild(
            replace(
                spec(),
                reporting_start=date(2023, 12, 29),
                source_cutoff=date(2024, 3, 1),
            ),
            datetime(2024, 3, 2, tzinfo=timezone.utc),
        )
        cls.sources = silver_fixtures(cls.spark)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for name, value in cls.previous_python.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def literal_frame(spark, columns, rows):
    """Bounded literal fixture; no Python worker dependency on native Windows."""
    frames = [
        spark.range(1).select(
            *[
                F.lit(value).cast(kind).alias(name)
                for (name, kind), value in zip(columns, row)
            ]
        )
        for row in rows
    ]
    if not frames:
        return spark.createDataFrame(
            [], StructType([StructField(name, kind) for name, kind in columns])
        )
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return (
        result.withColumn("_batch_id", F.lit("batch-a"))
        .withColumn("_record_hash", F.lit("silver-source-hash"))
        .withColumn(
            "_silver_transformed_at", F.lit("2024-03-01T00:00:00Z").cast("timestamp")
        )
    )


def silver_fixtures(spark):
    text, number, day = StringType(), LongType(), DateType()
    return {
        "employees": literal_frame(
            spark,
            [
                ("employee_id", number),
                ("employee_number", text),
                ("hire_date", day),
                ("termination_date", day),
                ("employment_status", text),
                ("employment_type", text),
                ("email", text),
                ("annual_salary", text),
            ],
            [
                (
                    1,
                    "E001",
                    date(2023, 12, 31),
                    None,
                    "Active",
                    "Permanent",
                    "excluded@example.invalid",
                    "100.00",
                ),
                (
                    2,
                    "E002",
                    date(2024, 1, 31),
                    date(2024, 2, 29),
                    "Terminated",
                    "Fixed-term",
                    "excluded@example.invalid",
                    "200.00",
                ),
            ],
        ),
        "departments": literal_frame(
            spark,
            [
                ("department_id", number),
                ("business_unit_id", number),
                ("department_name", text),
                ("cost_center", text),
            ],
            [(1, 10, "Engineering", "CC001"), (2, 10, "Operations", "CC002")],
        ),
        "business_units": literal_frame(
            spark,
            [("business_unit_id", number), ("unit_name", text)],
            [(10, "Technology")],
        ),
        "locations": literal_frame(
            spark,
            [
                ("location_id", number),
                ("office_name", text),
                ("city", text),
                ("country", text),
                ("timezone", text),
            ],
            [
                (1, "HQ", "London", "UK", "Europe/London"),
                (2, "Office \u00e9 : N", "Paris", "France", "Europe/Paris"),
            ],
        ),
        "job_roles": literal_frame(
            spark,
            [
                ("role_id", number),
                ("role_name", text),
                ("grade", text),
                ("salary_band_min", text),
            ],
            [(1, "Engineer", "G07", "100.00"), (2, "Manager", "G10", "200.00")],
        ),
    }


def core_dimensions(spark, sources, build):
    dates = build_dim_date(spark, build)
    return {
        "dim_date": dates,
        "dim_employee": build_dim_employee(sources["employees"], dates, build),
        "dim_department": build_dim_department(
            sources["departments"], sources["business_units"], build
        ),
        "dim_location": build_dim_location(sources["locations"], build),
        "dim_job_role": build_dim_job_role(sources["job_roles"], build),
    }
