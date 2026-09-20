"""Deterministic attendance source and historical assignment fixtures."""

from datetime import date
from decimal import Decimal

from pyspark.sql.types import DateType, DecimalType, LongType, StringType, TimestampType
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase, literal_frame
from tests.spark.gold.payroll_fixtures import payroll_fixture


def attendance_fixture(spark, sources, build):
    build, sources = payroll_fixture(spark, sources, build)
    rows = [
        (1001, 1, "2023-12-01", "Present", None, "7.5000", "0.0000"),
        (1002, 1, "2024-01-15", "Remote", None, "7.2500", "0.0000"),
        (1003, 1, "2024-02-29", "Absent", "Medical Appointment", "0.0000", "0.0000"),
        (1004, 1, "2024-03-15", "Hybrid", None, "8.0000", "0.5000"),
        (1005, 1, "2024-04-15", "Absent", "Family Emergency", "0.0000", "0.0000"),
        (2001, 2, "2024-01-15", "Training", None, "7.5000", "0.0000"),
        (3001, 3, "2024-04-15", "Business Travel", None, "8.0000", "1.0000"),
    ]
    sources["attendance"] = literal_frame(
        spark,
        [
            ("attendance_id", LongType()),
            ("employee_id", LongType()),
            ("work_date", TimestampType()),
            ("status", StringType()),
            ("absence_reason", StringType()),
            ("hours_worked", DecimalType(18, 4)),
            ("overtime_hours", DecimalType(18, 4)),
        ],
        rows,
    )
    return build, sources
