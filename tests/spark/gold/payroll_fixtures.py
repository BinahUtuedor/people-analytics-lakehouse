"""Compact source-shaped payroll and authoritative Phase 2 history fixtures."""

from dataclasses import replace
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    DateType,
    StringType,
    DecimalType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from tests.spark.gold.dimension_fixtures import literal_frame, core_dimensions
from spark.gold.workforce_history import build_dim_employee_assignment
from spark.gold.payroll import MONEY_FIELDS


def source_amounts(base, overtime):
    """Independent source-generator arithmetic before per-field persistence rounding."""
    b, o = Decimal(base), Decimal(overtime)
    bonus, pension, tax = b * Decimal(".03"), b * Decimal(".05"), b * Decimal(".20")
    gross, deductions = b + o + bonus, pension + tax
    return tuple(
        v.quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
        for v in (b, o, bonus, deductions, pension, tax, gross, gross - deductions)
    )


def payroll_fixture(spark, sources, build):
    build = replace(
        build,
        spec=replace(
            build.spec,
            reporting_start=date(2023, 12, 1),
            reporting_end=date(2024, 4, 15),
            source_cutoff=date(2024, 4, 15),
        ),
    )
    number, day, text = LongType(), DateType(), StringType()
    employees = literal_frame(
        spark,
        [("employee_id", number), ("hire_date", day), ("termination_date", day)],
        [
            (1, date(2023, 12, 1), None),
            (2, date(2023, 12, 1), None),
            (3, date(2024, 1, 15), date(2024, 4, 15)),
        ],
    )
    employees = (
        employees.withColumn(
            "employee_number", F.concat(F.lit("E"), F.col("employee_id"))
        )
        .withColumn(
            "employment_status",
            F.when(F.col("termination_date").isNull(), "Active").otherwise(
                "Terminated"
            ),
        )
        .withColumn("employment_type", F.lit("Permanent"))
        .withColumn("department_id", F.lit(1).cast("long"))
        .withColumn("role_id", F.lit(1).cast("long"))
        .withColumn("location_id", F.lit(1).cast("long"))
        .withColumn("manager_id", F.lit(None).cast("long"))
    )
    promotions = literal_frame(
        spark,
        [
            ("promotion_id", number),
            ("employee_id", number),
            ("promotion_date", day),
            ("new_role_id", number),
            ("old_role_id", number),
        ],
        [(11, 1, date(2024, 2, 10), 2, 1)],
    )
    transfers = literal_frame(
        spark,
        [
            ("transfer_id", number),
            ("employee_id", number),
            ("transfer_date", day),
            ("new_department_id", number),
            ("new_location_id", number),
            ("new_manager_id", number),
            ("old_department_id", number),
            ("old_location_id", number),
            ("old_manager_id", number),
        ],
        [(21, 1, date(2024, 3, 10), 2, 2, 2, 1, 1, None)],
    )
    exits = literal_frame(
        spark,
        [
            ("exit_event_id", number),
            ("employee_id", number),
            ("exit_date", day),
            ("exit_type", text),
            ("voluntary_flag", BooleanType()),
            ("regrettable_flag", BooleanType()),
        ],
        [(31, 3, date(2024, 4, 15), "Voluntary", True, False)],
    )
    # Source employees describe the event endpoint, not the historical seed.
    for attribute in ("department_id", "location_id", "manager_id"):
        employees = employees.withColumn(
            attribute,
            F.when(F.col("employee_id") == 1, F.lit(2).cast("long")).otherwise(
                F.col(attribute)
            ),
        )
    employees = employees.withColumn(
        "role_id",
        F.when(F.col("employee_id") == 1, F.lit(2).cast("long")).otherwise(
            F.col("role_id")
        ),
    )
    sources = dict(
        sources,
        employees=employees,
        promotions=promotions,
        transfers=transfers,
        employee_exits=exits,
    )
    for frame in sources.values():
        frame.cache().count()
    sources.update(core_dimensions(spark, sources, build))
    sources["dim_employee_assignment"] = build_dim_employee_assignment(
        employees, promotions, transfers, exits, build
    )
    records = [
        (101, 1, date(2023, 12, 1), date(2023, 12, 31), "GBP", "100.02", "10.01"),
        (102, 1, date(2024, 1, 1), date(2024, 1, 31), "GBP", "1000.02", "0"),
        (103, 1, date(2024, 2, 1), date(2024, 2, 29), "GBP", "1100.10", "12.34"),
        (104, 1, date(2024, 3, 1), date(2024, 3, 31), "GBP", "1200.05", "0"),
        (105, 1, date(2024, 4, 1), date(2024, 4, 15), "GBP", "600.02", "1.01"),
        (201, 2, date(2024, 1, 1), date(2024, 1, 31), "EUR", "2000.01", "0"),
        (301, 3, date(2024, 4, 1), date(2024, 4, 15), "GBP", "700.02", "0"),
    ]
    # Exercise actual Silver retained DOUBLE fields and midnight TIMESTAMP dates.
    columns = [
        ("payroll_id", number),
        ("employee_id", number),
        ("pay_period_start", TimestampType()),
        ("pay_period_end", TimestampType()),
        ("currency", text),
        ("payroll_status", text),
    ] + [
        (
            n,
            (
                DoubleType()
                if n in ("bonus", "deductions", "pension_contribution")
                else DecimalType(18, 2)
            ),
        )
        for n in MONEY_FIELDS
    ]
    rows = [
        (
            pid,
            e,
            start.isoformat(),
            end.isoformat(),
            currency,
            "Processed",
            *source_amounts(b, o),
        )
        for pid, e, start, end, currency, b, o in records
    ]
    sources["payroll"] = literal_frame(spark, columns, rows).withColumn(
        "notes", F.lit("excluded source free text")
    )
    for frame in sources.values():
        frame.cache().count()
    return build, sources
