"""Approved MVP logical contracts, independent of ORM and storage configuration."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_VERSION = "v1"
SERIALIZATION_VERSION = "v1"
KEY_VERSION = "v1"
MANIFEST_VERSION = "v1"
UNKNOWN_NUMERIC_KEY = 0
UNKNOWN_ASSIGNMENT_KEY = "0"


class ModelType(str, Enum):
    DIMENSION = "dimension"
    INTERVAL_DIMENSION = "interval_dimension"
    EVENT_FACT = "event_fact"
    PERIODIC_SNAPSHOT = "periodic_snapshot"
    TRANSACTION_FACT = "transaction_fact"


@dataclass(frozen=True)
class FieldContract:
    name: str
    logical_type: str
    nullable: bool = False
    unknown_only_null: bool = False

    def spark_field(self) -> StructField:
        types = {
            "STRING": StringType(),
            "BIGINT": LongType(),
            "INT": IntegerType(),
            "BOOLEAN": BooleanType(),
            "DATE": DateType(),
            "TIMESTAMP": TimestampType(),
            "DECIMAL(18,2)": DecimalType(18, 2),
            "DECIMAL(18,4)": DecimalType(18, 4),
        }
        return StructField(self.name, types[self.logical_type], self.nullable)


COMMON_METADATA = (
    FieldContract("_source_batch_id", "STRING"),
    FieldContract("_gold_build_id", "STRING"),
    FieldContract("_gold_generated_at", "TIMESTAMP"),
    FieldContract("_record_hash", "STRING"),
)


@dataclass(frozen=True)
class ModelContract:
    name: str
    model_type: ModelType
    grain: tuple[str, ...]
    primary_key: tuple[str, ...]
    additional_unique: tuple[tuple[str, ...], ...]
    silver_sources: tuple[str, ...]
    partition_fields: tuple[str, ...]
    unknown_member: bool
    business_fields: tuple[FieldContract, ...]
    mvp: bool = True

    @property
    def fields(self) -> tuple[FieldContract, ...]:
        return self.business_fields + COMMON_METADATA

    @property
    def contract_id(self) -> str:
        return f"gold:{SCHEMA_VERSION}:{self.name}"


def get_gold_schema(model: str) -> StructType:
    """Return a fresh exact schema, so callers cannot mutate the registry."""
    return StructType([field.spark_field() for field in GOLD_MODELS[model].fields])


def unknown_business_row(model: str) -> dict[str, object]:
    """Construct only the reserved member's business values; never a fact row.

    Callers supply fixed release metadata separately. Real-source reconciliation
    must exclude this row; missing mandatory parents must still fail validation.
    """
    contract = GOLD_MODELS[model]
    if not contract.unknown_member:
        raise ValueError(f"No unknown fact rows are permitted: {model}")
    row: dict[str, object] = {}
    for field in contract.business_fields:
        if field.name == "is_unknown":
            value = True
        elif field.name == "assignment_key":
            value = UNKNOWN_ASSIGNMENT_KEY
        elif field.name.endswith("_history_basis"):
            value = "unknown"
        elif field.nullable:
            value = None
        elif field.logical_type in ("INT", "BIGINT"):
            value = UNKNOWN_NUMERIC_KEY
        else:
            value = "Unknown"
        row[field.name] = value
    return row


# Static transcription of the approved logical field inventory.
_BUSINESS_FIELDS = MappingProxyType(
    {
        "dim_employee": (
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("employee_number", "STRING", True, True),
            FieldContract("hire_date_key", "INT", False, False),
            FieldContract("termination_date_key", "INT", False, False),
            FieldContract("current_employment_status", "STRING", True, True),
            FieldContract("current_employment_type", "STRING", True, True),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "dim_department": (
            FieldContract("department_key", "BIGINT", False, False),
            FieldContract("department_name", "STRING", False, False),
            FieldContract("cost_center", "STRING", True, True),
            FieldContract("business_unit_id", "BIGINT", False, False),
            FieldContract("business_unit_name", "STRING", False, False),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "dim_location": (
            FieldContract("location_key", "BIGINT", False, False),
            FieldContract("office_name", "STRING", False, False),
            FieldContract("city", "STRING", False, False),
            FieldContract("country", "STRING", False, False),
            FieldContract("timezone", "STRING", True, True),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "dim_job_role": (
            FieldContract("job_role_key", "BIGINT", False, False),
            FieldContract("role_name", "STRING", False, False),
            FieldContract("grade", "STRING", False, False),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "dim_date": (
            FieldContract("date_key", "INT", False, False),
            FieldContract("calendar_date", "DATE", True, True),
            FieldContract("day_of_month", "INT", True, True),
            FieldContract("iso_day_of_week", "INT", True, True),
            FieldContract("day_name", "STRING", True, True),
            FieldContract("iso_week_number", "INT", True, True),
            FieldContract("iso_week_year", "INT", True, True),
            FieldContract("month_number", "INT", True, True),
            FieldContract("month_name", "STRING", True, True),
            FieldContract("year_month", "INT", True, True),
            FieldContract("month_start_date", "DATE", True, True),
            FieldContract("month_end_date", "DATE", True, True),
            FieldContract("calendar_quarter", "INT", True, True),
            FieldContract("calendar_year", "INT", True, True),
            FieldContract("is_weekend", "BOOLEAN", True, True),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "dim_employee_assignment": (
            FieldContract("assignment_key", "STRING", False, False),
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("valid_from_date", "DATE", True, True),
            FieldContract("valid_to_exclusive", "DATE", True, True),
            FieldContract("department_key", "BIGINT", False, False),
            FieldContract("job_role_key", "BIGINT", False, False),
            FieldContract("location_key", "BIGINT", False, False),
            FieldContract("manager_employee_key", "BIGINT", False, False),
            FieldContract("department_history_basis", "STRING", False, False),
            FieldContract("role_history_basis", "STRING", False, False),
            FieldContract("location_history_basis", "STRING", False, False),
            FieldContract("manager_history_basis", "STRING", False, False),
            FieldContract("is_unknown", "BOOLEAN", False, False),
        ),
        "fact_workforce_monthly": (
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("snapshot_month_key", "INT", False, False),
            FieldContract("reporting_year", "INT", False, False),
            FieldContract("assignment_date_key", "INT", False, False),
            FieldContract("assignment_key", "STRING", False, False),
            FieldContract("department_key", "BIGINT", False, False),
            FieldContract("job_role_key", "BIGINT", False, False),
            FieldContract("location_key", "BIGINT", False, False),
            FieldContract("manager_employee_key", "BIGINT", False, False),
            FieldContract("headcount_eom", "INT", False, False),
            FieldContract("tenure_days_eom", "INT", True, False),
        ),
        "fact_payroll": (
            FieldContract("payroll_id", "BIGINT", False, False),
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("pay_period_start_key", "INT", False, False),
            FieldContract("pay_period_end_key", "INT", False, False),
            FieldContract("payroll_month_key", "INT", False, False),
            FieldContract("reporting_year", "INT", False, False),
            FieldContract("pay_period_days", "INT", False, False),
            FieldContract("assignment_key", "STRING", False, False),
            FieldContract("department_key", "BIGINT", False, False),
            FieldContract("job_role_key", "BIGINT", False, False),
            FieldContract("location_key", "BIGINT", False, False),
            FieldContract("manager_employee_key", "BIGINT", False, False),
            FieldContract("currency", "STRING", False, False),
            FieldContract("payroll_status", "STRING", False, False),
            FieldContract("base_salary", "DECIMAL(18,2)", False, False),
            FieldContract("overtime_pay", "DECIMAL(18,2)", False, False),
            FieldContract("bonus", "DECIMAL(18,2)", False, False),
            FieldContract("deductions", "DECIMAL(18,2)", False, False),
            FieldContract("pension_contribution", "DECIMAL(18,2)", False, False),
            FieldContract("tax_amount", "DECIMAL(18,2)", False, False),
            FieldContract("gross_pay", "DECIMAL(18,2)", False, False),
            FieldContract("net_pay", "DECIMAL(18,2)", False, False),
        ),
        "fact_attendance": (
            FieldContract("attendance_id", "BIGINT", False, False),
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("work_date_key", "INT", False, False),
            FieldContract("reporting_year", "INT", False, False),
            FieldContract("assignment_key", "STRING", False, False),
            FieldContract("department_key", "BIGINT", False, False),
            FieldContract("job_role_key", "BIGINT", False, False),
            FieldContract("location_key", "BIGINT", False, False),
            FieldContract("manager_employee_key", "BIGINT", False, False),
            FieldContract("attendance_status", "STRING", False, False),
            FieldContract("absence_reason", "STRING", True, False),
            FieldContract("hours_worked", "DECIMAL(18,4)", False, False),
            FieldContract("overtime_hours", "DECIMAL(18,4)", False, False),
            FieldContract("recorded_day_count", "INT", False, False),
            FieldContract("absent_day_count", "INT", False, False),
        ),
        "fact_employee_movement": (
            FieldContract("movement_key", "STRING", False, False),
            FieldContract("movement_type", "STRING", False, False),
            FieldContract("source_dataset", "STRING", False, False),
            FieldContract("source_record_id", "BIGINT", False, False),
            FieldContract("employee_key", "BIGINT", False, False),
            FieldContract("event_date_key", "INT", False, False),
            FieldContract("before_assignment_key", "STRING", False, False),
            FieldContract("after_assignment_key", "STRING", False, False),
            FieldContract("before_department_key", "BIGINT", False, False),
            FieldContract("after_department_key", "BIGINT", False, False),
            FieldContract("before_job_role_key", "BIGINT", False, False),
            FieldContract("after_job_role_key", "BIGINT", False, False),
            FieldContract("before_location_key", "BIGINT", False, False),
            FieldContract("after_location_key", "BIGINT", False, False),
            FieldContract("before_manager_employee_key", "BIGINT", False, False),
            FieldContract("after_manager_employee_key", "BIGINT", False, False),
            FieldContract("event_count", "INT", False, False),
            FieldContract("headcount_delta", "INT", False, False),
            FieldContract("exit_type", "STRING", True, False),
            FieldContract("voluntary_flag", "BOOLEAN", True, False),
            FieldContract("regrettable_flag", "BOOLEAN", True, False),
        ),
    }
)

# Dependencies include lifecycle evidence consumed through assignment resolution.
_ASSIGNMENT_SOURCES = (
    "employees",
    "transfers",
    "promotions",
    "recruitment",
    "employee_exits",
)


def _model(name, kind, grain, pk, unique=(), sources=(), partitions=()):
    return ModelContract(
        name,
        kind,
        grain,
        pk,
        unique,
        sources,
        partitions,
        kind in (ModelType.DIMENSION, ModelType.INTERVAL_DIMENSION),
        _BUSINESS_FIELDS[name],
    )


GOLD_MODELS = MappingProxyType(
    {
        model.name: model
        for model in (
            _model(
                "dim_employee",
                ModelType.DIMENSION,
                ("employee_key",),
                ("employee_key",),
                sources=("employees",),
            ),
            _model(
                "dim_department",
                ModelType.DIMENSION,
                ("department_key",),
                ("department_key",),
                sources=("departments", "business_units"),
            ),
            _model(
                "dim_location",
                ModelType.DIMENSION,
                ("location_key",),
                ("location_key",),
                sources=("locations",),
            ),
            _model(
                "dim_job_role",
                ModelType.DIMENSION,
                ("job_role_key",),
                ("job_role_key",),
                sources=("job_roles",),
            ),
            _model(
                "dim_date",
                ModelType.DIMENSION,
                ("calendar_date",),
                ("date_key",),
                unique=(("calendar_date",),),
            ),
            _model(
                "dim_employee_assignment",
                ModelType.INTERVAL_DIMENSION,
                ("employee_key", "valid_from_date"),
                ("assignment_key",),
                unique=(("employee_key", "valid_from_date"),),
                sources=_ASSIGNMENT_SOURCES,
            ),
            _model(
                "fact_employee_movement",
                ModelType.EVENT_FACT,
                ("movement_type", "source_record_id"),
                ("movement_key",),
                unique=(("movement_type", "source_record_id"),),
                sources=(
                    "employees",
                    "employee_exits",
                    "promotions",
                    "transfers",
                    "recruitment",
                ),
            ),
            _model(
                "fact_workforce_monthly",
                ModelType.PERIODIC_SNAPSHOT,
                ("employee_key", "snapshot_month_key"),
                ("employee_key", "snapshot_month_key"),
                sources=_ASSIGNMENT_SOURCES,
                partitions=("reporting_year",),
            ),
            _model(
                "fact_payroll",
                ModelType.TRANSACTION_FACT,
                ("employee_key", "pay_period_start_key", "pay_period_end_key"),
                ("payroll_id",),
                unique=(
                    ("employee_key", "pay_period_start_key", "pay_period_end_key"),
                ),
                sources=("payroll",) + _ASSIGNMENT_SOURCES,
                partitions=("reporting_year",),
            ),
            _model(
                "fact_attendance",
                ModelType.TRANSACTION_FACT,
                ("employee_key", "work_date_key"),
                ("attendance_id",),
                unique=(("employee_key", "work_date_key"),),
                sources=("attendance",) + _ASSIGNMENT_SOURCES,
                partitions=("reporting_year",),
            ),
        )
    }
)
