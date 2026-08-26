"""Shared registry of operational datasets supported by Raw and Bronze."""

from __future__ import annotations


SUPPORTED_DATASETS: tuple[str, ...] = (
    "business_units",
    "departments",
    "locations",
    "job_roles",
    "employees",
    "employee_exits",
    "attendance",
    "payroll",
    "leave_requests",
    "recruitment",
    "promotions",
    "transfers",
    "training",
    "performance_reviews",
    "employee_surveys",
    "manager_feedback",
    "exit_interviews",
)
