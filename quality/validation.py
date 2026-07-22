"""
Main data validation runner.

Run from project root:

    python -m quality.validation
"""

from __future__ import annotations

from dataclasses import dataclass

from config.logger import logger
from database.connection import get_session
from database.models import Attendance, Employee, Payroll

from quality.business_rules import (
    find_employees_with_negative_salary,
    find_employees_with_termination_before_hire,
    find_employees_without_manager,
    find_exit_interviews_for_active_employees,
    find_negative_payroll_records,
    find_non_managers_without_manager,
)
from quality.duplicate_checks import (
    find_duplicate_emails,
    find_duplicate_employee_numbers,
)
from quality.exceptions import DataQualityError
from quality.integrity_checks import (
    find_employees_with_missing_department,
    find_employees_with_missing_job_role,
    find_employees_with_missing_location,
    find_orphan_attendance_records,
    find_orphan_payroll_records,
)
from quality.metrics import get_table_counts
from quality.report import export_csv, export_json
from quality.validate_promotion_salary import (
    find_promotions_outside_new_role_salary_band,
    find_promotions_with_invalid_role_salary_band,
)


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    record_count: int
    severity: str


def log_result(result: ValidationResult) -> None:
    status = "PASS" if result.passed else "FAIL"

    message = (
        f"{status} | {result.severity} | "
        f"{result.check_name} | Records: {result.record_count}"
    )

    if result.passed:
        logger.info(message)
    elif result.severity == "CRITICAL":
        logger.error(message)
    else:
        logger.warning(message)


def validate_count_greater_than_zero(session, model, label: str) -> ValidationResult:
    count = session.query(model).count()

    return ValidationResult(
        check_name=f"{label} count greater than zero",
        passed=count > 0,
        record_count=count,
        severity="CRITICAL",
    )


def validate_empty_result(check_name: str, records: list, severity: str) -> ValidationResult:
    return ValidationResult(
        check_name=check_name,
        passed=len(records) == 0,
        record_count=len(records),
        severity=severity,
    )


def validate_expected_single_top_manager(session) -> ValidationResult:
    employees_without_manager = find_employees_without_manager(session)

    return ValidationResult(
        check_name="Exactly one employee has no manager",
        passed=len(employees_without_manager) == 1,
        record_count=len(employees_without_manager),
        severity="CRITICAL",
    )


def print_table_counts(session) -> None:
    logger.info("=" * 60)
    logger.info("TABLE COUNTS")
    logger.info("=" * 60)

    for table_name, count in get_table_counts(session).items():
        logger.info(f"{table_name}: {count}")


def run_validations() -> None:
    logger.info("=" * 60)
    logger.info("PEOPLE ANALYTICS DATA QUALITY VALIDATION")
    logger.info("=" * 60)

    session = get_session()
    results: list[ValidationResult] = []

    try:
        print_table_counts(session)

        results.extend(
            [
                validate_count_greater_than_zero(session, Employee, "Employees"),
                validate_count_greater_than_zero(session, Attendance, "Attendance"),
                validate_count_greater_than_zero(session, Payroll, "Payroll"),
                validate_expected_single_top_manager(session),
                validate_empty_result(
                    "No non-manager employees without manager",
                    find_non_managers_without_manager(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No duplicate employee numbers",
                    find_duplicate_employee_numbers(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No duplicate employee emails",
                    find_duplicate_emails(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No employees with missing department",
                    find_employees_with_missing_department(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No employees with missing job role",
                    find_employees_with_missing_job_role(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No employees with missing location",
                    find_employees_with_missing_location(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No orphan attendance records",
                    find_orphan_attendance_records(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No orphan payroll records",
                    find_orphan_payroll_records(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No employees with negative salary",
                    find_employees_with_negative_salary(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No employees with termination before hire date",
                    find_employees_with_termination_before_hire(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No exit interviews for active employees",
                    find_exit_interviews_for_active_employees(session),
                    "CRITICAL",
                ),
                validate_empty_result(
                    "No negative payroll records",
                    find_negative_payroll_records(session),
                    "CRITICAL",
                ),
                # -------------------------------------------------------
                # Promotion salary validations.
                #
                # A promoted employee's new salary must remain within
                # the configured salary band for the destination role.
                # -------------------------------------------------------
                validate_empty_result(
                    "No promotion salaries outside new role salary band",
                    find_promotions_outside_new_role_salary_band(session),
                    "CRITICAL",
                ),
                # -------------------------------------------------------
                # Reference-data safeguard.
                #
                # A destination role must not have a salary-band minimum
                # that exceeds its salary-band maximum.
                # -------------------------------------------------------
                validate_empty_result(
                    "No promotions with invalid new role salary band",
                    find_promotions_with_invalid_role_salary_band(session),
                    "CRITICAL",
                ),
            ]
        )

        logger.info("=" * 60)
        logger.info("VALIDATION RESULTS")
        logger.info("=" * 60)

        for result in results:
            log_result(result)

        # Existing reporting behaviour is preserved. The two new checks
        # are automatically written to the same CSV and JSON reports.
        export_csv(results)
        export_json(results)

        failed_critical = [
            result
            for result in results
            if not result.passed and result.severity == "CRITICAL"
        ]

        logger.info("=" * 60)
        logger.info(
            f"SUMMARY | Passed: {len(results) - len(failed_critical)} | "
            f"Failed Critical: {len(failed_critical)}"
        )
        logger.info("=" * 60)

        if failed_critical:
            raise DataQualityError(
                f"Data validation failed with {len(failed_critical)} critical issue(s)."
            )

        logger.info("All critical data quality checks passed.")

    finally:
        session.close()


if __name__ == "__main__":
    run_validations()