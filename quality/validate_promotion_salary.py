"""
Promotion salary validation checks.

This module validates salaries assigned to employees following promotion.

The promotion simulator constrains the employee's new salary to the
salary band configured for the destination JobRole.

This validation module independently verifies that rule.

A promotion salary is invalid when:

    promotion.new_salary < new_role.salary_band_min

or:

    promotion.new_salary > new_role.salary_band_max

The functions in this module follow the same pattern as the existing
quality framework:

- they accept a SQLAlchemy session;
- they query the database directly;
- they return records that violate the validation rule;
- they do not write reports themselves.

The main quality.validation module is responsible for converting the
returned records into ValidationResult objects and exporting them to:

    quality_reports/validation_report.csv
    quality_reports/validation_report.json
"""

from __future__ import annotations

from database.models import JobRole, Promotion


def find_promotions_below_new_role_salary_band(session):
    """
    Find promotions where the employee's new salary is below the
    minimum salary configured for the destination role.

    Args:
        session:
            Active SQLAlchemy database session.

    Returns:
        list[Promotion]:
            Promotion records whose new salary falls below the
            destination role's minimum salary.
    """

    return (
        session.query(Promotion)
        .join(
            JobRole,
            Promotion.new_role_id == JobRole.role_id,
        )
        .filter(
            Promotion.new_salary < JobRole.salary_band_min
        )
        .all()
    )


def find_promotions_above_new_role_salary_band(session):
    """
    Find promotions where the employee's new salary exceeds the
    maximum salary configured for the destination role.

    Args:
        session:
            Active SQLAlchemy database session.

    Returns:
        list[Promotion]:
            Promotion records whose new salary exceeds the
            destination role's maximum salary.
    """

    return (
        session.query(Promotion)
        .join(
            JobRole,
            Promotion.new_role_id == JobRole.role_id,
        )
        .filter(
            Promotion.new_salary > JobRole.salary_band_max
        )
        .all()
    )


def find_promotions_outside_new_role_salary_band(session):
    """
    Find all promotions where the new salary falls outside the salary
    range configured for the destination JobRole.

    Valid:

        salary_band_min <= new_salary <= salary_band_max

    Invalid:

        new_salary < salary_band_min

    or:

        new_salary > salary_band_max

    Args:
        session:
            Active SQLAlchemy database session.

    Returns:
        list[Promotion]:
            Promotion records with salaries outside the new role's
            configured salary range.
    """

    return (
        session.query(Promotion)
        .join(
            JobRole,
            Promotion.new_role_id == JobRole.role_id,
        )
        .filter(
            (Promotion.new_salary < JobRole.salary_band_min)
            | (Promotion.new_salary > JobRole.salary_band_max)
        )
        .all()
    )


def find_promotions_with_invalid_role_salary_band(session):
    """
    Find promotions associated with a JobRole whose salary-band
    configuration is itself invalid.

    A salary band is invalid when:

        salary_band_min > salary_band_max

    This indicates a reference-data problem rather than a promotion
    calculation problem.

    Args:
        session:
            Active SQLAlchemy database session.

    Returns:
        list[Promotion]:
            Promotion records associated with invalid destination-role
            salary-band configuration.
    """

    return (
        session.query(Promotion)
        .join(
            JobRole,
            Promotion.new_role_id == JobRole.role_id,
        )
        .filter(
            JobRole.salary_band_min > JobRole.salary_band_max
        )
        .all()
    )
