"""
Employee ORM model.

The Employee table is the central master table in the operational HR database.
Most transactional HR tables reference this table.

This model supports:
- Employee demographics
- Employment status
- Department, role, location and manager assignment
- Self-referencing manager hierarchy
- Relationships to HR transactions such as payroll, attendance and reviews
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional

from sqlalchemy import Boolean, Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


class Employee(Base):
    """
    Employee master record.

    Each row represents one employee in the fictional organisation.
    """

    __tablename__ = "employees"

    # ---------------------------------------------------------
    # Primary Key
    # ---------------------------------------------------------

    employee_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # ---------------------------------------------------------
    # Employee Identifiers
    # ---------------------------------------------------------

    employee_number: Mapped[str] = mapped_column(
        String(30),
        unique=True,
        nullable=False,
        index=True,
    )

    email: Mapped[str] = mapped_column(
        String(150),
        unique=True,
        nullable=False,
        index=True,
    )

    # ---------------------------------------------------------
    # Personal Details
    # ---------------------------------------------------------

    first_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    last_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    gender: Mapped[Optional[str]] = mapped_column(
        String(30),
        nullable=True,
    )

    date_of_birth: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    # ---------------------------------------------------------
    # Employment Details
    # ---------------------------------------------------------

    hire_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    termination_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
    )

    employment_status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="Active",
        index=True,
    )

    employment_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="Permanent",
    )

    contract_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
    )

    # ---------------------------------------------------------
    # Compensation
    # ---------------------------------------------------------

    annual_salary: Mapped[float] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )

    currency: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="GBP",
    )

    # ---------------------------------------------------------
    # Foreign Keys
    # ---------------------------------------------------------

    department_id: Mapped[int] = mapped_column(
        ForeignKey("departments.department_id"),
        nullable=False,
        index=True,
    )

    role_id: Mapped[int] = mapped_column(
        ForeignKey("job_roles.role_id"),
        nullable=False,
        index=True,
    )

    location_id: Mapped[int] = mapped_column(
        ForeignKey("locations.location_id"),
        nullable=False,
        index=True,
    )

    manager_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("employees.employee_id"),
        nullable=True,
        index=True,
    )

    # ---------------------------------------------------------
    # Flags
    # ---------------------------------------------------------

    is_manager: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
    )

    # ---------------------------------------------------------
    # Relationships: Lookup Tables
    # ---------------------------------------------------------

    department: Mapped["Department"] = relationship(
        back_populates="employees",
    )

    job_role: Mapped["JobRole"] = relationship(
        back_populates="employees",
    )

    location: Mapped["Location"] = relationship(
        back_populates="employees",
    )

    # ---------------------------------------------------------
    # Self-referencing Manager Relationship
    # ---------------------------------------------------------

    manager: Mapped[Optional["Employee"]] = relationship(
        "Employee",
        remote_side=[employee_id],
        back_populates="direct_reports",
    )

    direct_reports: Mapped[List["Employee"]] = relationship(
        "Employee",
        back_populates="manager",
    )

    # ---------------------------------------------------------
    # Relationships: Transaction Tables
    # ---------------------------------------------------------

    attendance_records: Mapped[List["Attendance"]] = relationship(
        "Attendance",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    payroll_records: Mapped[List["Payroll"]] = relationship(
        "Payroll",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    leave_requests: Mapped[List["LeaveRequest"]] = relationship(
        "LeaveRequest",
        foreign_keys="LeaveRequest.employee_id",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    training_records: Mapped[List["Training"]] = relationship(
        "Training",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    performance_reviews: Mapped[List["PerformanceReview"]] = relationship(
        "PerformanceReview",
        foreign_keys="PerformanceReview.employee_id",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    promotions: Mapped[List["Promotion"]] = relationship(
        "Promotion",
        foreign_keys="Promotion.employee_id",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    transfers: Mapped[List["Transfer"]] = relationship(
        "Transfer",
        foreign_keys="Transfer.employee_id",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    surveys: Mapped[List["EmployeeSurvey"]] = relationship(
        "EmployeeSurvey",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    feedback_received: Mapped[List["ManagerFeedback"]] = relationship(
        "ManagerFeedback",
        foreign_keys="ManagerFeedback.employee_id",
        back_populates="employee",
        cascade="all, delete-orphan",
    )

    feedback_given: Mapped[List["ManagerFeedback"]] = relationship(
        "ManagerFeedback",
        foreign_keys="ManagerFeedback.manager_id",
        back_populates="manager",
        cascade="all, delete-orphan",
    )

    exit_interview: Mapped[Optional["ExitInterview"]] = relationship(
        "ExitInterview",
        back_populates="employee",
        cascade="all, delete-orphan",
        uselist=False,
    )

    # ---------------------------------------------------------
    # String Representation
    # ---------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Employee("
            f"id={self.employee_id}, "
            f"employee_number='{self.employee_number}', "
            f"name='{self.first_name} {self.last_name}', "
            f"status='{self.employment_status}'"
            f")"
        )