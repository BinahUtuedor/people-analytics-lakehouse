"""
Data quality metrics.
"""

from database.models import (
    Attendance,
    Employee,
    EmployeeExit,
    EmployeeSurvey,
    ExitInterview,
    LeaveRequest,
    ManagerFeedback,
    Payroll,
    PerformanceReview,
    Promotion,
    Recruitment,
    Training,
    Transfer,
)


def get_table_counts(session) -> dict[str, int]:
    """
    Return record counts for generated operational tables.
    """

    return {
        "employees": session.query(Employee).count(),
        "attendance": session.query(Attendance).count(),
        "payroll": session.query(Payroll).count(),
        "leave_requests": session.query(LeaveRequest).count(),
        "training": session.query(Training).count(),
        "performance_reviews": session.query(PerformanceReview).count(),
        "promotions": session.query(Promotion).count(),
        "transfers": session.query(Transfer).count(),
        "recruitment": session.query(Recruitment).count(),
        "employee_surveys": session.query(EmployeeSurvey).count(),
        "manager_feedback": session.query(ManagerFeedback).count(),
        "employee_exits": session.query(EmployeeExit).count(),
        "exit_interviews": session.query(ExitInterview).count(),
    }