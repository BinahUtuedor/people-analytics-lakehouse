"""
Exit interview simulation module.

Generates realistic exit interview records for employees who already
have an EmployeeExit event.

EmployeeExit is the authoritative source for:
- who left;
- the exit date;
- the exit type;
- the exit reason;
- whether the exit was voluntary.

This module no longer:
- selects employees to leave;
- generates a separate termination date;
- changes Employee.is_active;
- changes Employee.employment_status;
- changes Employee.termination_date.

Instead, it generates an ExitInterview for a selected proportion of
existing EmployeeExit events.

This separation keeps the operational lifecycle consistent:

    Employee
        ↓
    EmployeeExit
        ↓
    ExitInterview
"""

from __future__ import annotations

import random
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import EmployeeExit, ExitInterview


# -------------------------------------------------------------------
# Reproducibility
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Exit interview participation rate
#
# Not every employee who leaves completes an exit interview.
#
# Approximately 80% of exit events will therefore generate an
# ExitInterview record.
# -------------------------------------------------------------------

EXIT_INTERVIEW_PARTICIPATION_RATE = 0.80


# -------------------------------------------------------------------
# Exit interview content configuration
#
# Content is now keyed to EmployeeExit.exit_type so the interview
# remains consistent with the authoritative exit event.
# -------------------------------------------------------------------

EXIT_INTERVIEW_CONTENT = {
    "Resignation": {
        "themes": [
            "Career Progression;Development;External Opportunity",
            "Compensation;Career Growth;Retention",
            "Work-Life Balance;Career Development;Employee Experience",
            "Personal Priorities;Career Change;Development",
        ],
        "texts": [
            (
                "The employee described the departure as a voluntary decision "
                "and reflected positively on many aspects of their experience, "
                "while identifying opportunities for improved career progression."
            ),
            (
                "The employee accepted an external opportunity and highlighted "
                "career development, recognition and future progression as important "
                "factors in the decision to leave."
            ),
            (
                "The employee valued colleagues and the working environment but "
                "felt a change of role would better support longer-term career goals."
            ),
            (
                "The employee described the resignation as a considered career "
                "decision and provided constructive feedback on development, "
                "workload and employee experience."
            ),
        ],
        "sentiments": [
            "Positive",
            "Neutral",
            "Neutral",
            "Negative",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Further Study",
            "Unknown",
        ],
    },

    "Retirement": {
        "themes": [
            "Retirement;Career Completion;Succession",
            "Retirement;Knowledge Transfer;Transition",
            "Career Completion;Retirement;Legacy",
        ],
        "texts": [
            (
                "The employee is retiring after a long career and expressed "
                "appreciation for the opportunities and relationships developed "
                "within the organisation."
            ),
            (
                "The employee described retirement as a planned personal decision "
                "and reported a positive overall experience."
            ),
            (
                "The employee is leaving the workforce following retirement and "
                "highlighted the importance of effective knowledge transfer and "
                "succession planning."
            ),
        ],
        "sentiments": [
            "Positive",
            "Positive",
            "Neutral",
        ],
        "destinations": [
            "Retirement",
        ],
    },

    "End of Contract": {
        "themes": [
            "Contract Completion;Temporary Assignment;Transition",
            "End of Contract;Project Completion;Workforce Planning",
            "Contract End;Transition;Future Opportunities",
        ],
        "texts": [
            (
                "The employee's fixed-term engagement concluded as planned and "
                "the employee provided constructive feedback on the assignment "
                "and working environment."
            ),
            (
                "The employee completed the agreed temporary assignment and "
                "reported that the role provided useful experience and development."
            ),
            (
                "The contract ended following completion of the planned work, "
                "with the employee highlighting both positive experiences and "
                "areas where onboarding and transition could be improved."
            ),
        ],
        "sentiments": [
            "Positive",
            "Neutral",
            "Neutral",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Further Study",
            "Unknown",
        ],
    },

    "Redundancy": {
        "themes": [
            "Redundancy;Restructuring;Organisational Change",
            "Role Removal;Workforce Change;Communication",
            "Restructuring;Employee Support;Transition",
        ],
        "texts": [
            (
                "The employee's role was affected by organisational restructuring "
                "and the discussion focused on communication, transition support "
                "and the impact of the change."
            ),
            (
                "The employee reflected on the redundancy process and highlighted "
                "the importance of clear communication and support during periods "
                "of organisational change."
            ),
            (
                "The role was removed following business changes, and the employee "
                "provided feedback on consultation, transition arrangements and "
                "their overall experience."
            ),
        ],
        "sentiments": [
            "Neutral",
            "Negative",
            "Negative",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Unknown",
        ],
    },

    "Dismissal": {
        "themes": [
            "Performance;Conduct;Employment Termination",
            "Performance Management;Expectations;Support",
            "Conduct;Management Process;Employee Relations",
        ],
        "texts": [
            (
                "The employment relationship ended following a formal process, "
                "and the discussion captured the employee's perspective on "
                "expectations, support and communication."
            ),
            (
                "The employee provided feedback on the management process leading "
                "to termination and identified areas where expectations and "
                "communication could have been clearer."
            ),
            (
                "The interview recorded the employee's perspective on the events "
                "leading to dismissal and the support provided during the formal process."
            ),
        ],
        "sentiments": [
            "Negative",
            "Negative",
            "Neutral",
        ],
        "destinations": [
            "Different Industry",
            "Unknown",
        ],
    },
}


def generate_sentiment_score(
    sentiment_label: str,
) -> Decimal:
    """
    Generate a sentiment score aligned with the selected label.

    Positive:
        0.65 to 0.95

    Neutral:
        0.40 to 0.65

    Negative:
        0.10 to 0.40
    """

    if sentiment_label == "Positive":
        score = random.uniform(
            0.65,
            0.95,
        )

    elif sentiment_label == "Negative":
        score = random.uniform(
            0.10,
            0.40,
        )

    else:
        score = random.uniform(
            0.40,
            0.65,
        )

    return Decimal(
        str(
            round(
                score,
                4,
            )
        )
    )


def generate_satisfaction_at_exit(
    sentiment_label: str,
) -> Decimal:
    """
    Generate satisfaction at exit consistent with interview sentiment.
    """

    if sentiment_label == "Positive":
        value = random.uniform(
            3.5,
            5.0,
        )

    elif sentiment_label == "Negative":
        value = random.uniform(
            1.5,
            3.0,
        )

    else:
        value = random.uniform(
            2.5,
            4.0,
        )

    return Decimal(
        str(
            round(
                value,
                2,
            )
        )
    )


def generate_likelihood_to_recommend(
    sentiment_label: str,
) -> Decimal:
    """
    Generate recommendation likelihood consistent with sentiment.
    """

    if sentiment_label == "Positive":
        value = random.uniform(
            3.5,
            5.0,
        )

    elif sentiment_label == "Negative":
        value = random.uniform(
            1.0,
            3.0,
        )

    else:
        value = random.uniform(
            2.5,
            4.0,
        )

    return Decimal(
        str(
            round(
                value,
                2,
            )
        )
    )


def generate_exit_interviews(
    employee_exits: list[EmployeeExit],
) -> list[ExitInterview]:
    """
    Generate exit interviews from existing EmployeeExit events.

    EmployeeExit is authoritative. Therefore:

    - EmployeeExit.employee determines who left.
    - EmployeeExit.exit_date becomes ExitInterview.termination_date.
    - EmployeeExit.exit_reason becomes ExitInterview.exit_reason.
    - EmployeeExit.voluntary_flag becomes ExitInterview.voluntary_exit.
    - EmployeeExit.exit_type determines the interview content family.

    Not every leaver participates in an exit interview. Approximately
    80% of exit events generate an interview.

    Args:
        employee_exits:
            Existing EmployeeExit ORM objects.

    Returns:
        list[ExitInterview]:
            Generated exit interview records.
    """

    records: list[ExitInterview] = []

    for exit_event in employee_exits:

        # ---------------------------------------------------------------
        # Exit interview participation.
        #
        # The actual exit already happened regardless of whether the
        # employee chooses to participate in an interview.
        # ---------------------------------------------------------------

        if (
            random.random()
            >= EXIT_INTERVIEW_PARTICIPATION_RATE
        ):
            continue

        # ---------------------------------------------------------------
        # Resolve interview content from the authoritative exit type.
        # ---------------------------------------------------------------

        content = EXIT_INTERVIEW_CONTENT.get(
            exit_event.exit_type
        )

        # A newly introduced exit type should fail explicitly rather
        # than silently generating unrelated interview content.
        if content is None:
            raise ValueError(
                "Unsupported EmployeeExit exit_type "
                f"'{exit_event.exit_type}'. "
                "Add matching content to EXIT_INTERVIEW_CONTENT."
            )

        sentiment_label = random.choice(
            content[
                "sentiments"
            ]
        )

        interview_text = random.choice(
            content[
                "texts"
            ]
        )

        key_themes = random.choice(
            content[
                "themes"
            ]
        )

        destination_type = random.choice(
            content[
                "destinations"
            ]
        )

        # ---------------------------------------------------------------
        # Create ExitInterview.
        #
        # No Employee fields are modified here. The EmployeeExit
        # simulator has already updated the current Employee state.
        # ---------------------------------------------------------------

        records.append(
            ExitInterview(

                # Same employee as the authoritative exit event.
                employee=exit_event.employee,

                # Must exactly match EmployeeExit.exit_date.
                termination_date=(
                    exit_event.exit_date
                ),

                # Preserve the actual contextual reason from EmployeeExit.
                exit_reason=(
                    exit_event.exit_reason
                ),

                # Must exactly match the exit-event classification.
                voluntary_exit=(
                    exit_event.voluntary_flag
                ),

                destination_type=(
                    destination_type
                ),

                satisfaction_at_exit=(
                    generate_satisfaction_at_exit(
                        sentiment_label
                    )
                ),

                likelihood_to_recommend=(
                    generate_likelihood_to_recommend(
                        sentiment_label
                    )
                ),

                interview_text=(
                    interview_text
                ),

                key_themes=(
                    key_themes
                ),

                sentiment_label=(
                    sentiment_label
                ),

                sentiment_score=(
                    generate_sentiment_score(
                        sentiment_label
                    )
                ),
            )
        )

    return records