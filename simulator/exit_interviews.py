"""
Exit interview simulation module.

Generates realistic exit interview records with qualitative content that
aligns with each employee's reason for leaving.

The module:
- Generates different interview text for different exit reasons.
- Generates key themes aligned with the selected exit reason.
- Aligns sentiment with the nature of the exit reason.
- Prevents future termination dates.
- Updates the employee record to Terminated.
- Remains compatible with the existing ExitInterview ORM model.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, ExitInterview


# -------------------------------------------------------------------
# Reproducibility
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Exit interview content configuration
#
# Each exit reason contains:
# - possible themes
# - possible interview statements
# - realistic sentiment labels
# - likely destination types
#
# One option from each list is selected for every generated interview.
# -------------------------------------------------------------------

EXIT_INTERVIEW_CONTENT = {
    "Career Progression": {
        "themes": [
            "Career Progression;Promotion Opportunities;Professional Development",
            "Career Growth;Advancement;Internal Mobility",
            "Progression;Leadership Opportunities;Development",
        ],
        "texts": [
            (
                "The employee enjoyed working with the team but felt there "
                "were limited opportunities for progression into more senior roles."
            ),
            (
                "The employee decided to leave after securing a position that "
                "offered a clearer promotion pathway and greater responsibility."
            ),
            (
                "The employee valued the organisation but wanted faster career "
                "progression and more opportunities to develop leadership experience."
            ),
            (
                "The employee felt the current role no longer provided sufficient "
                "scope for professional growth and decided to pursue a more "
                "challenging opportunity."
            ),
            (
                "The employee appreciated the learning opportunities available "
                "but believed career advancement within the current structure "
                "was too limited."
            ),
        ],
        "sentiments": [
            "Neutral",
            "Positive",
            "Neutral",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Further Study",
            "Unknown",
        ],
        "voluntary": True,
    },

    "Compensation": {
        "themes": [
            "Compensation;Pay;Market Competitiveness",
            "Reward;Salary;Benefits",
            "Pay Progression;Compensation;Recognition",
        ],
        "texts": [
            (
                "The employee enjoyed the role but felt salary progression had "
                "not kept pace with increased responsibilities and external market rates."
            ),
            (
                "The employee accepted an external opportunity offering a more "
                "competitive salary and stronger overall benefits package."
            ),
            (
                "The employee felt compensation progression was slower than "
                "expected despite consistently strong performance."
            ),
            (
                "The employee cited pay as the main reason for leaving and "
                "believed comparable roles elsewhere offered better financial recognition."
            ),
            (
                "The employee appreciated the working environment but felt the "
                "total reward package did not adequately reflect their contribution."
            ),
        ],
        "sentiments": [
            "Negative",
            "Neutral",
            "Negative",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Unknown",
        ],
        "voluntary": True,
    },

    "Relocation": {
        "themes": [
            "Relocation;Geography;Personal Circumstances",
            "Location;Commute;Mobility",
            "Relocation;Family Commitments;Geographic Change",
        ],
        "texts": [
            (
                "The employee is relocating to another part of the country and "
                "is therefore unable to continue in the current role."
            ),
            (
                "The employee decided to leave because of a planned family "
                "relocation that would make regular travel to the workplace impractical."
            ),
            (
                "The employee valued the organisation but is moving overseas "
                "and could not continue in the position under the current working arrangement."
            ),
            (
                "The employee is leaving due to a change in personal circumstances "
                "that requires relocation closer to family."
            ),
            (
                "The employee cited geographic relocation as the primary reason "
                "for leaving and reported no significant concerns with the organisation."
            ),
        ],
        "sentiments": [
            "Positive",
            "Neutral",
            "Positive",
        ],
        "destinations": [
            "Different Industry",
            "Unknown",
        ],
        "voluntary": True,
    },

    "Workload": {
        "themes": [
            "Workload;Burnout;Work-Life Balance",
            "Resourcing;Work Pressure;Wellbeing",
            "Burnout;Capacity;Employee Wellbeing",
        ],
        "texts": [
            (
                "The employee reported that sustained workload pressures had "
                "negatively affected work-life balance and contributed to the decision to leave."
            ),
            (
                "The employee valued colleagues but felt persistent staffing "
                "pressures resulted in an unsustainable level of workload."
            ),
            (
                "The employee described periods of high pressure and limited "
                "capacity as key factors influencing the decision to seek another role."
            ),
            (
                "The employee felt workload expectations had increased significantly "
                "and that additional support or resourcing was required."
            ),
            (
                "The employee cited burnout and difficulty maintaining a healthy "
                "work-life balance as the main reasons for leaving."
            ),
        ],
        "sentiments": [
            "Negative",
            "Negative",
            "Neutral",
        ],
        "destinations": [
            "Competitor",
            "Different Industry",
            "Unknown",
        ],
        "voluntary": True,
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
                "The employee has chosen to retire and reported a positive "
                "experience overall, highlighting strong relationships with colleagues."
            ),
            (
                "The employee is leaving the workforce permanently following "
                "retirement and is supporting knowledge transfer before departure."
            ),
            (
                "The employee described retirement as a planned personal decision "
                "and expressed satisfaction with their career in the organisation."
            ),
            (
                "The employee is retiring and highlighted teamwork, professional "
                "development and long-term relationships as positive aspects of "
                "their experience."
            ),
        ],
        "sentiments": [
            "Positive",
            "Positive",
            "Neutral",
        ],
        "destinations": [
            "Unknown",
        ],
        "voluntary": True,
    },

    "Personal Reasons": {
        "themes": [
            "Personal Circumstances;Family;Work-Life Balance",
            "Personal Reasons;Wellbeing;Life Changes",
            "Family Commitments;Personal Priorities;Flexibility",
        ],
        "texts": [
            (
                "The employee is leaving due to personal circumstances and "
                "preferred not to provide further detail."
            ),
            (
                "The employee cited changing family commitments and personal "
                "priorities as the main reasons for leaving."
            ),
            (
                "The employee decided to step away from the role to focus on "
                "personal responsibilities outside work."
            ),
            (
                "The employee reported that a change in personal circumstances "
                "made it difficult to continue in the current position."
            ),
            (
                "The employee is leaving for personal reasons and expressed "
                "appreciation for the support received from colleagues and management."
            ),
        ],
        "sentiments": [
            "Neutral",
            "Positive",
            "Neutral",
        ],
        "destinations": [
            "Further Study",
            "Different Industry",
            "Unknown",
        ],
        "voluntary": True,
    },
}


def generate_sentiment_score(
    sentiment_label: str,
) -> Decimal:
    """
    Generate a sentiment score aligned with the sentiment label.

    The score ranges are deliberately separated so that downstream
    analytics can interpret them consistently.

    Positive:
        0.65 to 0.95

    Neutral:
        0.40 to 0.65

    Negative:
        0.10 to 0.40
    """

    if sentiment_label == "Positive":
        score = random.uniform(0.65, 0.95)

    elif sentiment_label == "Negative":
        score = random.uniform(0.10, 0.40)

    else:
        score = random.uniform(0.40, 0.65)

    return Decimal(
        str(
            round(
                score,
                4,
            )
        )
    )


def generate_exit_interviews(
    employees: list[Employee],
) -> list[ExitInterview]:
    """
    Generate realistic exit interview records.

    Only employees who:
    - are not managers;
    - have worked for at least 180 days; and
    - meet the simulated 4% attrition probability

    are selected as leavers.

    The function returns SQLAlchemy ExitInterview objects and updates
    the corresponding Employee objects to Terminated status.
    """

    records: list[ExitInterview] = []

    # -------------------------------------------------------------------
    # Select eligible leavers
    #
    # Employees must have at least 180 days of service so that a valid
    # historical termination date can always be generated.
    # -------------------------------------------------------------------

    leavers = [
        employee
        for employee in employees
        if (
            not employee.is_manager
            and (date.today() - employee.hire_date).days >= 180
            and random.random() < 0.04
        )
    ]

    # -------------------------------------------------------------------
    # Generate one exit interview per selected employee
    # -------------------------------------------------------------------

    for employee in leavers:

        # Calculate how long the employee has worked up to today.
        days_employed = (
            date.today() - employee.hire_date
        ).days

        # Generate a termination date:
        #
        # - at least 180 days after hire;
        # - no more than 1,000 days after hire;
        # - never later than today's date.
        termination_date = (
            employee.hire_date
            + timedelta(
                days=random.randint(
                    180,
                    min(
                        1000,
                        days_employed,
                    ),
                )
            )
        )

        # -------------------------------------------------------------------
        # Select an exit reason
        # -------------------------------------------------------------------

        exit_reason = random.choice(
            list(
                EXIT_INTERVIEW_CONTENT.keys()
            )
        )

        # Retrieve the configuration associated with that exit reason.
        content = EXIT_INTERVIEW_CONTENT[
            exit_reason
        ]

        # -------------------------------------------------------------------
        # Generate qualitative interview content
        # -------------------------------------------------------------------

        # Select interview text aligned with the reason for leaving.
        interview_text = random.choice(
            content["texts"]
        )

        # Select themes aligned with the same exit reason.
        key_themes = random.choice(
            content["themes"]
        )

        # Select an appropriate sentiment.
        sentiment_label = random.choice(
            content["sentiments"]
        )

        # Select a realistic destination type.
        destination_type = random.choice(
            content["destinations"]
        )

        # Generate sentiment score based on sentiment label.
        sentiment_score = generate_sentiment_score(
            sentiment_label
        )

        # -------------------------------------------------------------------
        # Create ExitInterview ORM object
        # -------------------------------------------------------------------

        exit_interview = ExitInterview(

            # Relationship to Employee.
            employee=employee,

            # Employment termination date.
            termination_date=termination_date,

            # Primary reason for leaving.
            exit_reason=exit_reason,

            # Currently derived from the exit-reason configuration.
            #
            # When exit reasons are fully sourced from:
            #
            # reference_data/exit_reasons.yml
            #
            # this value can be loaded directly from YAML.
            voluntary_exit=content["voluntary"],

            # Likely destination following departure.
            destination_type=destination_type,

            # Overall satisfaction score at departure.
            satisfaction_at_exit=Decimal(
                str(
                    round(
                        random.uniform(
                            1.5,
                            5.0,
                        ),
                        2,
                    )
                )
            ),

            # Likelihood that the employee would recommend
            # the organisation to others.
            likelihood_to_recommend=Decimal(
                str(
                    round(
                        random.uniform(
                            1.0,
                            5.0,
                        ),
                        2,
                    )
                )
            ),

            # Qualitative exit interview response.
            interview_text=interview_text,

            # Semi-structured themes used later for NLP
            # and People Analytics.
            key_themes=key_themes,

            # Sentiment classification.
            sentiment_label=sentiment_label,

            # Numeric sentiment score.
            sentiment_score=sentiment_score,
        )

        records.append(
            exit_interview
        )

        # -------------------------------------------------------------------
        # Update employee employment status
        #
        # These changes are persisted when simulator.py commits
        # the SQLAlchemy session.
        # -------------------------------------------------------------------

        employee.is_active = False

        employee.employment_status = (
            "Terminated"
        )

        employee.termination_date = (
            termination_date
        )

    return records