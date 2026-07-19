"""
Manager feedback simulation module.

This module generates realistic manager feedback records for employees
across a three-year historical period.

Each eligible employee receives three feedback records, one for each of
the last three years.

The qualitative feedback is dynamically generated based on:

- feedback_score;
- feedback_type.

This ensures that:
- comments vary between records;
- strengths align with the employee's feedback context;
- improvement areas are relevant to the score and type of feedback;
- high-scoring employees receive appropriately positive feedback;
- lower-scoring employees receive constructive development feedback.

The module remains compatible with the existing ManagerFeedback ORM model.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

from config.constants import DEFAULT_RANDOM_SEED
from database.models import Employee, ManagerFeedback


# -------------------------------------------------------------------
# Random seed
#
# Ensures reproducible simulated data when the same seed is used.
# -------------------------------------------------------------------

random.seed(DEFAULT_RANDOM_SEED)


# -------------------------------------------------------------------
# Supported feedback types.
#
# Each generated feedback record is assigned one of these categories.
# -------------------------------------------------------------------

FEEDBACK_TYPES = [
    "Quarterly Check-in",
    "Performance",
    "Development",
]


# -------------------------------------------------------------------
# Feedback content configuration.
#
# Feedback is grouped by:
#
# 1. feedback type
# 2. performance context
#
# Performance contexts:
#
# exceptional
#     Feedback score >= 4.50
#
# strong
#     Feedback score >= 3.50 and < 4.50
#
# developing
#     Feedback score < 3.50
#
# This structure allows the qualitative content to remain logically
# aligned with both the feedback score and the purpose of the feedback.
# -------------------------------------------------------------------

FEEDBACK_CONTENT = {
    "Quarterly Check-in": {
        "exceptional": {
            "comments": [
                (
                    "The employee continues to make an excellent contribution "
                    "and is progressing strongly against agreed priorities."
                ),
                (
                    "The employee is performing at a consistently high level "
                    "and has maintained strong momentum throughout the period."
                ),
                (
                    "The employee remains highly engaged, reliable and proactive "
                    "in delivering agreed objectives."
                ),
                (
                    "The employee has demonstrated excellent progress and "
                    "continues to contribute positively across the team."
                ),
            ],
            "strengths": [
                "Delivery;Ownership;Reliability",
                "Collaboration;Initiative;Consistency",
                "Accountability;Communication;Delivery",
                "Problem Solving;Teamwork;Ownership",
            ],
            "improvement_areas": [
                (
                    "Continue broadening organisational awareness and seek "
                    "opportunities to contribute beyond immediate team priorities."
                ),
                (
                    "Maintain the current high standard of delivery while taking "
                    "on progressively more complex responsibilities."
                ),
                (
                    "Continue building confidence when influencing colleagues "
                    "across wider stakeholder groups."
                ),
                (
                    "Seek opportunities to share knowledge and support the "
                    "development of colleagues."
                ),
            ],
        },

        "strong": {
            "comments": [
                (
                    "The employee is progressing well and continues to deliver "
                    "reliably against agreed priorities."
                ),
                (
                    "The employee has maintained good performance and contributes "
                    "positively to team objectives."
                ),
                (
                    "The employee is performing consistently and demonstrates "
                    "a dependable approach to day-to-day responsibilities."
                ),
                (
                    "The employee continues to make steady progress and responds "
                    "well to changing priorities."
                ),
            ],
            "strengths": [
                "Reliability;Teamwork;Delivery",
                "Collaboration;Communication;Accountability",
                "Adaptability;Organisation;Delivery",
                "Consistency;Teamwork;Responsiveness",
            ],
            "improvement_areas": [
                (
                    "Continue improving prioritisation when managing multiple "
                    "competing responsibilities."
                ),
                (
                    "Build greater confidence when communicating recommendations "
                    "to senior stakeholders."
                ),
                (
                    "Continue developing proactive risk identification and early "
                    "escalation of delivery challenges."
                ),
                (
                    "Seek opportunities to take greater ownership of complex tasks."
                ),
            ],
        },

        "developing": {
            "comments": [
                (
                    "The employee is making progress but would benefit from more "
                    "consistent delivery against agreed priorities."
                ),
                (
                    "The employee continues to develop capability and would benefit "
                    "from additional support in some areas of responsibility."
                ),
                (
                    "The employee has shown positive progress but needs to improve "
                    "consistency when managing workload and deadlines."
                ),
                (
                    "The employee is developing well in some areas but requires "
                    "greater confidence and independence in others."
                ),
            ],
            "strengths": [
                "Learning Agility;Commitment;Teamwork",
                "Adaptability;Collaboration;Potential",
                "Positive Attitude;Responsiveness;Learning",
                "Team Contribution;Commitment;Development",
            ],
            "improvement_areas": [
                (
                    "Improve prioritisation and time management to ensure key "
                    "deliverables are completed within agreed timescales."
                ),
                (
                    "Continue developing confidence when working independently "
                    "and seek support earlier when required."
                ),
                (
                    "Focus on improving consistency and attention to detail."
                ),
                (
                    "Provide more regular updates on progress, risks and dependencies."
                ),
            ],
        },
    },

    "Performance": {
        "exceptional": {
            "comments": [
                (
                    "The employee has delivered exceptional results and has "
                    "consistently exceeded performance expectations."
                ),
                (
                    "The employee demonstrates outstanding performance, strong "
                    "ownership and a high level of professional capability."
                ),
                (
                    "The employee has made a significant contribution to team "
                    "and organisational objectives during the review period."
                ),
                (
                    "Performance has been consistently excellent, with the employee "
                    "demonstrating strong judgement and delivery."
                ),
            ],
            "strengths": [
                "High Performance;Leadership;Delivery",
                "Strategic Thinking;Ownership;Results",
                "Problem Solving;Decision Making;Accountability",
                "Technical Expertise;Consistency;Impact",
            ],
            "improvement_areas": [
                (
                    "Continue developing strategic leadership capability and seek "
                    "opportunities to influence at a broader organisational level."
                ),
                (
                    "Build greater exposure to cross-functional initiatives and "
                    "longer-term strategic priorities."
                ),
                (
                    "Continue mentoring colleagues and sharing expertise across "
                    "the wider team."
                ),
                (
                    "Seek opportunities to lead more complex programmes and "
                    "organisational initiatives."
                ),
            ],
        },

        "strong": {
            "comments": [
                (
                    "The employee has delivered strong performance and consistently "
                    "met expectations during the review period."
                ),
                (
                    "The employee demonstrates reliable performance and contributes "
                    "positively to key team objectives."
                ),
                (
                    "The employee has performed well and shows a strong commitment "
                    "to delivering high-quality outcomes."
                ),
                (
                    "The employee continues to demonstrate good capability and "
                    "dependable performance across core responsibilities."
                ),
            ],
            "strengths": [
                "Reliability;Delivery;Accountability",
                "Quality;Teamwork;Consistency",
                "Ownership;Problem Solving;Collaboration",
                "Technical Capability;Delivery;Organisation",
            ],
            "improvement_areas": [
                (
                    "Continue building strategic awareness and consider the wider "
                    "business impact of decisions."
                ),
                (
                    "Develop greater confidence when influencing senior stakeholders."
                ),
                (
                    "Seek opportunities to take ownership of more complex work."
                ),
                (
                    "Continue strengthening communication and cross-functional collaboration."
                ),
            ],
        },

        "developing": {
            "comments": [
                (
                    "Performance has been mixed during the review period and "
                    "greater consistency is required in key areas."
                ),
                (
                    "The employee demonstrates capability but needs to improve "
                    "consistency in delivery and quality."
                ),
                (
                    "The employee has made progress but requires continued support "
                    "to consistently meet performance expectations."
                ),
                (
                    "There are positive contributions, although some performance "
                    "gaps remain and should be addressed through focused development."
                ),
            ],
            "strengths": [
                "Commitment;Teamwork;Learning",
                "Adaptability;Positive Attitude;Potential",
                "Collaboration;Responsiveness;Development",
                "Supportiveness;Learning Agility;Team Contribution",
            ],
            "improvement_areas": [
                (
                    "Improve consistency of delivery and work with the manager "
                    "to establish clear priorities and milestones."
                ),
                (
                    "Focus on improving quality and applying structured checks "
                    "before completing work."
                ),
                (
                    "Develop stronger time-management skills and communicate "
                    "earlier when deadlines are at risk."
                ),
                (
                    "Take greater ownership of assigned responsibilities and "
                    "seek clarification promptly when required."
                ),
            ],
        },
    },

    "Development": {
        "exceptional": {
            "comments": [
                (
                    "The employee demonstrates strong development potential and "
                    "is ready to take on broader responsibilities."
                ),
                (
                    "The employee has shown significant growth and demonstrates "
                    "the capability to progress into more complex roles."
                ),
                (
                    "The employee is developing rapidly and has demonstrated "
                    "strong potential for future leadership or specialist responsibilities."
                ),
                (
                    "The employee consistently seeks learning opportunities and "
                    "applies new skills effectively in the workplace."
                ),
            ],
            "strengths": [
                "Growth Potential;Learning Agility;Leadership",
                "Development;Strategic Thinking;Initiative",
                "Learning;Ownership;Adaptability",
                "Potential;Leadership;Professional Development",
            ],
            "improvement_areas": [
                (
                    "Continue expanding strategic exposure and seek opportunities "
                    "to lead cross-functional initiatives."
                ),
                (
                    "Build broader leadership experience through mentoring and "
                    "increased responsibility."
                ),
                (
                    "Continue developing commercial and organisational awareness."
                ),
                (
                    "Seek opportunities to deepen specialist expertise while "
                    "broadening leadership capability."
                ),
            ],
        },

        "strong": {
            "comments": [
                (
                    "The employee is developing well and continues to build "
                    "capability in areas relevant to future progression."
                ),
                (
                    "The employee has demonstrated good development progress and "
                    "responds positively to feedback and learning opportunities."
                ),
                (
                    "The employee continues to strengthen professional capability "
                    "and shows potential for broader responsibilities."
                ),
                (
                    "The employee is making good progress against development "
                    "objectives and demonstrates a strong willingness to learn."
                ),
            ],
            "strengths": [
                "Learning Agility;Development;Commitment",
                "Adaptability;Potential;Collaboration",
                "Professional Growth;Responsiveness;Learning",
                "Initiative;Development;Teamwork",
            ],
            "improvement_areas": [
                (
                    "Continue building confidence when taking ownership of "
                    "more complex responsibilities."
                ),
                (
                    "Develop stronger stakeholder management and influencing skills."
                ),
                (
                    "Seek opportunities to deepen technical and strategic capability."
                ),
                (
                    "Continue broadening experience through cross-functional work."
                ),
            ],
        },

        "developing": {
            "comments": [
                (
                    "The employee is developing capability but requires a more "
                    "structured approach to achieving agreed development goals."
                ),
                (
                    "The employee has identified useful development areas and "
                    "would benefit from continued coaching and practical experience."
                ),
                (
                    "The employee demonstrates development potential but needs "
                    "greater consistency in applying new skills."
                ),
                (
                    "The employee is progressing but would benefit from clearer "
                    "development objectives and regular feedback."
                ),
            ],
            "strengths": [
                "Learning Potential;Commitment;Adaptability",
                "Positive Attitude;Development;Teamwork",
                "Responsiveness;Learning;Collaboration",
                "Potential;Commitment;Professional Growth",
            ],
            "improvement_areas": [
                (
                    "Create a focused development plan with clear objectives, "
                    "milestones and regular progress reviews."
                ),
                (
                    "Seek additional training and practical opportunities to "
                    "strengthen technical capability."
                ),
                (
                    "Build confidence by taking greater ownership of defined tasks."
                ),
                (
                    "Actively seek feedback and apply learning more consistently "
                    "in day-to-day responsibilities."
                ),
            ],
        },
    },
}


def determine_feedback_context(
    feedback_score: Decimal,
) -> str:
    """
    Determine qualitative feedback context from the numeric score.

    Score bands:

        4.50 - 5.00  -> exceptional
        3.50 - 4.49  -> strong
        2.50 - 3.49  -> developing

    The simulator currently generates scores between 2.50 and 5.00,
    so no lower category is required.

    Args:
        feedback_score:
            Numeric manager feedback score.

    Returns:
        str:
            Feedback context key.
    """

    if feedback_score >= Decimal("4.50"):
        return "exceptional"

    if feedback_score >= Decimal("3.50"):
        return "strong"

    return "developing"


def generate_sentiment_score(
    sentiment_label: str,
) -> Decimal:
    """
    Generate a sentiment score aligned with the sentiment label.

    Positive feedback receives a higher score range.
    Neutral feedback receives a mid-range score.

    Args:
        sentiment_label:
            "Positive" or "Neutral".

    Returns:
        Decimal:
            Sentiment score rounded to four decimal places.
    """

    if sentiment_label == "Positive":
        value = random.uniform(
            0.65,
            0.95,
        )

    else:
        value = random.uniform(
            0.30,
            0.64,
        )

    return Decimal(
        str(
            round(
                value,
                4,
            )
        )
    )


def generate_manager_feedback(
    employees: list[Employee],
) -> list[ManagerFeedback]:
    """
    Generate three years of manager feedback for eligible employees.

    Employees without a manager are skipped.

    For 2,000 employees with one top-level employee who has no manager:

        1,999 eligible employees
        x 3 annual feedback records
        = 5,997 feedback records

    Args:
        employees:
            List of Employee ORM objects.

    Returns:
        list[ManagerFeedback]:
            Generated manager feedback records.
    """

    # Container for all generated records.
    records: list[ManagerFeedback] = []

    # Determine current calendar year.
    current_year = date.today().year

    # -------------------------------------------------------------------
    # Generate feedback for every eligible employee.
    # -------------------------------------------------------------------

    for employee in employees:

        # The top-level employee has no manager and therefore cannot
        # receive manager feedback.
        if employee.manager is None:
            continue

        # ---------------------------------------------------------------
        # Generate one feedback record for each of the last three years.
        # ---------------------------------------------------------------

        for year in range(
            current_year - 2,
            current_year + 1,
        ):

            # -----------------------------------------------------------
            # Generate numeric feedback score.
            #
            # Existing score range is preserved.
            # -----------------------------------------------------------

            feedback_score = Decimal(
                str(
                    round(
                        random.uniform(
                            2.5,
                            5.0,
                        ),
                        2,
                    )
                )
            )

            # -----------------------------------------------------------
            # Select the type of manager feedback.
            # -----------------------------------------------------------

            feedback_type = random.choice(
                FEEDBACK_TYPES
            )

            # -----------------------------------------------------------
            # Determine feedback context from the score.
            # -----------------------------------------------------------

            feedback_context = (
                determine_feedback_context(
                    feedback_score
                )
            )

            # -----------------------------------------------------------
            # Retrieve qualitative content corresponding to both:
            #
            # - feedback type;
            # - performance context.
            # -----------------------------------------------------------

            feedback_options = (
                FEEDBACK_CONTENT[
                    feedback_type
                ][
                    feedback_context
                ]
            )

            # -----------------------------------------------------------
            # Select varied contextual qualitative feedback.
            # -----------------------------------------------------------

            comments = random.choice(
                feedback_options[
                    "comments"
                ]
            )

            strengths = random.choice(
                feedback_options[
                    "strengths"
                ]
            )

            improvement_areas = random.choice(
                feedback_options[
                    "improvement_areas"
                ]
            )

            # -----------------------------------------------------------
            # Determine sentiment.
            #
            # Existing business logic is preserved:
            #
            # >= 3.50 -> Positive
            # < 3.50  -> Neutral
            # -----------------------------------------------------------

            sentiment_label = (
                "Positive"
                if feedback_score
                >= Decimal(
                    "3.50"
                )
                else "Neutral"
            )

            # Generate a numeric sentiment score that is logically
            # aligned with the sentiment label.
            sentiment_score = (
                generate_sentiment_score(
                    sentiment_label
                )
            )

            # -----------------------------------------------------------
            # Generate feedback date.
            #
            # Existing logic is preserved:
            # feedback occurs in April, August or November.
            # -----------------------------------------------------------

            feedback_date = date(
                year,
                random.choice(
                    [
                        4,
                        8,
                        11,
                    ]
                ),
                10,
            )

            # -----------------------------------------------------------
            # Create ManagerFeedback ORM object.
            # -----------------------------------------------------------

            manager_feedback = ManagerFeedback(

                # Employee receiving feedback.
                employee=employee,

                # Employee's assigned manager.
                manager=employee.manager,

                # Date on which feedback was provided.
                feedback_date=feedback_date,

                # Type of feedback interaction.
                feedback_type=feedback_type,

                # Numeric feedback score.
                feedback_score=feedback_score,

                # Contextual overall manager comments.
                comments=comments,

                # Contextual employee strengths.
                strengths=strengths,

                # Contextual development recommendations.
                improvement_areas=(
                    improvement_areas
                ),

                # Sentiment derived from feedback score.
                sentiment_label=(
                    sentiment_label
                ),

                # Numeric sentiment score aligned with sentiment label.
                sentiment_score=(
                    sentiment_score
                ),
            )

            # Add generated record to output collection.
            records.append(
                manager_feedback
            )

    # Return all generated ManagerFeedback ORM objects
    # to the simulation orchestrator.
    return records
