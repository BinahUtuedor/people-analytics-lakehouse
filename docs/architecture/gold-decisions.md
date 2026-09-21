# Gold Design Decisions

Status: the decisions below are approved for the design baseline. Phase 0
contracts and Phase 1/2/3 local transformations and validation are complete.
Phase 4 payroll is complete and validated. No production Gold release exists. [Gold layer](gold-layer.md) is the engineering contract and
[implementation plan](../plans/gold-implementation-plan.md) defines approval
gates. Approval of these decisions does not authorize implementation, cloud
access, infrastructure, publication or a serving-engine selection.

The complete approved MVP schemas are documented in the Gold layer contract.
Phase 0 encodes those contracts in `spark/gold/`; further phases need separate approval.
The decisions below preserve the approved review and the explicit MVP choices.

| Decision | Rationale | Alternatives considered | Consequence | Future reconsideration trigger |
| --- | --- | --- | --- | --- |
| Closed monthly workforce snapshots for MVP | Stable reporting periods and bounded initial scope | Daily snapshots; partial-month/as-of snapshots | Only closed calendar months with employment-overlap rows; closing headcount uses its flag, not row count | Separately approved operational/as-of reporting need |
| Post-event closing headcount | Closing population reflects exits already effective on reporting date | Inclusive exit-date headcount | Exit on reporting date contributes zero; activity facts may retain inclusive employment windows | Explicit change to reporting cutoff definition |
| Reconstruct assignment from employees, transfers, promotions and relevant lifecycle evidence; label current-state assumptions where history cannot be proven | Uses available evidence without claiming unsupported history | Current-state attribution everywhere; claiming complete source SCD history; rejecting all partial evidence | Four history-basis fields qualify reconstruction; half-open intervals end at next change or employment/source coverage plus one day; conflicts require resolution | Authoritative effective-dated source records or a richer lifecycle model |
| Current department-to-business-unit mapping for MVP | No verified historical reorganisation source | Invented effective-dated business-unit history; no business-unit analysis | Analytical assumption applied across periods, visibly labelled | Verified historical mappings become available |
| Payroll uses resolved assignment at actual payroll period end: analytical period-end attribution | Deterministic attribution aligned with actual source period, including clipped final periods | Calendar month-end regardless of actual period; current assignment; day-weighted allocation | No day weighting and no claim of authoritative payroll cost allocation | Approved allocation requirement and adequate historical inputs |
| Publish counts before final rate labels; approve exact formulas and inputs before rates | Avoids ambiguous denominators and misleading labels | Unqualified turnover/retention/promotion/transfer rates | Candidate formulas in the contract remain unapproved; require denominator, cohort, window and zero handling | Source-supported formula review and explicit approval |
| Retain restricted employee-level physical facts | Supports reusable analytical detail and reconciliations | Aggregate-only Gold; unrestricted detailed consumption | Later broader access uses approved aggregate/reporting models | Approved access/product requirement changes |
| Exclude demographics from MVP reporting dimensions | Keeps approved reporting scope limited | Include date of birth, gender or derived demographic fields | No demographic attributes or derived proxies in MVP reporting schema | Separate demographic reporting approval |
| Defer SQL serving-engine selection | Validate portable Spark Gold before choosing consumption technology | Select/provision a serving engine now | No serving infrastructure; dbt/Power BI technology decision later | Locally validated Spark Gold MVP and concrete serving requirements |
| Spark Gold owns reusable dimensions, assignment history, snapshots, facts, high-volume joins, deterministic derivations and Parquet | One reusable implementation for analytical semantics | Put all Gold logic in dbt; duplicate Spark/dbt logic | Future dbt owns reporting marts, semantic presentation, lightweight aggregates, BI views and approved KPI presentation | Evidence that a responsibility change improves the approved architecture, followed by explicit review |

Any later change must update this record and the affected schema, validation
and operational contracts before implementation. In particular, do not silently
switch the cutoff, snapshot frequency, history assumptions, attribution method
or rate denominator while implementing an individual model.


## Approved Phase 4 calendar reference refinement

`dim_date` covers `reporting_start` through the calendar month-end containing
`source_cutoff`. Business/source coverage remains bounded by `source_cutoff`.
The presence of a later reference date does not assert processed business activity.
Assignment intervals, movement events, workforce snapshots and actual payroll
periods retain their independent cutoff rules. The existing
`closed-month-overlap-v1` and `post-event-closing-headcount-v1` policies are unchanged.

This refinement is required because `fact_payroll.payroll_month_key` represents
the source period's calendar month-end, including incomplete current-month pay.
For cutoff 2024-04-15, the calendar includes 2024-04-30; payroll ending April 15
uses reporting key 20240430 and assignment on April 15. No April workforce
snapshot becomes eligible. The source cutoff is not extended.

## Approved Phase 5 attendance

Attendance is a daily source-grain fact. The source guarantees one record per
employee/work date, `Absent` alone contributes an absent day, and controlled
absence categories are retained. Assignment is historical at work date;
current employee organisation is not used. Decimal hours are reconciled
separately, attendance does not consume leave requests, and work-date
eligibility remains employment- and source-cutoff-bounded. See the [Phase 5
validation](../plans/gold-phase5-validation.md) record.

## Phase 6 MVP-wide validation

The ten approved MVP models are validated together through shared references,
dates, metadata, deterministic inventory and immutable release evidence.
Model-specific semantics remain authoritative; Phase 6 does not add a model or
alter cutoff, assignment, payroll, attendance, or workforce rules. See the
[Phase 6 validation](../plans/gold-phase6-validation.md) record.
