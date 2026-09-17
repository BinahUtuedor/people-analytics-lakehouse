# Gold Design Decisions

Status: the decisions below are approved for the design baseline. Phase 0
contracts and Phase 1/2/3 local transformations and validation are complete.
Phase 4 has not started. No production Gold release exists. [Gold layer](gold-layer.md) is the engineering contract and
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
