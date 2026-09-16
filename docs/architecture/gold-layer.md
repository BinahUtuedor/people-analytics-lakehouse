# Gold Layer Design Contract

## Status and authority

Gold data products are **design approved; no production release exists**. Phase 0
is complete and approved. Phase 1 core-dimension code and required Linux Parquet proof are complete;
Phase 2 complete: assignment and movement contract tests and local physical proof pass.
Phase 3 has not started.
No Gold job or CLI exists. This document records
the explicit approved decisions and is the authoritative repository Gold design
entry point. [Gold decisions](gold-decisions.md) records their rationale;
[implementation plan](../plans/gold-implementation-plan.md) controls delivery.

The approved architecture review, including all ten MVP logical schemas, is
recorded below with the user's explicit decisions. These schemas and grains
are authoritative; source/ORM fields do not expand the approved Gold schema.
Phase 0 encodes and validates this baseline in `spark/gold/`. Further phases
require separate approval; Gold transformations, jobs and infrastructure are not authorized here.

Source inspection: `database/models/`, `simulator/payroll.py`, and
`spark/silver/{transform,validate,job}.py`, at commit `7caa41f`. Silver retains
source columns, cleans strings, casts analytical types and deduplicates source
hash repeats. It does not provide authoritative effective-dated assignments.
No source data or AWS resources were accessed to prepare this specification.

## Purpose and boundaries

```text
Silver -> Spark Gold -> future dbt/reporting models -> BI / approved consumers
```

Gold creates reusable analytical dimensions, history, facts and snapshots from
an explicit Silver batch. Its restricted physical facts retain employee detail.
It is not a copy of Silver, an aggregate-only layer, or an ML feature store.
Broader consumption requires approved reporting/aggregate models. No serving
engine is selected; dbt and Power BI connectivity follow local MVP validation.

## Approved table catalogue and grain contracts

All grains and uniqueness checks apply **within one Gold build**. Conformed
dimensions include one reserved unknown member in addition to real members.
Source names below refer exclusively to the selected Silver batch. Facts also
reference Gold dimensions built from that same batch/release.

| Table | Type | Mechanically testable grain | Silver sources | Business purpose | Status |
| --- | --- | --- | --- | --- | --- |
| `dim_employee` | Dimension | One real row per `employee_id` | employees | Restricted employee identity and employment context | MVP |
| `dim_department` | Dimension | One real row per `department_id` | departments, business_units | Department and current business-unit mapping | MVP |
| `dim_location` | Dimension | One real row per `location_id` | locations | Office/geographical context | MVP |
| `dim_job_role` | Dimension | One real row per `role_id` | job_roles | Role and grade context | MVP |
| `dim_date` | Dimension | One row per calendar date, plus unknown | Generated calendar covering required source dates and reporting periods | Conformed calendar | MVP |
| `dim_employee_assignment` | Interval dimension | One row per employee and `valid_from_date`; no overlapping intervals | employees, transfers, promotions; recruitment and employee_exits as relevant lifecycle evidence | Resolved department, role, location and manager with evidence | MVP |
| `fact_employee_movement` | Event fact | One row per typed source event; unique `(movement_type, source_record_id)` | employees, employee_exits, promotions, transfers; recruitment as evidence only | Consolidated lifecycle event counts and headcount changes | MVP |
| `fact_workforce_monthly` | Periodic snapshot | One row per employee per closed month with employment overlap; unique `(employee_key, snapshot_month_key)` | employees, employee_exits, transfers, promotions; lifecycle evidence | Closing headcount and tenure by resolved organisation | MVP |
| `fact_payroll` | Transaction fact | One row per payroll record and recorded pay interval; unique `payroll_id` and employee/actual start/actual end | payroll; assignment sources | Payroll component analysis by actual period-end assignment | MVP |
| `fact_attendance` | Transaction fact | One row per `attendance_id`; also unique employee/work date | attendance; assignment sources | Recorded attendance, absence and hours | MVP |
| `fact_leave_request` | Transaction fact | One row per source leave request PK | leave_requests; employees | Request-level leave analysis, not expanded leave days | Post-MVP |
| `fact_recruitment` | Process fact | One row per source recruitment PK | recruitment; departments, job_roles, employees | Vacancy/recruitment analysis, not candidate-level history | Post-MVP |
| `fact_performance_review` | Event fact | One row per source review PK | performance_reviews; employees | Restricted review analysis | Post-MVP |
| `fact_training` | Event fact | One row per source training PK | training; employees | Employee training participation | Post-MVP |
| `fact_employee_survey` | Event fact | One row per source survey PK | employee_surveys; employees | Restricted survey analysis | Post-MVP |
| `fact_manager_feedback` | Event fact | One row per source feedback PK | manager_feedback; employees | Restricted feedback analysis | Post-MVP |
| `fact_exit_interview` | Event fact | One row per source interview PK | exit_interviews; employee_exits, employees | Restricted exit-interview analysis | Post-MVP |

Post-MVP grains are source-record design boundaries, not permission to implement.
No additional dimensions or facts are approved by implication.

The eventual catalogue count may equal Silver's dataset count, but the models
are different: business units fold into departments, lifecycle events consolidate,
and Gold introduces a date dimension, assignment intervals and monthly snapshots.

## Complete MVP logical schemas

Each model's schema is its table below **plus the four common metadata fields**.
No other business columns are implicitly approved. Nullability exceptions for
unknown members do not permit nulls on real members. All grains apply within
one selected Gold release.

### Common metadata — every MVP table

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `_source_batch_id` | STRING | Silver `_batch_id` | No | Explicit Silver input batch |
| `_gold_build_id` | STRING | Deterministic release identity | No | Links the row to Gold release/manifest |
| `_gold_generated_at` | TIMESTAMP | Fixed UTC timestamp for accepted build | No | Build audit timestamp |
| `_record_hash` | STRING | SHA-256 of canonical Gold business columns | No | Business-content fingerprint |

Unknown dimension members carry the same release metadata and are excluded
from real-source entity-count reconciliation.

### `dim_employee`

Grain: one row per employee per selected Gold release. PK: `employee_key`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `employee_key` | BIGINT | `employees.employee_id` | No | PK; 0 reserved for unknown |
| `employee_number` | STRING | `employee_number` | Yes, unknown only | Restricted business identifier |
| `hire_date_key` | INT | Date key from `hire_date` | No | Hire-date role |
| `termination_date_key` | INT | Date key from `termination_date`; 0 if absent | No | Exit-date role |
| `current_employment_status` | STRING | `employment_status` | Yes, unknown only | State observed in selected source batch |
| `current_employment_type` | STRING | `employment_type` | Yes, unknown only | Current type; not reconstructed history |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

Exclude names, email, date of birth, gender, salary, contract type and current
organisational attributes. Organisation history belongs in assignment intervals.

### `dim_department`

Grain: one row per department per selected Gold release. PK: `department_key`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `department_key` | BIGINT | `department_id` | No | PK; 0 reserved |
| `department_name` | STRING | `department_name` | No | Department name |
| `cost_center` | STRING | `cost_center` | Yes, unknown only | Source cost-centre identifier |
| `business_unit_id` | BIGINT | `departments.business_unit_id` | No | Source BU ID; 0 for unknown |
| `business_unit_name` | STRING | Join `business_units.unit_name` | No | Flattened current BU hierarchy |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

The current business-unit mapping is an MVP analytical assumption, explicitly
labelled in the release assumptions and reporting documentation; it is not
verified historical reorganisation data. No extra history field is added here.

### `dim_location`

Grain: one row per location per selected Gold release. PK: `location_key`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `location_key` | BIGINT | `location_id` | No | PK; 0 reserved |
| `office_name` | STRING | `office_name` | No | Office label |
| `city` | STRING | `city` | No | City |
| `country` | STRING | `country` | No | Country |
| `timezone` | STRING | `timezone` | Yes, unknown only | Source office timezone |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

### `dim_job_role`

Grain: one row per job role per selected Gold release. PK: `job_role_key`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `job_role_key` | BIGINT | `role_id` | No | PK; 0 reserved |
| `role_name` | STRING | `role_name` | No | Role description |
| `grade` | STRING | `grade` | No | Source grade; do not invent numeric ranking |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

### `dim_date`

Grain: one row per calendar date, plus one unknown member. PK: `date_key`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `date_key` | INT | `YYYYMMDD`; 0 unknown | No | PK |
| `calendar_date` | DATE | Generated calendar | Yes on unknown | Actual date |
| `day_of_month` | INT | Calendar date | Yes on unknown | 1–31 |
| `iso_day_of_week` | INT | Calendar date | Yes on unknown | Monday=1 through Sunday=7 |
| `day_name` | STRING | Calendar date | Yes on unknown | Display label |
| `iso_week_number` | INT | ISO calendar | Yes on unknown | ISO week |
| `iso_week_year` | INT | ISO calendar | Yes on unknown | ISO week-year |
| `month_number` | INT | Calendar date | Yes on unknown | 1–12 |
| `month_name` | STRING | Calendar date | Yes on unknown | Display label |
| `year_month` | INT | `year * 100 + month` | Yes on unknown | Sortable month |
| `month_start_date` | DATE | First date of month | Yes on unknown | Period start |
| `month_end_date` | DATE | Last date of month | Yes on unknown | Period end |
| `calendar_quarter` | INT | Calendar date | Yes on unknown | 1–4 |
| `calendar_year` | INT | Calendar date | Yes on unknown | Calendar year |
| `is_weekend` | BOOLEAN | ISO weekday 6/7 | Yes on unknown | Calendar attribute only |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

Closed-period eligibility belongs to the fixed build cutoff, not a
wall-clock-dependent `is_current_month` field. **Fiscal calendar requires business definition**.

### `dim_employee_assignment`

Grain: one row per employee per constant-assignment interval. PK:
`assignment_key`; unique `(employee_key, valid_from_date)` for real intervals.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `assignment_key` | STRING | Hash of namespace + employee ID + interval start | No | PK; `"0"` reserved |
| `employee_key` | BIGINT | Employee identifier | No | Employee FK |
| `valid_from_date` | DATE | Hire/change boundary | Yes, unknown only | Inclusive interval start |
| `valid_to_exclusive` | DATE | Next boundary / coverage end | Yes, unknown only | Exclusive interval end |
| `department_key` | BIGINT | Transfer/current-state reconstruction | No | Department FK; 0 when legitimately unknown |
| `job_role_key` | BIGINT | Promotion/current-state reconstruction | No | Role FK |
| `location_key` | BIGINT | Transfer/current-state reconstruction | No | Location FK |
| `manager_employee_key` | BIGINT | Transfer/current-state reconstruction | No | Manager employee FK; 0 if absent |
| `department_history_basis` | STRING | Reconstruction evidence | No | Event-derived/current-state-assumed/unknown |
| `role_history_basis` | STRING | Reconstruction evidence | No | Role-history qualification |
| `location_history_basis` | STRING | Reconstruction evidence | No | Location-history qualification |
| `manager_history_basis` | STRING | Reconstruction evidence | No | Manager-history qualification |
| `is_unknown` | BOOLEAN | Generated member flag | No | Reserved-member indicator |

Real intervals are half-open and clipped to known employment/source coverage.
The exclusive end is bounded by the next change, termination date + 1 day,
or source cutoff + 1 day. This is reconstructed history, not complete source SCD history.

### `fact_workforce_monthly`

Grain: one row per employee per closed month with employment overlap.
PK: `(employee_key, snapshot_month_key)` within a build; no synthetic snapshot PK.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `employee_key` | BIGINT | Employee ID | No | Employee FK |
| `snapshot_month_key` | INT | Month-end date key | No | Snapshot date FK |
| `reporting_year` | INT | Snapshot year | No | Physical partition |
| `assignment_date_key` | INT | Earlier of month-end and termination date | No | Organisation attribution reference date |
| `assignment_key` | STRING | Assignment at reference date | No | History/audit FK |
| `department_key` | BIGINT | Resolved assignment | No | Reporting department |
| `job_role_key` | BIGINT | Resolved assignment | No | Reporting role |
| `location_key` | BIGINT | Resolved assignment | No | Reporting location |
| `manager_employee_key` | BIGINT | Resolved assignment | No | Reporting manager |
| `headcount_eom` | INT | Approved closing-headcount predicate | No | 0 or 1 |
| `tenure_days_eom` | INT | Month end minus hire date when HC=1 | Yes | Month-end tenure; null for exited rows |

With no termination, the assignment reference is month-end. Sum `headcount_eom`
at one snapshot date, not row count. Do not sum tenure without dividing by the
contributing closing population.

### `fact_payroll`

Grain: one row per payroll record and recorded pay interval. PK: `payroll_id`.
Also require unique `(employee_key, pay_period_start_key, pay_period_end_key)`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `payroll_id` | BIGINT | `payroll_id` | No | Source record key |
| `employee_key` | BIGINT | `employee_id` | No | Employee FK |
| `pay_period_start_key` | INT | Explicit conversion of `pay_period_start` | No | Actual period start |
| `pay_period_end_key` | INT | Explicit conversion of `pay_period_end` | No | Actual period end |
| `payroll_month_key` | INT | Calendar month-end key for source period | No | Reporting month |
| `reporting_year` | INT | Payroll month year | No | Partition |
| `pay_period_days` | INT | End - start + 1 | No | Calendar days covered |
| `assignment_key` | STRING | Assignment on actual period end | No | Analytical attribution |
| `department_key` | BIGINT | Resolved assignment | No | Reporting department |
| `job_role_key` | BIGINT | Resolved assignment | No | Reporting role |
| `location_key` | BIGINT | Resolved assignment | No | Reporting location |
| `manager_employee_key` | BIGINT | Resolved assignment | No | Reporting manager |
| `currency` | STRING | Payroll currency | No | Mandatory monetary grouping |
| `payroll_status` | STRING | Source status | No | Status filter |
| `base_salary` | DECIMAL(18,2) | Validated source value | No | Period base pay |
| `overtime_pay` | DECIMAL(18,2) | Source value | No | Overtime component |
| `bonus` | DECIMAL(18,2) | Explicit conversion | No | Bonus |
| `deductions` | DECIMAL(18,2) | Explicit conversion | No | Recorded total deductions |
| `pension_contribution` | DECIMAL(18,2) | Explicit conversion | No | Recorded contribution; do not presume employer cost |
| `tax_amount` | DECIMAL(18,2) | Source value | No | Recorded tax |
| `gross_pay` | DECIMAL(18,2) | Source value | No | Recorded gross |
| `net_pay` | DECIMAL(18,2) | Source value | No | Recorded net |

Do not reconstruct salary history to replace source payroll values. Attribution
is assignment at actual `pay_period_end`, labelled **analytical period-end attribution**
in the release contract and reporting descriptions, not an extra fact column.

### `fact_attendance`

Grain: one row per attendance record. PK: `attendance_id`.
Also require unique `(employee_key, work_date_key)`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `attendance_id` | BIGINT | Source `attendance_id` | No | Source record key |
| `employee_key` | BIGINT | `employee_id` | No | Employee FK |
| `work_date_key` | INT | `work_date` | No | Date FK |
| `reporting_year` | INT | Work-date year | No | Partition |
| `assignment_key` | STRING | Assignment on work date | No | Historical attribution |
| `department_key` | BIGINT | Resolved assignment | No | Department FK |
| `job_role_key` | BIGINT | Resolved assignment | No | Role FK |
| `location_key` | BIGINT | Resolved assignment | No | Location FK |
| `manager_employee_key` | BIGINT | Resolved assignment | No | Manager FK |
| `attendance_status` | STRING | Source `status` | No | Controlled status |
| `absence_reason` | STRING | Source `absence_reason` | Yes | Restricted descriptive reason |
| `hours_worked` | DECIMAL(18,4) | Source | No | Recorded hours |
| `overtime_hours` | DECIMAL(18,4) | Source | No | Recorded overtime |
| `recorded_day_count` | INT | Constant 1 | No | Recorded employee-day denominator |
| `absent_day_count` | INT | 1 when governed status is Absent; otherwise 0 | No | 0/1 indicator |

Clock-in/out fields are not required in the MVP analytical fact.

### `fact_employee_movement`

Grain: one row per typed HIRE, EXIT, PROMOTION or TRANSFER source event.
PK: `movement_key`; unique business key: `(movement_type, source_record_id)`.

| Column | Type | Source/derivation | Nullable | Description |
| --- | --- | --- | --- | --- |
| `movement_key` | STRING | Deterministic namespace/type/source-ID encoding | No | Event PK |
| `movement_type` | STRING | HIRE/EXIT/PROMOTION/TRANSFER | No | Consolidated event type |
| `source_dataset` | STRING | Contributing event dataset | No | Row-level source discriminator |
| `source_record_id` | BIGINT | Employee ID for hire; source event ID otherwise | No | Source lineage key |
| `employee_key` | BIGINT | Employee ID | No | Employee FK |
| `event_date_key` | INT | Hire/exit/promotion/transfer date | No | Event date FK |
| `before_assignment_key` | STRING | Assignment immediately before event | No | `"0"` where not applicable |
| `after_assignment_key` | STRING | Assignment immediately after event | No | `"0"` where not applicable |
| `before_department_key` | BIGINT | Before assignment | No | 0 where N/A |
| `after_department_key` | BIGINT | After assignment | No | 0 where N/A |
| `before_job_role_key` | BIGINT | Before assignment | No | Old role |
| `after_job_role_key` | BIGINT | After assignment | No | New role |
| `before_location_key` | BIGINT | Before assignment | No | Old location |
| `after_location_key` | BIGINT | After assignment | No | New location |
| `before_manager_employee_key` | BIGINT | Before assignment | No | Old manager |
| `after_manager_employee_key` | BIGINT | After assignment | No | New manager |
| `event_count` | INT | Constant 1 | No | Additive event count |
| `headcount_delta` | INT | HIRE +1, EXIT -1, internal move 0 | No | Workforce movement |
| `exit_type` | STRING | `employee_exits.exit_type` | Yes | Null for non-exit |
| `voluntary_flag` | BOOLEAN | Authoritative exit flag | Yes | Null for non-exit |
| `regrettable_flag` | BOOLEAN | Authoritative exit flag | Yes | Null for non-exit |

EXIT before means final assignment while employed on the exit date. Independent
same-day internal events share the combined date boundary; each source event
counts once, while each attribute transition must be counted only once.
Promotion salary measures are deferred because promotion events have no explicit
currency. Exit reason and transfer type/reason are also outside this approved
MVP schema; source availability does not add fields.

## Key strategy

Preserve source/business identities for reconciliation. Source IDs are scoped
to this source system; identifiers must not be assumed globally unique across
future HR systems. Resolve all fact dimension FKs during Spark processing.
Check each FK against the same accepted build, including before/after employee
and assignment roles. Do not silently discard records with unresolved keys.

Employee, department, location and role keys retain the source IDs as BIGINT.
Date keys are INT `YYYYMMDD`. `assignment_key` hashes the canonical namespace,
employee ID and interval start; `movement_key` encodes namespace, movement type
and source ID unambiguously. Phase 0 pins their serialization and hash utilities.
The workforce PK is the composite `(employee_key, snapshot_month_key)`; payroll
and attendance retain their source PKs. No additional surrogate fact keys are
approved. Never use partition-dependent row numbers, random UUIDs, ingestion
order or build timestamps for logical keys. Check both PK and business-grain
uniqueness, including payroll period and attendance employee/date uniqueness.

Reserve numeric **0** in numeric dimensions and string **"0"** for the assignment
unknown member. All dimensions include `is_unknown`; real members are false,
and each dimension has exactly one true member. Non-null descriptive labels on
unknown members use `Unknown`; date attributes and assignment interval dates
are null as their schemas specify. Unknown assignment FKs are 0 and its four
history-basis values are `unknown`. Unknown employees' date keys are 0.
Validate positive real source IDs so reserved members cannot collide with them.

Fact employee keys must resolve to real employees. A missing optional manager or
legitimately unknown assignment attribute may use 0 under its documented basis;
unexpected missing required parents fail. Use assignment "0" only when genuinely
not applicable, such as HIRE before or EXIT after. Do not use unknown members to
conceal contradictory history or invalid mandatory dates. Date FKs all resolve
to `dim_date`, including absence-of-termination key 0; manager FKs resolve to
`dim_employee`. Flattened fact organisation keys must equal their referenced
assignment's keys. Business-unit ID is validated against Silver business units;
there is no separate Gold business-unit dimension.

## Historical assignment contract

Use half-open intervals `[valid_from_date, valid_to_exclusive)` over employee,
department, role, location and manager. Resolve before and after event state
from employees, transfers, promotions and relevant lifecycle evidence. Transfer
records expose old/new department, location and manager; promotions expose
old/new role. Recruitment offers contextual evidence but must not be assumed to
encode effective-dated hire assignment without corroboration.

Every interval exposes its four approved history-basis fields, distinguishing
`event-derived`, `current-state-assumed` and `unknown` for each attribute.
Source event identities remain traceable through the explicit input batch and
reconstruction reconciliation evidence; do not add unapproved event-ID columns
to the dimension. A single claim of authoritative history is insufficient when
some attributes are assumed. Validate old/new chains against employees' current
state; conflicting evidence fails rather than being silently replaced.

Current-state assumptions are permitted only as labelled synthetic-baseline
assumptions where effective-dated evidence cannot prove a value. They are not
authoritative source history. The current department-to-business-unit mapping
applies throughout the MVP as an analytical assumption, not verified historical
reorganisation data. Do not construct effective-dated business-unit history.

Coalesce same-day changes to a consistent daily boundary, without zero-length
intervals. Source dates do not prove intraday sequencing: conflicting changes
to the same attribute require explicit resolution, not arbitrary source-ID order.
Test one matching interval per required reference date and no overlaps.
Employment eligibility is separate from assignment lookup. In particular, exit
day activity/payroll may require an assignment although closing headcount is zero.
For each real interval, the exclusive end is the earliest applicable next
assignment boundary, termination date + 1 day, or source cutoff + 1 day. Start
at hire or a subsequent change boundary; require start < end. This retains the
exit date for activity, payroll and exit-event final assignment without counting
the employee in post-event closing headcount. Do not clamp payroll dates to the
previous day or leave real intervals unbounded.

## Workforce snapshot semantics

MVP snapshots cover **closed calendar months only**. Supply a fixed source
coverage cutoff and reporting range in the release specification; never infer
closure from a runtime clock alone. A snapshot date is the calendar month-end.
No daily or partial-month/as-of product is included.

Post-event closing headcount at date D counts an employee exactly when
`hire_date <= D` and there is no exit on or before D. Reconcile exit events with
employee termination state before applying this rule. An exit on D therefore
has closing headcount zero. Current `is_active` cannot determine past headcount.
Activity facts may use `hire_date <= activity_date <= termination_date`.

For month start S and month end D, include exactly the employees with
`hire_date <= D` and `(termination_date IS NULL OR termination_date >= S)`.
This employment-overlap rule retains exit-month rows, including an exit on S
or D. `headcount_eom` is 1 only under the post-event closing predicate and 0
otherwise. Row count therefore includes some employees no longer in closing
headcount; sum `headcount_eom` at one `snapshot_month_key` instead.

`assignment_date_key` is the earlier of month-end and termination date, using
month-end when termination is absent. Resolve assignment and the flattened
organisation FKs at that date. `tenure_days_eom` is date difference D minus hire
date (no inclusive +1) when `headcount_eom = 1`, otherwise null. It never accrues
for exited rows. For average closing tenure, divide the sum of tenure days by
the contributing closing population, returning no average for zero population.
Headcount is semi-additive over time: never sum it across monthly snapshots.

## Movement semantics

| Event | Authoritative event input | Before / after meaning | `event_count` | `headcount_delta` | Event-specific source semantics |
| --- | --- | --- | --- | --- | --- |
| HIRE | employees.hire_date, employee_id | No employed assignment / resolved hire assignment | 1 | +1 | Recruitment is supporting evidence, not a second hire |
| EXIT | employee_exits.exit_event_id, exit_date | Final assignment while employed on exit date / no employed assignment | 1 | -1 | Exit type and voluntary/regrettable flags |
| PROMOTION | promotions.promotion_id, promotion_date | Before / after combined date boundary | 1 | 0 | Old/new role via assignment; salary measures deferred (no source currency) |
| TRANSFER | transfers.transfer_id, transfer_date | Before / after combined date boundary | 1 | 0 | Old/new department, location and manager via assignment |

Reconcile HIRE dates with employee state and EXIT with termination evidence;
do not emit a second exit from the employee table. Preserve event-specific nulls
for inapplicable fields rather than making them look like real zero values.
By contrast, inapplicable assignment/dimension FKs use the reserved members.
For HIRE, before is "0" and after is the resolved hire-date assignment; for
EXIT, before resolves on exit date and after is "0". For internal changes on D,
before is the interval ending at D and after the interval beginning at D.
Independent changes on D share that combined boundary; date-only source records
do not establish an intraday sequence.

Simultaneous promotion and transfer remain two source events, but their shared
before/after daily assignment must not yield two department moves or two role
moves. Attribute transitions are counted once per employee/date/attribute;
transfer owns its organisation changes and promotion owns its role change.
Hires/exits alter membership rather than generating additional internal moves.
Do not sum generic assignment differences independently on every event row.
Ambiguous competing same-day changes fail validation pending evidence resolution.

## Payroll attribution and components

One source payroll record becomes one fact row. Attribute organisation using
the resolved assignment at **actual `pay_period_end`**, labelled **analytical
period-end attribution**. The simulator clips the final period to termination
and the initial period to hire; do not substitute calendar month-end. This is
not authoritative source cost-allocation history. No day-weighted allocation.
Convert both period boundaries explicitly to dates, calculate inclusive
`pay_period_days`, and derive `payroll_month_key` from the source period's calendar
month end. Validate that the current monthly source interval lies in that month;
do not split a record or invent a cross-month allocation. `reporting_year` is the
year of `payroll_month_key`; it does not change assignment at actual period end.
Preserve source payroll amounts rather than substituting reconstructed salary.

Preserve currency and aggregate monetary amounts only within currency; no
implicit conversion. Use explicit fixed-point monetary typing and reject
overflow or lossy casts. Source `base_salary`, `overtime_pay`, `bonus`,
`deductions`, `pension_contribution`, `tax_amount`, `gross_pay` and `net_pay`
retain separate semantics. `gross + net + deductions` is not a valid cost total.

The current simulator derives gross = base + overtime + bonus, deductions =
pension + tax, and net = gross - deductions. Source values are individually
rounded for persistence: document the justified rounding tolerance during the
schema contract review rather than inventing a zero-tolerance equality or
recomputing components. Employer total cost cannot be inferred from these fields.
Reconcile each approved component by source ID, period and currency before and
after assignment joins; joins must never multiply payments.

## Attendance semantics

One Silver attendance record becomes one fact row; enforce both attendance ID
uniqueness and `(employee_id, work_date)` uniqueness. Do not silently choose one
of conflicting daily records. Preserve source attendance status. Recorded day
count is 1 per record, not a claim that every expected working day was recorded.
Absent day count is 1 for the governed absent status, otherwise 0; validate the
status against governed reference data and do not infer absence from missing rows.

Preserve hours worked and overtime as separate source measures; establish from
source generation whether overtime is included before constructing any combined
hours measure. Reconcile each separately. Retain restricted absence reason only
in approved detailed access, excluding it from default reporting. Validate
inclusive employment windows and resolve work-date assignment in Spark.

## Dates and rates

Date keys use integer `YYYYMMDD`; the unknown member uses key 0 and no fabricated
real date. Include calendar day fields, ISO week and ISO week-year, calendar
month, quarter and year in the agreed field inventory. ISO week-year can differ
from calendar year. Validate leap days, month ends and year boundaries.
**Fiscal calendar requires business definition**. Do not invent fiscal years or
a single global holiday calendar.

Counts can precede rates. No final turnover, retention, promotion-rate or
transfer-rate labels are approved here. Candidate formulas for later review:

| Candidate (not approved KPI) | Explicit proposed numerator / denominator | Required decisions/evidence |
| --- | --- | --- |
| Monthly turnover | Exits in month / mean of prior and current closing headcount | Confirm population, exit scope, organisation attribution, and suitability of two-point average |
| Cohort retention | Employees in opening cohort still employed at closing / opening cohort employees | Fix opening instant, interval, rehire treatment and cohort organisation |
| Promotion incidence | Distinct promoted opening-cohort employees during month / opening cohort employees | Confirm eligible cohort and handling of new hires/multiple events |
| Transfer incidence | Distinct transferred opening-cohort employees during month / opening cohort employees | Define qualifying transfer attributes and eligibility |

Require evidence for every numerator and denominator, zero-denominator handling,
time window and filter scope before approval. Return no rate for an undefined
denominator; do not label an event count divided by an arbitrary population as a
standard rate. The source has no general multiple-employment-spell history.

## Technical metadata and reproducibility

All models carry the four common metadata fields above. `_record_hash` covers
all persisted analytical columns, including keys, flags, amounts and
history-basis fields. Exclude the four common technical fields and physical
file paths. Use sorted column names, typed canonical serialization with explicit
nulls, stable decimal/date encodings and SHA-256. Pin the schema/serialization
version; Silver's source `_record_hash` is not a Gold analytical content hash.

The build ID identifies one reproducible release specification: explicit Silver
batch and input fingerprints, transformation/code revision, schema/key/hash
versions, reporting range and closure cutoff, assumption policies and relevant
runtime configuration. Use a deterministic digest of that canonical manifest.
Fix `_gold_generated_at` in the release manifest and reuse it on verification
and reproducibility checks of that build; volatile wall-clock timestamps
must not create different analytical keys or hashes. Reproducibility concerns
rows and logical content, not identical Parquet file names or byte ordering.

## Physical storage convention

Approved design convention; no bucket is accessed or created by this document.
Use equivalent relative layouts under a configured local root for validation.

| Model class | Layout | Partition rule |
| --- | --- | --- |
| All dimensions, including assignment | `s3://<bucket>/gold/<model>/build_id=<gold-build-id>/` | No year partition for small dimensions; keep complete interval history together in MVP |
| Payroll and attendance facts | `s3://<bucket>/gold/<model>/reporting_year=<year>/build_id=<gold-build-id>/` | Year of payroll reporting month or work date respectively; `reporting_year` is an approved field |
| Movement fact | `s3://<bucket>/gold/fact_employee_movement/build_id=<gold-build-id>/` | No year partition in MVP: the approved movement schema has no `reporting_year`; do not add it through partition discovery |
| Monthly workforce snapshot | `s3://<bucket>/gold/fact_workforce_monthly/reporting_year=<year>/build_id=<gold-build-id>/` | Snapshot calendar year |
| Release manifest | `s3://<bucket>/gold/_releases/build_id=<gold-build-id>/` | Release-wide input, model/partition and validation inventory |

Do not partition by employee. The manifest lists all expected partitions,
including explicitly empty models/periods. Read accepted releases by manifest,
never glob all builds or infer the latest release from object modification time.
Unknown/missing mandatory fact dates fail before year partitioning.

## Build/release behaviour

```text
Resolve inputs -> validate contracts -> transform -> validate analytics
    -> write -> read back -> reconcile -> accept release
```

Require one explicit Silver source batch and complete model dependencies; no
implicit latest-batch selection or cross-batch joins. Validate source contracts
even when Silver validation passed: its current validator is not a full Gold
business-key/schema/history gate.

Validate all analytics before publication. Write immutable Parquet using
duplicate protection; never append, silently overwrite or delete accepted or
partial output. A release is accepted only after physical readback, content
hash comparison, model reconciliations and cross-model FK checks all pass.
Per-directory `_SUCCESS` alone is not release acceptance. Publish acceptance
with an exclusive release claim; reject competing writers for the same build.
Partial output remains unaccepted and requires explicit operator resolution.

Verification-only mode performs no writes, including to missing partitions or
manifests. It checks the exact release specification, expected partition set,
schemas, hashes, grains, FKs and reconciliations; missing or changed output
fails. Do not copy Bronze/Silver's publish-missing resume behaviour into this
mode. Rebuild verification uses an isolated local destination and compares
logical content, not overwriting an existing release.

Future operational documentation must use: `command -> prerequisites -> what
happens internally -> input -> output -> side effects -> success condition`.
There is no Gold runbook until runnable commands exist.

## Data-quality contracts

Common gates: exact approved schema and nullability; explicit cast failures;
PK and grain uniqueness; FK completeness; unknown-member correctness; source
batch/build consistency; deterministic content hashes; duplicate-safe writes;
readback equality; governed values; no excluded reporting fields. Log model,
batch/build, input/output paths, counts, result and elapsed time without secrets
or restricted free text. Failure prevents release acceptance.

| Model | Required controls and reconciliation |
| --- | --- |
| Core dimensions | Exactly one real member per eligible Silver source key plus unknown; department-business-unit FK; no join fan-out; employee lifecycle consistency |
| Date | Unique/continuous declared range, valid `YYYYMMDD`, ISO boundaries, leap days, one unknown member |
| Assignment | Positive/nonoverlapping intervals; contiguous coverage from hire to min(termination, source cutoff) inclusive; source old/new continuity; current-state endpoint reconciliation; all four basis fields valid; same-day conflict handling |
| Movement | Source-event coverage by type and ID; unique movement key and `(movement_type, source_record_id)`; source dataset/type agree; no duplicated hire/exit; event count 1; signed deltas; final exit-date assignment; one attribute transition despite simultaneous events; no promotion salary fields |
| Workforce monthly | Closed periods only; exact employment-overlap population and employee/month PK; cutoff fixtures including hire/exit on month-end and same-day hire/exit; earlier-of-month-end/exit assignment; tenure null exactly for HC=0, else month-end minus hire; sum HC flags equals independently reconstructed closing population; opening + hires - exits = closing at organisation-wide level |
| Payroll | Source PK and employee/actual-period uniqueness; exact source-record coverage; start <= end and positive inclusive period days; monthly period and reporting-year consistency; fixed monetary types; component identities with reviewed rounding; per-currency component sums; actual period-end assignment; no join fan-out |
| Attendance | Source-record coverage; employee/work-date uniqueness; governed status; recorded/absent flag domain; separate source hours/overtime sums; inclusive employment date checks; no inference from missing days |

For departmental movement reconciliation, account for resolved internal inflows
and outflows once. Unknown/current-assumed attribution must be visible in
reconciliation, not silently dropped. Gold total rows need not equal Silver
total rows: dimensions add unknowns, events consolidate sources, assignment
history expands intervals, and snapshots expand periods. One-to-one activity
facts do require model-specific source coverage. Reconcile flattened organisation
keys against the interval FK on every fact, with movement's before/after roles
and reserved-member exceptions treated explicitly.

## Governance and consumption

Exclude names, email, date of birth, gender, salary, contract type and current
organisation fields from `dim_employee`; no derived demographics belong in MVP
reporting. Its employee number is a restricted business identifier. Salary bands
are excluded from `dim_job_role`; promotion compensation is deferred from movement
because no explicit source currency exists. Restrict payroll compensation and
employee-level linkage. Absence/leave reasons
are sensitive; performance, surveys, manager feedback and exit interviews stay
post-MVP and require separate access review.

Separate free text (notes, comments and narrative reasons) from default
reporting models. Do not recreate excluded attributes as derivations. Broader
aggregate products require a future small-population suppression policy and
approved access controls; no legal rules or numeric thresholds are invented.

Power BI models should use conformed dimensions, dimension-to-fact one-to-many
relationships and single-direction filters. Use role-playing date and employee
dimensions for period start/end, before/after or manager roles as appropriate.
No fact-to-fact relationships and no reliance on interval joins in BI: Spark
materialises the correct FKs first. Avoid ambiguous alternative relationship
paths through assignment and direct organisation keys. Headcount is evaluated
at a selected snapshot, never summed across monthly periods.

Spark owns reusable conformed dimensions, assignment reconstruction, snapshots,
facts, high-volume joins, deterministic reusable derivations and Parquet.
Future dbt owns reporting marts, semantic presentation, lightweight business
aggregations, BI-facing views and approved KPI presentation. Do not duplicate
logic across them. Neither serving technology nor infrastructure is selected.

Reporting Gold is not an ML feature store. Future point-in-time features,
observation windows and leakage prevention belong in `analytics/` or another
separately approved feature layer; do not treat reconstructed reporting history
as proof of when an attribute became known.


## Phase 0 executable contract foundation

The approved Phase 0 foundation in `spark/gold/` provides the following contracts
(the Phase 1 additions are described separately below):

- `contracts.py`: immutable ten-model registry, exact ordered business fields
  followed by the four metadata fields, fresh `get_gold_schema(model)` Spark
  schemas, and deterministic reserved-member business rows. Schema-nullable
  fields restricted to unknown members retain that additional rule. Source
  dependencies include assignment/lifecycle evidence; `dim_date` has no direct
  Silver dependency. No ORM-derived fields or business transformations exist.
- `keys.py`: positive signed-BIGINT source-ID checks, explicit `date`/`None`
  date keys, and namespace-scoped SHA-256 assignment/movement keys. Key encoding
  frames `gold-key`, key version, serialization version, namespace, key kind,
  then employee ID/start date or movement type/source ID. Every value is a
  separate length-safe frame; the unknown assignment key remains `"0"`.
- `hashing.py`: the Python reference oracle for serialization `v1`. A frame is
  ASCII decimal UTF-8 byte length, colon, then unchanged UTF-8 bytes. A record
  starts with framed `gold-record`, version and model name, followed by sorted
  business column names. Each column frames its name, approved logical type and
  `N` (null) or `V` (value); only `V` has a following value frame. Integers use
  base ten, booleans lowercase `true`/`false`, dates ISO `YYYY-MM-DD`, and decimals
  the exact schema scale, without exponent or negative zero. Lossy rounding,
  overflow and unsupported types fail. SHA-256 produces lowercase hex. All four
  metadata fields and physical lineage paths are outside this record encoding.
  Phase 1 adds `spark_record_hash`, using built-in UTF-8/binary-length/concat/
  cast/date/conditional/hash expressions that match this reference. The original
  Phase 0 reference encoding and version remain unchanged.
- `manifest.py`: typed build specification, fixed UTC audit timestamp, exact
  model/partition inventories, release statuses and strict existing-output
  policy. Build JSON uses sorted string keys, compact separators, ASCII escapes,
  explicit date strings and no floating values. The build digest covers a
  `gold-build` envelope and the spec, including source namespace, one explicit
  Silver batch, per-dataset logical-content SHA-256 fingerprints, code revision,
  all four contract versions, reporting bounds/cutoff, approved assumption IDs
  and typed deterministic runtime settings. File names, roots and execution
  timestamps have no build-spec fields. Assumption IDs label the existing
  approved decisions; alternative policies/versions can be compared by identity
  but cannot instantiate a supported release. Input fingerprint computation is
  deferred until readers exist; it must remain independent of physical layout.
- `validate.py`: typed check rules, read-only checker protocol, contextual
  results and deterministic failure aggregation. Exact schema and reference
  member-row checks execute now. PK/grain, FK, governed values, numeric/date,
  hash, source coverage and aggregate reconciliation have reusable interfaces;
  distributed/model-specific checks remain later-phase work. Reports preserve
  `check_name`, `passed`, `record_count` (violating records) and `severity`, plus
  model, batch, build, explicit execution timestamp and reconciliation metrics.

Schema, serialization/hash, key and manifest versions are each `v1`. Changing
any material version changes build identity. Unknown constructors return only
business values; future callers must attach the fixed release metadata and
canonical record hash. They never create fact rows or repair mandatory FKs.

A manifest lists exactly all ten MVP models and explicit partition sets, even
when empty. Unpartitioned models have one unpartitioned entry. Counts and
logical-content fingerprints can be absent while BUILDING; VALIDATED/ACCEPTED
require counts, fingerprints, reconciliation metrics and successful evidence for
all declared verification gates. These are evidence contracts, not a physical
acceptance implementation or a substitute for an exclusive release claim.
A `_SUCCESS` file alone never means acceptance.

`VerificationRequest` has no write capability or repair options. Its executable
inventory check requires the exact build, fixed audit timestamp and model/partition
set; schema, content/hash, grain, FK, reconciliation and physical readback are
mandatory future verifier obligations. Verification never mutates manifests or
promotes acceptance. Creation rejects every existing state, including partial
output without a manifest (represented as BUILDING). No append, overwrite,
automatic deletion or implicit latest selection is allowed.

There is no Gold CLI or runbook. Contract tests use the existing unittest
entry point; test execution is documented in the implementation plan.


## Phase 1 core dimensions - local proof complete

The five Phase 1 dimensions (`dim_date`, `dim_employee`, `dim_department`,
`dim_location` and `dim_job_role`) are built in `spark/gold/transform.py`.
Inputs are explicitly supplied Silver-conformed DataFrames, never discovered
Raw/Bronze tables or an implicit latest batch. No input reader or Gold CLI is
introduced. All outputs select only central-contract fields, append the shared
unknown member, and attach a single `DimensionBuild`'s batch/build/UTC timestamp.
The approved full-MVP `BuildSpec` is reused without changing its schema, versions
or input-fingerprint contract. Phase 1 does not compute source fingerprints or
accept the full ten-model release; those identities are supplied by the caller.

The declared Phase 1 date range is `BuildSpec.reporting_start` through
`BuildSpec.source_cutoff`, inclusive, and therefore participates in build identity.
Callers must provide bounds covering every required source date. An employee
hire/termination date outside the supplied calendar fails; transforms never
extend coverage using runtime clocks. This initial callable boundary does not
provide a separate historical calendar-start option outside the build spec.
Calendar generation uses Spark range/date expressions. ISO year uses the year
of the week's Thursday, and ISO weekdays run Monday=1 through Sunday=7.
There are no fiscal, holiday, demographic or current-month attributes.

`dimension_validation.py` adds executable source, schema, unknown-member,
metadata/hash, PK, lifecycle, date FK and source-value/coverage checks using
Phase 0 validation results. Source IDs must be positive BIGINTs; consumed fields
must already have Silver types and standardized text. Employee-number and
cost-centre uniqueness follow the source contracts. Department and BU keys are
checked before the current-mapping join, preventing fan-out; orphan BUs fail.
Location timezone is preserved as required source text, without enrichment.
Real rows reconcile to eligible source entities, and total rows add exactly one
unknown. The calendar additionally requires unique dates, correct attributes,
declared bounds and the full expected count, establishing gapless coverage.

Exact logical nullability is enforced without driver-side row conversion.
Required nulls raise; nullable fields retain their approved unknown-member
exceptions. Runtime UTC, ANSI and case-sensitive settings must match the typed
build specification. Spark analytical hashes match the Phase 0 Python reference;
metadata is excluded, including the audit timestamp and source batch.

`writer.py` supplies local proof functions only. Native local roots resolve to
`gold/<model>/build_id=<id>/`; URI/UNC/glob roots and unimplemented models are rejected.
The same writer also supports the two Phase 2 models.
`write_local_dimension` validates before an `errorifexists` write, then invokes
`verify_local_dimension`. Existing or partial destinations are never appended,
overwritten or deleted. Verification is a separate read-only function: missing
output fails before any DataFrame or storage mutation. No manifest is written or
accepted, and there is no production publication.

Spark Parquet readback widens nullable flags. Verification first checks the
physical names/order/types and actual required values, then restores the central
logical nullability contract. It revalidates hashes, metadata, unknown members,
source reconciliation and all-row multiset equality, including metadata. This
readback code ran in the required Linux runtime. The three physical integration
tests passed, including schema/nullability, count/hash/metadata preservation,
unknown-member preservation and immutable duplicate-output checks.

The small fixtures contain 64 real calendar dates plus unknown (2023-12-29 through
2024-03-01), two employees (one active, one terminated), two departments sharing
one BU, two locations and two job roles. Each source-backed dimension has two
real rows plus unknown. Additional calendar tests cover the 2020/2021 ISO boundary
and a single leap day. See the implementation plan for the current test evidence.

## Phase 2 complete - assignment and movement

Phase 2 now provides callable builders in `spark/gold/phase2.py` for
`dim_employee_assignment` and `fact_employee_movement`. Assignment intervals are
half-open and bounded by hire, event boundaries, termination plus one day, or
the explicit source cutoff plus one day. Current employee organisation values
are labelled `current-state-assumed`; promotion and transfer values are labelled
`event-derived`. Same-day events are coalesced at one daily boundary, while
conflicting values for the same attribute fail validation. Business-unit history
uses the current department mapping. Movement rows represent HIRE, EXIT,
PROMOTION and TRANSFER with deltas +1, -1, 0 and 0 respectively; no restricted
employee attributes are selected. These functions are local, caller-supplied
DataFrame transformations only and do not publish Gold storage.

The [Phase 2 validation matrix](../plans/gold-phase2-validation.md) maps each
required behaviour to an executable test: no-event and sequential histories,
attribute carry-forward, manager and unknown-reference handling, interval
failures, assignment/event/movement uniqueness, compatible and conflicting
same-day evidence, deterministic keys/hashes, exact schemas and governance.

Transfer events preserve role; promotions preserve organisation and manager.
A null transfer manager means no manager. Legitimately unknown attributes use
key 0 and an unknown history basis; missing required positive dimension parents
fail local publication. Optional unresolved managers use 0 without synthesizing
employees. The source cutoff still bounds employees with future termination.

Both models use the existing immutable local writer. Its sources mapping
contains employees, promotions, transfers, employee_exits, departments,
locations and job_roles from the explicit Silver batch. Publication validates
required references and reconstructs the expected rows before writing.
Duplicate paths fail; append and overwrite are unavailable; accepted files
remain unchanged. This is local proof, not a production release.
Phase 3 has not started.
