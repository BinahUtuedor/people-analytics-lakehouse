# Gold Phase 4 - fact_payroll validation

Status: complete. Implementation module: `spark/gold/payroll.py`.
Baseline: existing uncommitted Phase 4 working tree on `main`,
`fde508fee8decfc3647740da222909c56fd8d3f2`.
Phase 5 attendance is now implemented and validated separately. No production
release or cloud publication.

## Calendar conflict resolution

Conflict: Payroll reporting month-end can fall after a mid-month source cutoff,
while the previous dim_date rule ended exactly at source_cutoff.

Resolution: dim_date coverage now extends through the calendar month-end containing
source_cutoff, starting at reporting_start. Schema, unknowns and YYYYMMDD remain unchanged.

Business-data impact: NONE. Source, assignment, event and model-specific business
eligibility remain independently cutoff-bounded.

Payroll impact: Legitimate incomplete-month payroll records can reference their
required calendar month-end while retaining actual-period-end assignment attribution.
The code revision in the existing BuildSpec identifies the changed implementation;
no new build-identity mechanism or workforce assumption identifier is introduced.

## Contract review and exact mapping (before payroll implementation)

The Phase 0 contract remains unchanged: primary key `payroll_id`; additional
unique grain `(employee_key, pay_period_start_key, pay_period_end_key)`. Distinct
IDs for the identical employee/actual period are rejected under this committed
contract. There are no supported correction/supplemental categories. No employee-
month collapse or deduplication occurs. Every legitimate source record is retained.

All 26 fields below are logically **non-null** and appear in this exact order.
All date references require real dim_date members (never 0). Employee keys must
resolve to positive dim_employee members. Required assignment cannot be 0;
organisation/manager 0 is retained only from authoritative Phase 2 unknown state,
and must resolve to the corresponding reserved parent member. Broken positive
references fail. Every output also undergoes exact schema, canonical hash and
full source reconstruction validation.

| Target | Source | Transformation / category | Spark type | Validation / unknown behaviour |
| --- | --- | --- | --- | --- |
| payroll_id | payroll.payroll_id | Direct business identity | BIGINT | Positive; unique; exact source coverage |
| employee_key | payroll.employee_id | Retained source key | BIGINT | Positive required employee FK |
| pay_period_start_key | payroll.pay_period_start | Actual date to YYYYMMDD | INT | Valid date FK; within employment and cutoff |
| pay_period_end_key | payroll.pay_period_end | Actual date to YYYYMMDD | INT | Valid date FK; start <= end; within employment and cutoff |
| payroll_month_key | payroll.pay_period_end | Last day of actual-end month to YYYYMMDD | INT | Calendar FK; no exclusion when later than cutoff |
| reporting_year | payroll.pay_period_end | Year of reporting month-end | INT | Matches month key; physical year partition |
| pay_period_days | payroll start/end | Inclusive datediff + 1 | INT | Positive; exact source reconstruction |
| assignment_key | dim_employee_assignment | Half-open lookup at actual period end | STRING | Exactly one real interval; no unknown fallback |
| department_key | resolved assignment | Historical direct FK | BIGINT | Parent exists; approved 0 allowed |
| job_role_key | resolved assignment | Historical direct FK | BIGINT | Parent exists; approved 0 allowed |
| location_key | resolved assignment | Historical direct FK | BIGINT | Parent exists; approved 0 allowed |
| manager_employee_key | resolved assignment | Historical manager FK | BIGINT | Parent exists; approved 0 allowed |
| currency | payroll.currency | Direct source text | STRING | Required standardized nonblank text, source max length 10; no ISO domain invented |
| payroll_status | payroll.payroll_status | Direct source text | STRING | Required standardized nonblank text, source max length 30; open domain |
| base_salary | payroll.base_salary | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; no lossy rounding/overflow |
| overtime_pay | payroll.overtime_pay | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; no lossy rounding/overflow |
| bonus | payroll.bonus | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; no lossy rounding/overflow |
| deductions | payroll.deductions | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; includes pension and tax |
| pension_contribution | payroll.pension_contribution | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; deduction component, not employer cost |
| tax_amount | payroll.tax_amount | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; deduction component |
| gross_pay | payroll.gross_pay | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; gross identity |
| net_pay | payroll.net_pay | Exact monetary normalization | DECIMAL(18,2) | Nonnegative; net identity |
| _source_batch_id | build.spec.silver_batch_id | Existing fixed build metadata | STRING | Equal source batch |
| _gold_build_id | build.spec.build_id | Existing deterministic BuildSpec digest | STRING | Exact supplied build identity |
| _gold_generated_at | build.generated_at | Fixed accepted timestamp in UTC | TIMESTAMP | Exact supplied timestamp; no wall clock |
| _record_hash | All 22 business fields above | Existing canonical Gold SHA-256 | STRING | Recompute; metadata excluded |

## Source and monetary semantics

Authoritative sources: database/models/payroll.py, simulator/payroll.py,
etl/extract.py, spark/silver/transform.py and the Gold architecture/contracts.
`dimensions.py` does not exist; existing core builders are in `transform.py`.

PostgreSQL money fields are NUMERIC(12,2). Extraction uses pandas read_sql_table
with its existing default numeric coercion; Bronze retains physical types.
Silver explicitly casts base_salary, overtime_pay, tax_amount, gross_pay and
net_pay to DECIMAL(18,2). Bonus, deductions and pension_contribution retain the
upstream numeric type, commonly DOUBLE through the current extractor. Gold must
normalize these explicitly, checking cent precision and finite/range validity
before monetary arithmetic; it must not require a fictitious uniform Silver schema.
The source payroll Date fields may arrive as midnight timestamps; Silver's
suffix-based date conversion does not cover pay_period_start/pay_period_end.
Gold accepts dates or exact midnight timestamps, rejecting time-bearing values.
No source-reader, upstream transformation or schema is modified.

All monetary columns are required, nonnegative under the current generated
source model (no reversal/correction semantics). Zero components are legitimate;
operational validation also allows zero base/gross/net, although the generator
skips nonpositive base records. No extra positive-only domain is imposed.
Currency defaults to GBP and status to Processed in the generator, but these
are defaults, not committed enumerations. Preserve other valid source strings;
never convert currency or combine currencies in totals.

Gross definition: base + overtime + bonus. Pension is 5% of base and tax 20%
in the simulator; bonus is 3%. These rates explain source generation and are
not new Gold rate validations or reconstructed payroll amounts.
Deductions definition: pension + tax. Net definition: gross - deductions.
All persisted components use independent Decimal ROUND_HALF_UP to two decimals.

Evidence-backed arithmetic gates:

- gross = base + overtime + bonus exactly: base/overtime already have cent scale,
  so rounding gross commutes with adding those cent amounts to rounded bonus.
- abs(deductions - pension - tax) <= 0.01.
- abs(net - gross + deductions) <= 0.01.

Independent rounding of two operands and their result bounds the latter two
cent-scale residuals by one cent. A Decimal-only enumeration of 10,000 source
base-cent amounts confirmed maxima 0.00, 0.01, 0.01 (first nonzero example base
0.02). These tolerances apply only to within-record component identities.
Source-to-Gold normalized component values and per-period/per-currency totals
must match **exactly**; no reconciliation tolerance. Pension and tax are never
subtracted again alongside total deductions. No employer total-cost metric.

## Periods, attribution and publication

Current source intervals occupy one calendar month, clipped to hire, termination
and generation cutoff. Gold validates rather than reclips these dates. Reject
reversed/cross-month periods, periods before hire, after termination or after
source_cutoff. Final pay on termination is included; post-employment actual pay
periods are invalid in the committed generator/operational quality rules.

Use supplied Phase 2 history with valid_from_date <= actual_period_end <
valid_to_exclusive. Never reconstruct history or use current employee org for
payroll attribution. Calendar month-end is only the reporting date, not the
assignment date. Reporting year is the year of that reporting month-end.

Shared immutable local writer integration must preserve the payroll layout
`gold/fact_payroll/reporting_year=<year>/build_id=<id>/`. No append, overwrite,
accepted-file deletion, cloud access or production release acceptance.

Governance: all identity PII and source notes/free text are excluded by exact
contract projection. Payroll measures and employee linkage remain approved
restricted financial detail. Hashes are integrity identifiers, not anonymisation.
Historical organisation inherits Phase 2 event-derived/current-state-assumed
limitations and current department-to-business-unit mapping; no new history claim.

## Validation evidence

The completed pre-payroll calendar/reference regression is retained: 82 total,
82 passed, 0 failed, 0 errors, 0 skipped, exit 0. It is not rerun separately;
no production calendar, workforce-history or workforce-monthly changes were
made during this validation continuation.

The first focused Linux run completed 35 tests: 34 passed, 1 failed, 0 errors,
0 skipped, exit 1. The missing-employee test changed every source employee ID
to 999, inadvertently creating duplicate employee/period keys. The uniqueness
gate correctly rejected that input before the expected employment gate. The
test now uses one source record to isolate the missing employee; its strict
employment-window assertion is retained. Production code was unchanged.
The final focused run completed in 698.829 seconds: **35 total, 35 passed,
0 failed, 0 errors, 0 skipped, exit 0**. Command:
`docker compose run --rm --pull never spark-tests python -m unittest tests.spark.gold.test_payroll -v`.
Local ignored log: `logs/phase4-payroll-verified.log`.

Physical evidence is included in those 35 tests: **4 total, 4 passed, 0 failed,
0 errors, 0 skipped**, in the same exit-0 invocation. These prove exact schema
and logical nullability, source grain and references, DECIMAL arithmetic,
currency/status, metadata/hash and source monetary reconciliation; immutable
year-partition roundtrip; duplicate/append/overwrite rejection with unchanged
accepted bytes; invalid pre-write and corrupt-readback rejection; wrong/missing
partition rejection; and independent schema checking of the later year partition.

Complete Gold regression: **165 total, 165 passed, 0 failed, 0 errors, 0
skipped, exit 0**. Main Linux Spark regression: **206 total, 206 passed, 0
failed, 0 errors, 0 skipped, exit 0**. Local EMR compatibility: **206 total,
206 passed, 0 failed, 0 errors, 0 skipped, exit 0**, using Python 3.11.13,
Java 17.0.16 and PySpark 3.5.6. Safe Bronze/Silver regression: **41 total,
41 passed, 0 failed, 0 errors, 0 skipped, exit 0**. No AWS or S3 access was
used. Bronze behavior changed: NO. Silver behavior changed: NO.

Final quality gates passed: compileall exit 0, Black check exit 0 and
`git diff --check` exit 0. Phases 0, 1, 2, 3 and 4 are complete; Phase 5 has
not started.

## Controlled fixture and review matrix

Controlled reconciliation observed seven source rows/keys and seven Gold
rows/keys, with zero missing, unexpected or duplicate keys. Every one of the
eight monetary component differences is exactly 0.00 in every group below.
Rows are compared as multisets; physical ordering is never assumed.

| Actual period | Currency | Source/Gold rows | Gross (each side) | Net (each side) |
| --- | --- | --- | --- | --- |
| 2023-12-01 to 2023-12-31 | GBP | 1 | 113.03 | 88.03 |
| 2024-01-01 to 2024-01-31 | GBP | 1 | 1030.02 | 780.02 |
| 2024-01-01 to 2024-01-31 | EUR | 1 | 2060.01 | 1560.01 |
| 2024-02-01 to 2024-02-29 | GBP | 1 | 1145.44 | 870.42 |
| 2024-03-01 to 2024-03-31 | GBP | 1 | 1236.05 | 936.04 |
| 2024-04-01 to 2024-04-15 | GBP | 2 | 1340.05 | 1015.05 |

For the canonical incomplete month, source cutoff and actual period end are
2024-04-15, payroll_month_key is 20240430, and assignment reference is
2024-04-15. Leap-year coverage includes February 29; December/January rows
retain their respective 2023/2024 reporting-year partitions. Calendar reference
coverage never extends workforce eligibility or assignment/event history.

The compact fixture retains December/January year-boundary records, a
February promotion, a March transfer/manager change, and April incomplete-month
and termination-date final pay. Two currencies exercise separate reconciliation;
this tests the source's open string currency representation, not a claim that
the current generator emits multiple currencies. No correction category or
second payroll transaction for an identical employee/period is fabricated.

Coverage verified by the focused and broad regressions: exact ordered schema/nullability;
PK and employee-period uniqueness; timestamp/date inputs; reversed/cross-month
and post-cutoff periods; employee/date/org/manager/assignment references; required
assignment and overlap rejection; final-pay and post-employment boundaries;
cent precision, overflow/nonfinite/lossy conversion, zero/negative domains;
rounding residual boundaries; open currency/status preservation; row/key and
eight-component per-period/currency reconciliation; full per-record comparison;
reorder/repartition stability; metadata/hash/governance; immutable year-first
physical partitions, pre-write rejection and corrupt-readback detection.

The implementation is local-only and callable; there is no Gold CLI, production
publication, AWS access, S3 access or release claim. Phase 0 complete; Phase 1
complete; Phase 2 complete; Phase 3 complete; Phase 4 complete. Phase 5 has not
started.
