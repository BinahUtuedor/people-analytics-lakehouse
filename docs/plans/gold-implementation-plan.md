# Gold Implementation Plan

## Status and approvals

Gold design decisions are approved. Phase 0 is complete and approved. Phase 1 core-dimension code,
in-memory validation and Linux Parquet validation are complete. Phase 1 is
COMPLETE; Phase 2 complete. Phase 3 complete: `fact_workforce_monthly` local validation and physical proof pass. Phase 4 payroll is complete and validated. Phase 5 attendance is implemented and validated locally. Phase 6 MVP-wide validation is complete. No production Gold output
or Gold CLI exists. This plan is not permission to begin another phase. The authoritative design is
[Gold layer](../architecture/gold-layer.md), with
[decisions](../architecture/gold-decisions.md). All ten approved MVP schemas,
keys, snapshot overlap rules and bounded assignment intervals are available in
that contract. Phase 0 encodes and validates this approved baseline; it does not
reopen the design or add fields from the ORM. Implementation approval is separate.

Each phase requires explicit approval to begin, a review of its concrete
implementation and validation evidence at completion, then separate approval for
the next phase. A failed check or unresolved contract blocks advancement.
Do not create post-MVP models incidentally while implementing MVP dependencies.

All implementation phases first use small synthetic fixtures and isolated local
Parquet destinations. No phase authorizes S3 access, upstream data changes, AWS
infrastructure, live EMR execution, serving-engine selection, staging, committing
or pushing. Those actions need separately scoped approval. Do not run the
operational pipeline simply to obtain test data.

The operational format is fixed: `command -> prerequisites -> what happens
internally -> input -> output -> side effects -> success condition`. Replace
the placeholders only when real tested entry points exist. Create
`docs/operations/gold-runbook.md` at that point, not during this design task.

## Phase 0 — Contract foundation

**Status: COMPLETE and approved.**

No Gold processing command is introduced. The existing test runner exercises
the Phase 0 contracts as follows.

| Item | Contract test execution |
| --- | --- |
| Command | `python -m unittest discover -s tests/spark/gold -p "test_*.py" -v` |
| Prerequisites | Repository root; existing Python 3.12/PySpark environment; approved Gold Markdown contract; no AWS credentials or Spark session required |
| What happens internally | Imports static schemas and pure helpers; compares every field with approved documentation; exercises keys, hashes, release specifications, inventory/policy and validation contracts |
| Input | Small deterministic Gold-shaped contract fixtures and approved field tables; no source data |
| Output | unittest pass/fail report |
| Side effects | Python bytecode caches only; no data, manifest or cloud writes |
| Success condition | All focused contract tests pass without skips |

Historical validation at Phase 0 completion: 42 focused tests passed, zero failures
and zero skips, using Python 3.12.10/PySpark 4.2.0. An additional 18 existing
Bronze/Silver mock-based unit tests passed. `python -m compileall -q config spark
tests` passed. New Python files were formatted with the existing Black 26.5.1
executable targeting Python 3.11; production modules also parse with Python 3.11
syntax rules. `git diff --check` passed.

The main `spark-tests` and `emr-compat-tests` Docker suites were **not executed**:
the Docker Linux engine pipe was unavailable, confirmed outside the sandbox.
No packages were installed. Both Compose services already discover
`tests/spark/`, including Gold; no test infrastructure change was necessary.
Actual Python 3.11/Java 17/Spark 3.5.6 runtime compatibility and the complete
Linux regressions remain unverified in this environment. Physical storage
verification, distributed validation and every business transformation remain
later-phase work.

The implementation retains the following approved scope:

| Item | Contract |
| --- | --- |
| Prerequisites | Documented complete approved schema baseline; explicit Phase 0 implementation approval; inspect existing Spark utilities and callers before adding interfaces |
| What happens internally | Encode the approved model registry, exact schemas/nullability/grains, common metadata, deterministic key/hash utilities, unknown-member policy, validation interfaces and release manifest/lifecycle; pin canonical serialization and verification-only/exclusive duplicate protection |
| Input | Approved Gold contract and decisions, verified Silver schema definitions, small contract fixtures |
| Output | Reviewable contract modules and focused tests; no business transformations or production outputs |
| Side effects | Approved repository code/test/documentation changes and disposable local test artifacts only |
| Success condition | Every MVP field has source/derivation and nullability; exact key/hash encoding is stable; manifest identifies reproducible inputs/configuration; registry and validation interfaces have focused passing checks; no downstream semantic gaps |

**Approval gate:** review schema registry, key/unknown rules, quality interfaces,
release acceptance and failure behaviour. Explicitly approve Phase 1 only after
the contract foundation passes; no cloud or serving approval is implied.

## Phase 1 - Core dimensions

**Status: COMPLETE � local Linux physical Parquet proof and regressions passed.**

The five callable core-dimension builders, validation gates, and local-only
writer/readback contracts are implemented. No Gold reader, job, CLI or manifest
publication exists. Phase 2 completion evidence appears below.

Phase 1 validation evidence: `python -m unittest discover -s tests/spark/gold
-p "test_*.py" -v` ran 63 tests: 63 passed, 0 failed, 0 errors, and 3
physical Parquet tests skipped by the native-Windows safeguard. The same three
physical tests ran in the Linux Docker runtime and passed. The complete Linux
Docker Spark suite ran 104 tests and passed; the EMR compatibility suite ran
104 tests and passed on Python 3.11, Java 17 and PySpark 3.5.6. The safe
non-cloud Bronze/Silver regression set ran 22 tests and passed. Compileall,
Black and `git diff --check` passed. Phase 2 assignment and movement implementation is complete; Phase 3 complete: `fact_workforce_monthly` local validation and physical proof pass. Phase 4 payroll is complete and validated.

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 0 accepted; explicit Phase 1 approval; approved field inventory excludes demographics |
| What happens internally | Build dim_date, dim_employee, dim_department, dim_location and dim_job_role; add reserved unknowns and metadata; join current department/business-unit mapping without historical claims |
| Input | Local Silver-shaped employee/department/business-unit/location/job-role fixtures and explicit calendar coverage |
| Output | Validated core dimension DataFrames and isolated local Parquet/readback evidence |
| Side effects | Phase-specific modules/tests/docs and new local test output; upstream data unchanged |
| Success condition | Keys/grains unique; real member coverage reconciles plus unknowns; FKs valid; no join fan-out; date/ISO/leap boundaries pass; no excluded demographics or unapproved sensitive fields |

**Approval gate:** review dimension schemas, unknown members, business-unit
assumption and calendar coverage; explicitly approve Phase 2.

## Phase 2 — Assignment history

**Status: Phase 2 complete. Phase 3 complete: `fact_workforce_monthly` local validation and physical proof pass. Phase 4 payroll is complete and validated.**

The [coverage matrix and validation report](gold-phase2-validation.md) records
32 focused tests (31 native passes, one physical safeguard skip), the successful
Linux equivalent, 136/136 complete Spark tests, 136/136 local EMR compatibility
tests, and 41/41 separately rerun Bronze/Silver tests. Production corrections
exposed by the new tests are documented there. No Phase 0 contract changed.

Coverage includes sequential histories, supported carry-forward semantics,
duplicate rejection, same-day ambiguity, deterministic keys/hashes, exact
schema/nullability/governance, and immutable shared-writer integration.

**Command:** `spark.gold.attendance.build_fact_attendance`; local callable, no Gold CLI or production publication.

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 1 accepted; explicit Phase 2 approval; use the approved next-change/termination-plus-one/source-cutoff-plus-one interval bounds |
| What happens internally | Reconstruct dim_employee_assignment from lifecycle evidence; form half-open intervals; expose attribute-level history basis and evidence; reconcile current state; detect ambiguous same-day changes |
| Input | Local employee, transfer, promotion and relevant recruitment/exit fixtures plus core dimensions |
| Output | Assignment intervals, stable keys, history-basis fields and local physical validation evidence |
| Side effects | Assignment code/tests/docs and isolated local outputs |
| Success condition | No overlapping/zero-length intervals; reference dates resolve uniquely; source old/new chains agree; assumptions labelled; contradictory history fails; exit-day activity remains attributable without changing closing headcount |

**Approval gate:** inspect reconstructed examples, unsupported-history assumptions,
same-day conflict policy and interval tests; explicitly approve Phase 3.

## Historical Phase 3 movement work package

This original design work package was completed within Phase 2. The table below
is retained as a historical design reference, not outstanding Phase 3 work.
The current Phase 3 scope is workforce monthly, authorized separately from this completed movement work package.

**Command: TO BE IMPLEMENTED**

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 2 accepted; explicit Phase 3 approval; approved event-specific schema and attribute-transition ownership |
| What happens internally | Build fact_employee_movement for HIRE, EXIT, PROMOTION and TRANSFER; resolve before/after assignments; set event_count/headcount_delta; reconcile source events and simultaneous attribute movement |
| Input | Local employees, employee_exits, promotions, transfers and approved assignment/core dimensions |
| Output | Consolidated movement fact and per-event/source reconciliation results |
| Side effects | Movement modules/tests/docs and isolated local Parquet |
| Success condition | Each source event represented once; hire/exit not duplicated from supporting evidence; deltas +1/-1/0 correct; exit before uses final exit-date assignment; simultaneous promotion/transfer shares combined boundary but each attribute transition counts once; all FKs pass; promotion salary and unapproved event attributes excluded |

**Approval gate:** review event examples, source coverage and population/attribute
reconciliations; explicitly approve Phase 4. Final rate labels remain prohibited.

## Phase 3 — Workforce snapshot

**Status: Phase 3 complete. Phase 4 payroll is complete and validated.**

**Local callable implementation:** `spark.gold.workforce_monthly.build_fact_workforce_monthly`. No CLI or production publication. See [Phase 3 evidence](gold-phase3-validation.md).

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 2 accepted; explicit Phase 3 approval; approved employment-overlap population, day-based tenure and earlier-of-month-end/termination assignment contract |
| What happens internally | Generate fact_workforce_monthly for closed calendar periods with employment overlap; resolve organisation at earlier of month-end and termination; calculate headcount_eom and tenure_days_eom; validate against lifecycle evidence |
| Input | Local employee/lifecycle fixtures, assignment/core dimensions, explicit reporting range and closure/source coverage cutoff |
| Output | Monthly employee snapshots with headcount/tenure and independent closing-population reconciliation |
| Side effects | Snapshot code/tests/docs and isolated local year-partitioned output |
| Success condition | Composite employee/month PK unique; exact overlap population; no partial month; exit on snapshot date excluded from closing; current activity flags do not erase history; tenure is month-end minus hire only for HC=1 and null otherwise; opening + hires - exits equals closing; row count is never substituted for sum of headcount flags |

**Approval gate:** review boundary fixtures, temporal attribution, monthly
population and tenure behaviour; explicitly approve Phase 4.

## Phase 4 — Payroll

**Command:** local callable `spark.gold.payroll.build_fact_payroll`; no Gold CLI or production publication.

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 3 accepted; explicit Phase 4 approval; approved DECIMAL(18,2) monetary schema and access restrictions; source component rounding tolerance documented with fixture evidence |
| What happens internally | Build fact_payroll; preserve source components; join assignment at actual pay_period_end; label analytical period-end attribution; validate components and reconcile each by period/currency |
| Input | Local payroll fixtures including hire/exit-clipped periods, core dimensions and assignment history |
| Output | Restricted payroll fact and record/component/period/currency reconciliation evidence |
| Side effects | Payroll code/tests/docs and isolated local Parquet; no source allocation changes |
| Success condition | One fact row per source payroll record; no join fan-out; precise types; actual period-end lookup including exit date passes; source component identities checked with justified tolerance; no currency mixing or fabricated total-cost metric |

**Validation result:** complete. The focused payroll suite passed 35/35,
physical payroll checks passed 4/4, complete Gold passed 165/165, main Spark
passed 206/206, local EMR compatibility passed 206/206, and the safe
Bronze/Silver regression passed 41/41. Day-weighted allocation is excluded.

## Phase 5 — Attendance (complete)

**Command:** `spark.gold.attendance.build_fact_attendance`; local callable, no Gold CLI or production publication.

| Item | Contract |
| --- | --- |
| Prerequisites | Phase 4 accepted; governed status and absence/hour definitions verified against source generation |
| What happens internally | Build fact_attendance using distributed joins; resolve work-date assignment; retain separate status, recorded/absent day counts and hours/overtime measures; enforce sensitivity restrictions |
| Input | Local attendance fixtures, governed status definitions, core/assignment dimensions; bounded larger synthetic workload for scale checks |
| Output | Attendance fact, uniqueness and separate hours/overtime reconciliation results |
| Side effects | Attendance code/tests/docs and isolated local year-partitioned Parquet |
| Success condition | One row per source attendance record and employee/work date; absence derived only from governed status; missing records not turned into absence; inclusive employment windows pass; hours reconcile separately; no driver-side record loops or unbounded collect. Met: focused 8/8, physical 2/2, complete Gold 173/173. |

**Validation record:** [Gold Phase 5 validation](gold-phase5-validation.md).

## Phase 6 — Integrated MVP validation

**Command: TO BE IMPLEMENTED**

The existing local regression entry point is
`docker compose run --rm --build spark-tests`; it is not a Gold job command.
The existing `emr-compat-tests` service provides a local compatibility runtime,
not authorization to submit jobs to EMR. Review cached dependencies and the
Docker runtime before execution; do not require cloud access for these gates.

| Item | Contract |
| --- | --- |
| Prerequisites | Phases 0–5 accepted; explicit Phase 6 approval; Linux Docker runtime on Windows; complete local Silver fixture batch; portable entry point now implemented and documented |
| What happens internally | Run full Spark regression and physical Parquet integration; rebuild in separate local roots and compare logical hashes; run model reconciliations/cross-model FK checks; exercise duplicate failure, partial failure and verification-only; review BI star schema and EMR-target API/runtime compatibility |
| Input | Complete local fixture batch, pinned release specification, every MVP model, supported local and EMR-compatible runtimes |
| Output | Reviewable integrated validation report, reproducible local release and readback evidence, tested operational documentation |
| Side effects | Local test execution/artifacts and affected docs; no AWS resources, S3 output or upstream mutation |
| Success condition | Relevant existing and new suites pass without unexplained skips; content deterministic across rebuilds; every grain/FK/reconciliation passes; no writes in verification-only; duplicate/partial output cannot be accepted; BI uses dimension-to-fact single-direction relationships without interval/fact-to-fact joins; EMR-target compatibility established locally |

**Approval gate:** accept the local MVP only after reviewing the complete release
evidence. Serving-engine/dbt/Power BI technology selection, live EMR execution,
cloud publication and post-MVP models each require separate scope and approval.
Local validation alone does not grant any of those permissions.


Phase 2 implementation status: COMPLETE for the two approved models (dim_employee_assignment and fact_employee_movement). Builders are local callable functions with deterministic keys, bounded half-open intervals, same-day conflict detection, and approved movement deltas. Phase 3 complete: `fact_workforce_monthly` local validation and physical proof pass. Phase 4 payroll is complete and validated.


## Approved Phase 4 calendar reference refinement

Calendar reference coverage is reporting_start through the month-end containing
source_cutoff. Business coverage is unchanged. Payroll reporting month-end may
follow its actual period end and cutoff; assignment uses actual period end.
A reference date never makes a workforce snapshot or business event eligible.
