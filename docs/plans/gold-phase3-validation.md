# Gold Phase 3 workforce monthly validation

Status: **Phase 0 complete; Phase 1 complete; Phase 2 complete; Phase 3 complete.**
Phase 4 (payroll) has not started. Attendance has not started.

Baseline: clean `main`, Phase 2 commit
`f0f81b1898f3c50ab441b19a1c246335d0dbfff1`.
Phase 3 retains the approved `closed-month-overlap-v1` and
`post-event-closing-headcount-v1` policies. The earlier closing-only row
interpretation is superseded; the Phase 0 contract is unchanged.

Validation resumed on `main` from that same commit with the existing Phase 3
working tree retained and no staged files. No production or test code changed
during the resumed validation. No implementation was restarted or redesigned.

## Population and attribution

`spark.gold.workforce_monthly.build_fact_workforce_monthly(sources, build)` consumes
explicit Silver `employees` and `employee_exits`, the five Phase 1 dimensions,
and Phase 2 `dim_employee_assignment`. The mapping uses dataset/model names
as keys. Parent dimensions must already be validated; Phase 3 also checks
their exact schemas, unique primary keys and common build metadata. It fully
validates calendar coverage and checks every fact FK against these parents.
It never reconstructs organisation history or discovers source storage.

For month start S and month-end D:

- Generate month ends from `dim_date` within the inclusive reporting range
  and on or before `source_cutoff`. A mid-month upper bound excludes that
  incomplete month. No wall-clock value participates.
- Include a row when `hire_date <= D` and termination is absent or
  `termination_date >= S`. Employment overlap is inclusive: even an exit
  on S, or a same-day hire/exit, retains that month's participation row.
- Set `headcount_eom = 1` when termination is absent or later than D;
  otherwise set it to 0. Hire-on-D is included; exit-on-D is post-exit and
  contributes zero closing headcount.
- Resolve the assignment at `min(D, termination_date)`, using D when
  termination is absent. Use `valid_from_date <= reference_date` and
  `reference_date < valid_to_exclusive` against supplied Phase 2 history.
  Phase 2 retains exit-date assignment coverage through termination plus
  one day, so an exited employee's final assignment remains attributable.
- Fail closed for missing or multiple assignments. Preserve legitimate
  zero organisation/manager keys; never replace a broken positive FK with 0.
- Set tenure to `datediff(D, hire_date)` without an inclusive +1 for HC=1;
  set it to null for HC=0. Calendar arithmetic includes leap days.

Past/current termination dates require matching exit evidence. Supplied
exit dates must agree with employee state, and exit identities must be unique.
A future termination does not require an event already observed by the cutoff;
if supplied, that event must agree. Future termination never extends coverage.

## Exact source-to-target mapping

The table is in Phase 0 field order. All fields are logically non-null except
`tenure_days_eom`; no separate fact surrogate or business-unit key is added.

| Target | Spark type | Source/transformation | Null or unknown rule |
| --- | --- | --- | --- |
| employee_key | BIGINT | employees.employee_id | Positive real employee FK |
| snapshot_month_key | INT | dim_date.date_key at closed month-end | Required YYYYMMDD |
| reporting_year | INT | Year of snapshot date | Required |
| assignment_date_key | INT | dim_date.date_key at min(month-end, termination) | Month-end if termination absent; unresolved date fails |
| assignment_key | STRING | Effective Phase 2 assignment | Missing/ambiguous history fails, no unknown fact assignment |
| department_key | BIGINT | Resolved assignment.department_key | Approved 0 member allowed; positive orphan fails |
| job_role_key | BIGINT | Resolved assignment.job_role_key | Approved 0 member allowed; positive orphan fails |
| location_key | BIGINT | Resolved assignment.location_key | Approved 0 member allowed; positive orphan fails |
| manager_employee_key | BIGINT | Resolved assignment.manager_employee_key | Preserve Phase 2 real manager or 0 |
| headcount_eom | INT | Post-event employment at month-end | Exactly 0 or 1 |
| tenure_days_eom | INT | Month-end minus employees.hire_date | Null exactly when HC=0 |
| _source_batch_id | STRING | build.spec.silver_batch_id | Required; consumed source batches checked |
| _gold_build_id | STRING | Existing BuildSpec.build_id | Required deterministic build identity |
| _gold_generated_at | TIMESTAMP | Fixed build.generated_at in UTC | Required, never per-row current_timestamp |
| _record_hash | STRING | Existing canonical Gold business-field serialization | Required; excludes technical metadata |

## Controlled fixture reconciliation

Reporting range: 2024-01-01 through 2024-04-15; source cutoff 2024-04-15.
Latest generated snapshot: 2024-03-31. There is no April snapshot.

| Employee | Hire | Termination | Boundary/history purpose |
| --- | --- | --- | --- |
| 1 | Jan 1 | None | Continuing; promotion Feb 10, transfer Mar 10, promotion Mar 20 |
| 2 | Jan 1 | None | Continuing; manager after employee 1's transfer |
| 3 | Jan 15 | None | Mid-month hire |
| 4 | Jan 31 | None | Hire on month-end; tenure zero initially |
| 5 | Feb 1 | None | No January participation |
| 6 | Jan 1 | Jan 15 | Exit before first month-end |
| 7 | Jan 1 | Feb 29 | Exit on leap-year month-end |
| 8 | Jan 1 | Mar 1 | Exit on first day of month |
| 9 | Feb 29 | Feb 29 | Same-day hire/exit |
| 10 | Apr 1 | None | Incomplete hire month excluded |

| Snapshot | Expected overlap | Fact rows | Distinct employees | Expected closing | HC sum | Non-closing rows | Row-grain difference | HC difference |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2024-01-31 | 7 | 7 | 7 | 6 | 6 | 1 | 0 | 0 |
| 2024-02-29 | 8 | 8 | 8 | 6 | 6 | 2 | 0 | 0 |
| 2024-03-31 | 6 | 6 | 6 | 5 | 5 | 1 | 0 | 0 |

January's non-closing participant is employee 6; February's are 7 and 9;
March's is 8. There are 21 employee-month rows. This total is not an employee
count or an additive headcount metric. `SUM(headcount_eom)` reconciles to the
closing population separately for every month. Row count equals distinct
employees per month and may exceed closing headcount.

`monthly_reconciliation` returns distributed monthly metrics, including zero
populations. Expected counts are calculated from independent source predicates.
Full row reconstruction additionally rejects swapped per-employee headcount
flags even when monthly aggregates still match.

## Executable coverage

Tests are in `tests.spark.gold.test_phase3.WorkforceTests`.

| Coverage | Tests |
| --- | --- |
| Closed months, leap February, mid-month/exact-EOM cutoff, reporting-end bounds | test_calendar_closed_leap_months_and_midmonth_cutoff; test_calendar_exact_eom_and_before_first_eom; test_reporting_end_bounds_coverage |
| Active, mid-month/EOM hires, hire after snapshot | test_hire_boundaries_and_active_population |
| Pre-EOM/EOM exits, exit on month start, same-day hire/exit, no later participation | test_exit_before_eom_retained_but_not_next_month; test_exit_on_eom_and_same_day_hire_exit; test_exit_on_month_start_retains_month |
| Future termination cannot extend source horizon | test_future_termination_does_not_extend_cutoff |
| Promotion, transfer, sequential states, manager, current-state isolation | test_assignment_promotion_transfer_sequential_and_manager; test_history_wins_over_current_organisation |
| Multiple movements do not multiply employee-month grain | test_movement_count_does_not_multiply_months |
| New hire, one year, leap anniversary, zero-headcount null tenure | test_tenure_new_hire_and_leap_days; test_tenure_exact_year_and_leap_anniversary |
| Hand-counted monthly participation/closing populations; empty months | test_monthly_reconciliation_against_hand_counted_population; test_empty_population_reconciles_all_closed_months |
| Duplicate grain, missing/extra participants, balanced wrong flags | test_duplicate_grain_rejected; test_missing_and_extra_population_rejected; test_balanced_wrong_headcount_flags_rejected_per_employee |
| Required assignment, ambiguity, parent FKs, approved unknowns | test_missing_or_overlapping_assignment_fails_closed; test_missing_employee_and_organisation_parents; test_unknown_attributes_preserved |
| Missing calendar, wrong dates/assignment/tenure, wrong existing organisation FK | test_missing_calendar_fails; test_date_and_manager_foreign_keys_rejected; test_wrong_date_reference_assignment_and_tenure_rejected; test_valid_but_wrong_organisation_reference_rejected |
| Headcount domain and tenure rules | test_headcount_domain_and_null_tenure_rules_rejected |
| Schema, governance, Python canonical hash oracle, fixed metadata | test_exact_schema_governance_hash_oracle_and_metadata; test_wrong_hash_metadata_and_schema_rejected |
| Rebuild/reorder/repartition identity | test_determinism_rebuild_reorder_repartition |
| Exit reconciliation, duplicate IDs, wrong batch | test_missing_or_inconsistent_exit_evidence; test_duplicate_and_wrong_batch_source_rejected |
| Physical readback, logical nullability, all business/metadata values, partition, immutable destination | test_local_physical_roundtrip_and_immutable_output |
| Invalid output prevents write; corrupted readback fails | test_invalid_output_prevents_write_and_corruption_fails_readback |

## Physical proof and limitations

The existing shared immutable writer supports this fact at
`<local-root>/gold/fact_workforce_monthly/build_id=<id>/reporting_year=<year>/`.
The local build-first layout isolates the whole immutable output; this is not
the eventual production publication path. Tests use disposable `/workspace`
directories and remove them on completion. The writer validates before writing,
uses errorifexists, restores logical nullability after checking physical values,
and compares complete row multisets. Duplicate writes and append/overwrite
arguments are rejected. Physical row order is never assumed.

History has the same limitations as Phase 2: current-state-assumed baseline
attributes are not independently verified historical SCD evidence. Manager,
department, role and location history are only as complete as supplied Phase 2
evidence. Business-unit history remains the current department mapping. No
rehire/multiple-employment-spell model, daily snapshot, partial-month product,
new HR rounding rule or business-unit key is introduced. Names, email, DOB,
gender, salary and free text are excluded by the exact fact projection.

Build fingerprints and code revision remain explicit caller-supplied identity
inputs, as in Phases 0/1/2. This phase provides local callable functions and
physical proof, not a ten-model accepted release, CLI, production publication,
or cloud execution.

## Gate results

All required gates have successful terminal results, verified on 2026-09-17.
Totals include the physical cases; no skips are hidden in passed counts.

| Gate | Total | Passed | Failed | Errors | Skipped | Exit | Evidence |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Focused Phase 3 | 33 | 33 | 0 | 0 | 0 | 0 | Reused `phase3-focused.log`: `Ran 33 tests in 329.279s`, `OK` |
| Standalone Linux physical | 2 | 2 | 0 | 0 | 0 | 0 | Reused `phase3-linux-physical.log`: `Ran 2 tests in 203.803s`, `OK` |
| Complete Gold (Phases 0/1/2/3) | 128 | 128 | 0 | 0 | 0 | 0 | Reused `phase3-gold.log`: `Ran 128 tests in 1109.980s`, `OK` |
| Complete main Spark | 169 | 169 | 0 | 0 | 0 | 0 | `phase3-main-spark.log`: `Ran 169 tests in 1326.050s`, `OK`, `Exit status: 0` |
| Complete local EMR compatibility | 169 | 169 | 0 | 0 | 0 | 0 | `phase3-emr-compat.log`: `Ran 169 tests in 1185.563s`, `OK`, `Exit status: 0` |
| Safe non-cloud Bronze/Silver | 41 | 41 | 0 | 0 | 0 | 0 | `phase3-bronze-silver.log`: `Ran 41 tests in 95.891s`, `OK`, `Exit status: 0` |

The first three completed runs postdate the last production/test changes and
were not rerun as standalone gates. Their retained unittest terminal `OK`
results establish successful completion; the focused exit 0 was also supplied
as accepted session evidence. The three resumed regressions each ran once and
recorded the process exit status explicitly. Detailed logs are ignored local
artifacts under `logs/`, not staged evidence files.

The standalone physical run executed both
`test_local_physical_roundtrip_and_immutable_output` and
`test_invalid_output_prevents_write_and_corruption_fails_readback`.
It verified exact schema and logical nullability, 21 rows, reporting-year
partition 2024, unique employee-month grain, headcount values 0 and 1, hashes,
fixed metadata and complete row multiset equality. Duplicate destinations,
append and overwrite were rejected; existing output bytes remained unchanged.
Invalid output was rejected before write, and corrupt hashes failed readback.
Physical row order was not assumed. These cases also passed inside both full
Spark runtime regressions.

The recorded compatibility runtime in `phase3-emr-runtime.log` is Python
3.11.13, Java 17.0.16 and PySpark 3.5.6. The complete compatibility suite above,
not just the version check, passed. No AWS, actual EMR or S3 was contacted;
S3 interactions in unit tests use mocks. Runtime socket ResourceWarnings and
Docker Compose informational warnings did not cause failures.

Commands run in the resumed validation:

```powershell
docker compose run --rm spark-tests
docker compose run --rm emr-compat-tests
docker compose run --rm spark-tests python -m unittest tests.spark.bronze.test_job tests.spark.bronze.test_reader tests.spark.bronze.test_transform tests.spark.bronze.test_utilities tests.spark.bronze.test_validate tests.spark.bronze.test_writer tests.spark.silver.test_silver -v
python -m compileall -q config spark tests
black --check --target-version py311 spark/gold tests/spark/gold
git diff --check
```

After all regressions passed, compileall passed (exit 0), Black check passed
(21 files unchanged, exit 0), and `git diff --check` passed (exit 0).
Git's LF-to-CRLF warnings are informational. Black was not run in mutation mode.

Reorder/repartition, employee-month grain, assignment resolution, record hashes
and fixed build metadata are stable. For every controlled month, fact row count
equals distinct employees and headcount sum equals the closing population;
row count is not required to equal headcount sum. Names, emails, DOB, gender,
salary, free text and unexpected columns are absent from the exact fact schema.

Repository review confirms no `.env`, dependency, Raw, Bronze or Silver changes;
Bronze behavior changed: NO; Silver behavior changed: NO. No credentials or
secrets were introduced. No generated Parquet remains as repository changes.
No logs or other files were staged; no commit or push was made. No Phase 4,
payroll or attendance transformation logic was added. This completes local
Phase 3 validation and is ready for Phase 4 review, not permission to implement
Phase 4 or publish a production release.
