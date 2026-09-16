# Gold Phase 2 validation

Status: **Phase 2 complete**. Phase 3 has not started. No known required contract coverage gap remains.

## Executable coverage matrix

All test names below are methods of
tests.spark.gold.test_phase2.Phase2SmokeTests unless explicitly qualified.
Assertions compare logical rows or keys, never physical output order.

| Required behaviour | Executable evidence |
| --- | --- |
| No assignment-changing events; hire/termination/cutoff bounds; HIRE and conditional EXIT | test_no_event_employee |
| Promotion then transfer; chronological, contiguous states and before/after keys | test_promotion_then_transfer |
| Transfer then promotion; final state and before/after keys | test_transfer_then_promotion |
| Multiple sequential changes; no intermediate state lost | test_multiple_sequential_changes (four real states for employee 1) |
| Promotion preserves department, location, manager | test_promotion_carries_organisation_and_manager |
| Transfer preserves role; null new manager means no manager | test_transfer_carries_role_and_null_manager_means_no_manager |
| Null, valid and unresolved managers | test_null_manager; test_valid_manager; test_unresolved_manager |
| Unknown department, location and role use 0 with unknown basis; movements agree | test_unknown_dimension_attributes (three subcases) |
| Missing required department/location/role parents reject before publication | test_unresolved_required_dimension_references_fail_before_write (three subcases) |
| Zero-length interval | test_zero_length_interval |
| Negative interval | test_negative_interval |
| Overlap | test_overlap |
| Before hire | test_assignment_before_hire |
| Beyond termination | test_assignment_beyond_termination |
| Beyond source cutoff | test_assignment_beyond_cutoff |
| Future termination cannot extend cutoff | test_future_termination_does_not_extend_source_cutoff |
| Duplicate assignment grain | test_duplicate_assignment_grain |
| Duplicate assignment key | test_duplicate_assignment_key |
| Duplicate promotion IDs, both builders | test_duplicate_promotion |
| Duplicate transfer IDs, both builders | test_duplicate_transfer |
| Duplicate exit IDs, both builders | test_duplicate_exit |
| Duplicate movement keys | test_duplicate_movement |
| Compatible same-day evidence retains both movements and one combined boundary | test_compatible_same_day |
| Conflicting same-day values fail explicitly | test_conflicting_same_day |
| Stable assignment keys, movement keys and hashes across identical builds and reordered/repartitioned inputs | test_rebuild_reorder_repartition_identity_and_hashes |
| Exact Phase 0 names, types, logical nullability; required values; restricted columns excluded | test_schema_nullability_and_governance (both models) |
| HIRE/EXIT/PROMOTION/TRANSFER deltas and absent-side keys | test_before_after_states_and_deltas; sequential-history assertions also check every flattened before/after FK |
| Fixture reconciliation | test_fixture_reconciliation |
| Immutable local output, exact schema/readback, duplicate rejection and unchanged files | test_phase2_local_parquet_roundtrip (both models) |

Silver transfer contracts require new department/location. Transfer events do
not expose a changed job role; it carries forward. Promotion events do not
change organisation or manager; those attributes carry forward. A null
new_manager_id is an explicit absence of manager, not an unchanged value.
No test invents optional department/location semantics for Silver transfers.

The Phase 0 convention distinguishes legitimately unknown attributes (key 0)
from missing required positive parent references (validation failure). No
missing parent is synthesized as a new dimension member. Optional unresolved
managers resolve to 0; employee coverage remains the original source population.

## Shared immutable writer evidence

Both Phase 2 models use spark.gold.writer.write_local_dimension, with the
same errorifexists operation, path guard and readback code as Phase 1.
The writer has no append/overwrite mode argument.

Shared tests in tests.spark.gold.test_local_dimensions cover:

- LocalPathTests.test_existing_destination_fails_before_dataframe_use;
- LocalPathTests.test_missing_verification_does_not_create_output;
- LocalPathTests.test_native_layout_and_forbidden_storage_schemes;
- PhysicalDimensionTests.test_all_five_roundtrip_schema_counts_hashes_metadata_and_unknown;
- PhysicalDimensionTests.test_invalid_output_prevents_write_and_changed_output_fails_verification;
- PhysicalDimensionTests.test_unapproved_physical_column_rejected.

The dedicated Phase 2 physical test proves that both additional models route
through this writer, reject duplicate paths and append/overwrite arguments,
and preserve the original files byte for byte. All paths are disposable local
directories; no AWS/S3 operations are performed.

## Corrections exposed by contract tests

This completion pass required production corrections, not a Phase 2 redesign:

- Restore exact logical nullability through the existing schema-enforcement
  helper for both Phase 2 models.
- Resolve optional manager references against real employee keys; use 0 for
  absent/unresolved managers. Normalize legitimately unknown assignment
  attributes to 0 and the approved unknown history basis.
- Bound intervals by the earliest next event, termination plus one day or
  source cutoff plus one day, including future termination dates.
- Extend the existing immutable local writer to the two implemented Phase 2
  models. Before writing, check exact schema, required parent references and
  full equality with reconstructed source evidence, including hashes/metadata.
  Phase 3 models remain rejected.

The Phase 0 schema and existing builder signatures are unchanged.

## Fixture reconciliation

Employees: 2. Real assignments: 4. Unknown assignments: 1. Total assignments: 5.
HIRE: 2. EXIT: 1. PROMOTION: 1. TRANSFER: 1. Total movements: 5.

## Gate results

Validation date: 2026-09-16. No packages were installed.

| Gate | Total | Passed | Failed | Errors | Skipped | Exit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Native focused Phase 2 | 32 | 31 | 0 | 0 | 1 | 0 |
| Standalone Linux physical Phase 2 | 1 | 1 | 0 | 0 | 0 | 0 |
| Complete/main Spark (95 Gold + 41 Bronze/Silver) | 136 | 136 | 0 | 0 | 0 | 0 |
| Local EMR compatibility | 136 | 136 | 0 | 0 | 0 | 0 |
| Separate complete Bronze/Silver regression | 41 | 41 | 0 | 0 | 0 | 0 |

The native skip is the Windows physical Parquet safeguard; the Linux equivalent
passed. Both complete suites also executed the physical test on the final
implementation. All regression evidence was rerun in this completion pass
because production code changed; no earlier gate was substituted.
The EMR image reports Python 3.11.13, Java 17.0.16 and PySpark 3.5.6.

Commands used from the repository root:

- Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to (Get-Command python).Source,
  then run python -m unittest tests.spark.gold.test_phase2 -v.
- docker compose run --rm spark-tests python -m unittest
  tests.spark.gold.test_phase2.Phase2SmokeTests.test_phase2_local_parquet_roundtrip -v
- docker compose run --rm spark-tests
- docker compose run --rm emr-compat-tests
- docker compose run --rm spark-tests python -m unittest
  tests.spark.bronze.test_job tests.spark.bronze.test_reader
  tests.spark.bronze.test_transform tests.spark.bronze.test_utilities
  tests.spark.bronze.test_validate tests.spark.bronze.test_writer
  tests.spark.silver.test_silver -v

Local detailed logs are in logs/phase2-focused.log,
logs/phase2-linux-physical.log, logs/phase2-main-spark.log,
logs/phase2-emr-compat.log and logs/phase2-bronze-silver.log.
These logs are ignored working artifacts, not committed evidence files.

## Final quality and scope checks

All passed: python -m compileall -q config spark tests;
black --check --target-version py311 spark/gold tests/spark/gold;
git diff --check.

The final working tree contains only the expected Gold production corrections,
Phase 2 tests/shared-writer guard test, this coverage report and Gold status
updates in the README and architecture/implementation documentation.
No environment/dependency changes, credentials, AWS/S3 access, Raw/Bronze/Silver
changes, staging, commits or pushes occurred in that validation pass. No Phase 3 logic
was added. Production Gold publication remains outside this local proof.
