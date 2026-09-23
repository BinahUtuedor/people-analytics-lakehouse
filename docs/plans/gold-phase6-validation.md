# Gold Phase 6 validation — MVP-wide release readiness

Phase 6 adds `spark/gold/release_validation.py`, a shared integration layer
over the ten approved MVP models. It composes existing contracts and
model-specific validators; it does not create a new fact or publish output.

The validated inventory is `dim_employee`, `dim_department`, `dim_location`,
`dim_job_role`, `dim_date`, `dim_employee_assignment`,
`fact_employee_movement`, `fact_workforce_monthly`, `fact_payroll`, and
`fact_attendance`. Primary keys and grains remain those defined in
`contracts.py`. Integration checks cover positive employee, organisation,
manager, assignment, and date references, duplicate approved keys, shared
batch/build/timestamp metadata, and deterministic content evidence.

The controlled fixture combines hire, promotion, transfer, manager history,
termination, payroll period-end attribution, attendance work-date attribution,
and monthly snapshots. A mid-month cutoff of 2024-04-15 is preserved: the date
dimension contains April month-end for reference, attendance and movement stay
cutoff-bounded, workforce remains closed-month-only, and payroll ending April
15 may use payroll month key 20240430. Current employee organisation is not
used for historical fact attribution.

The release manifest remains the authoritative lifecycle contract: BUILDING,
VALIDATED, ACCEPTED, and FAILED. It requires exactly all ten MVP models,
partition inventories, row/content evidence, and every mandatory verification
check before VALIDATED or ACCEPTED. Missing/extra inventory, failed checks,
cross-build metadata, corrupted readback, and immutable destination reuse fail
closed. `_SUCCESS` alone cannot establish acceptance.

The Phase 6 focused suite passed **6/6** (exit 0), including cross-model
references, dates, historical assignment coherence, workforce/movement
relationships, deterministic manifest identity, negative failures, and physical
readback/immutability. Existing model-specific physical suites remain the
authoritative proof for each model; the integration test verifies the shared
release inventory and writer policy.

Final terminal evidence: complete Gold **179/179** (exit 0), main Linux Spark
**220/220** (exit 0), local EMR compatibility **220/220** (exit 0), and safe
Bronze/Silver **41/41** (exit 0). The EMR-compatible runtime remains Python
3.11.13, Java 17.0.16, and PySpark 3.5.6. No AWS or S3 access is used.
Deterministic hashes are integrity identifiers, not
anonymisation. No post-MVP model, dbt, serving engine, Terraform, or
orchestration work is included.

## Pre-Phase-7 corrective gate

The architecture review identified G01-G04 after the historical Phase 6
validation. Canonical key parity, historical evidence/endpoint reconciliation,
EXIT-day assignment lookup and governed absence categories are addressed in
the [bounded correction record](gold-pre-phase7-corrections.md).
It records compatibility, rebuild requirements and new regression evidence. Historical phase results
remain unchanged; Phase 7 has not started.

## R01 architecture-reconfirmation correction

After the G01-G04 gate, review identified caller-authorized acceptance in the
manifest helpers. The historical Phase 6 tests checked supplied success claims;
they did not establish an evidence-backed release promotion boundary. Their
recorded results remain historical evidence, not proof of that missing boundary.

R01 selects Pattern B: the frame factory is BUILDING-only, and direct manifests
allow BUILDING/FAILED only. VALIDATED/ACCEPTED remain intended lifecycle states
reserved for a future dedicated API binding logical and physical validation to
the exact build and inventory. No such promotion API exists today. Diagnostic
flags cannot authorize acceptance. Existing validators and immutable local
writer/readback behavior are unchanged; no production publisher is introduced.

See the [bounded R01 correction record](gold-pre-phase7-corrections.md#r01-architecture-reconfirmation-correction).
