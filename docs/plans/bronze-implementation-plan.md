# Bronze PySpark Foundation Implementation Plan

## 1. Purpose

This plan defines the smallest safe implementation required to complete the
local Bronze PySpark foundation for the People Analytics Lakehouse Platform.
The implementation will consume validated Raw Parquet from Amazon S3, preserve
source fidelity and existing extraction lineage, add Bronze technical metadata,
validate and reconcile the result, and publish Parquet to Amazon S3 Bronze.

The same transformation and validation functions must run under local PySpark,
`spark-submit`, and a future Amazon EMR runtime without redesign. Amazon EMR
deployment and S3 event/Lambda orchestration are not part of this plan.

## 2. Current Implementation Boundary

The existing operational-to-Raw pipeline is implemented and must remain
unchanged:

```text
PostgreSQL
    ↓
etl.extract
    ↓
Local Raw Parquet and batch manifest
    ↓
quality.raw_extraction_validation
    ↓
etl.export_s3
    ↓
Amazon S3 Raw
```

The current Spark foundation contains:

- `spark/utilities.py`, which creates a Spark session and configures S3A;
- `spark/bronze/reader.py`, which discovers and reads the latest Raw table
  extraction from S3;
- `spark/bronze/__init__.py`, which identifies transformation, writing, and
  processing as future work;
- S3 and Spark environment settings in `config/settings.py` and `env.example`.

The current reader does not transform, validate, reconcile, or write Bronze
data. It also cannot select a shared extraction batch explicitly, and its
partition parser can overwrite the shared `batch_id` with `extraction_id`.

## 3. Scope

### In scope

- Correct Raw batch and extraction lineage parsing.
- Deterministic selection of an explicit validated Raw batch.
- Local and `spark-submit`-portable Spark session construction.
- Preservation of all Raw columns and business values.
- Bronze technical metadata and deterministic record hashing.
- Bronze structural and lineage validation.
- Raw-to-Bronze row-count reconciliation.
- Batch-specific Amazon S3 Bronze Parquet output.
- A focused `business_units` implementation and test path.
- Unit tests using small local Spark DataFrames.
- One local Spark integration run against S3 Raw and S3 Bronze.
- Narrow documentation updates after the implementation is proven.
- Removal of unused Databricks configuration remnants encountered in the
  directly affected configuration files.

### Out of scope

- Changes to PostgreSQL, ORM models, reference data, simulation, or operational
  data-quality rules.
- Changes to Raw extraction columns, manifests, validation, upload behavior, or
  partition layout.
- Changes to existing root CLI commands or pipeline sequencing.
- Processing all Raw datasets before `business_units` is proven.
- Silver, Gold, dbt, API, catalogue, or analytics implementation.
- Amazon EMR provisioning, packaging, submission, or runtime tuning.
- AWS Lambda or S3 event orchestration.
- Terraform or other infrastructure changes.
- Streaming, Delta Lake, Databricks, Unity Catalog, or notebook-specific APIs.
- New dependencies unless testing demonstrates that the existing `pyspark` and
  `boto3` dependencies cannot support the agreed implementation.

## 4. Compatibility Constraints

The implementation must preserve:

- every existing `main.py` command and its behavior;
- `etl.extract` public functions and output metadata;
- `quality.raw_extraction_validation` as the Raw publication gate;
- `etl.export_s3` key construction and upload behavior;
- the Raw S3 hierarchy:

  ```text
  raw/postgresql/<table>/
      extraction_date=<YYYY-MM-DD>/
          batch_id=<batch-id>/
              extraction_id=<extraction-id>/
                  part-*.parquet
  ```

- existing Raw columns, including `_source_system`, `_source_schema`,
  `_source_table`, `_batch_id`, `_extraction_id`, and `_extracted_at_utc`;
- historical Raw batches and the ability to replay a requested batch;
- standard AWS credential-provider behavior, with no credentials placed in
  Spark configuration or logs.

Bronze must be introduced as a separate job. It must not be appended to
`raw-pipeline` or `full-refresh` during this implementation.

## 5. Target Processing Contract

```text
Validated Amazon S3 Raw batch
    ↓
Resolve table, batch, extraction date, and source objects
    ↓
Read Raw Parquet with PySpark
    ↓
Preserve Raw columns and add Bronze metadata
    ↓
Validate schema, metadata, lineage, and record hashes
    ↓
Reconcile Raw and Bronze row counts
    ↓
Write batch-specific Parquet with error-if-exists behavior
    ↓
Amazon S3 Bronze
```

No business cleansing, reference-data conformity, deduplication, or lifecycle
logic belongs in this flow. Those are Silver responsibilities.

## 6. Proposed Module Responsibilities

### `spark/utilities.py`

Retain the shared Spark session factory, S3A configuration, validation, and
safe shutdown behavior. Change master selection so production logic does not
default to `.master("local[*]")`.

- If an explicit development master is configured, apply it.
- Otherwise, allow `spark-submit` or the managed runtime to supply the master.
- Continue to set the session timezone to UTC.
- Continue to accept an optional compatible `hadoop-aws` Maven coordinate for
  local installations that do not bundle S3A.
- Do not embed AWS credentials.

### `spark/bronze/reader.py`

Keep S3 discovery and Raw reading responsibilities here.

- Parse `extraction_date`, `batch_id`, and `extraction_id` into distinct fields.
- Group objects by the shared batch and table extraction rather than treating
  the immediate file parent as the batch identity.
- Add explicit batch selection; do not rely on "latest" for the production
  Bronze job.
- Retain latest-batch discovery only as a backward-compatible reader utility if
  it remains useful.
- Validate that all selected objects agree on table, date, batch, and
  extraction identity.
- Capture physical Raw file lineage during the Spark read using
  `input_file_name()` and expose it as `_source_file` before the DataFrame
  leaves the reader. Physical file provenance must be captured here because it
  cannot be reconstructed reliably by later transformations.
- Return an immutable descriptor containing the resolved lineage and S3A paths.
- Keep transformation and writing out of this module.

The job must fail explicitly when the requested table or batch is absent,
ambiguous, or structurally inconsistent.

### `spark/bronze/transform.py`

Add one deterministic DataFrame transformation function. It must accept a Raw
DataFrame plus the resolved batch descriptor and return a Bronze DataFrame
without triggering writes.

The function must:

- preserve all Raw columns and their values;
- add `_bronze_ingested_at` as a UTC Spark timestamp;
- add `_extraction_date` from the resolved Raw partition identity;
- add `_record_hash` using Spark's SHA-256 functions;
- preserve the reader-provided `_source_file` without reconstructing or
  replacing its physical Raw file provenance;
- retain the existing `_source_system`, `_source_schema`, `_source_table`,
  `_batch_id`, `_extraction_id`, and `_extracted_at_utc` columns unchanged;
- use only built-in PySpark DataFrame functions;
- avoid `collect()`, pandas conversion, Python record loops, and Python UDFs.

#### Record-hash contract

`_record_hash` will represent source business content rather than ingestion
metadata. Its inputs will be all Raw columns whose names do not begin with `_`,
sorted lexicographically. The canonical payload will be generated with Spark
`to_json(struct(...))` with null fields retained, and hashed with `sha2(...,
256)`.

This contract provides stable column ordering, unambiguous field boundaries,
explicit null representation, and a hash that remains stable when the same
business row is replayed in a later batch. Spark will run in UTC so temporal
serialization is consistent. Tests must lock down null, timestamp, date,
numeric, Unicode, and column-order behavior before the contract is reused
across all tables.

If a Raw dataset contains no business columns, transformation must fail rather
than emit a meaningless common hash.

### `spark/bronze/validate.py`

Provide focused validation functions and a result model. Validation failures
must raise a Bronze-specific exception and prevent the write.

Checks must include:

- all input Raw columns remain present;
- required Bronze metadata columns are present;
- no unexpected column loss or overwrite occurred;
- `_source_system`, `_source_schema`, `_source_table`, `_batch_id`,
  `_extraction_id`, and `_extraction_date` agree with the resolved batch;
- required metadata is non-null for non-empty datasets;
- `_record_hash` is non-null and is a 64-character lowercase hexadecimal
  SHA-256 value for non-empty datasets;
- `_source_file` is populated for non-empty file-backed datasets;
- the Bronze row count equals the Raw row count;
- zero-row datasets retain the required schema and reconcile as zero-to-zero.

Validation may perform Spark actions needed for counts and aggregate failure
metrics, but it must not collect complete datasets to the driver.

### `spark/bronze/writer.py`

Keep output construction and writing separate from transformation.

The initial output contract will be:

```text
s3a://<bucket>/<bronze-prefix>/postgresql/<table>/
    extraction_date=<YYYY-MM-DD>/
        batch_id=<batch-id>/
            part-*.parquet
```

The writer must:

- write Parquet with Snappy compression;
- use the configured bucket and Bronze prefix unless explicit job arguments
  override them;
- write only after transformation, validation, and reconciliation pass;
- provide duplicate-safe batch publication by using Spark's `errorifexists`
  behavior;
- log table, batch, input path, output path, input count, output count, and
  completion status;
- surface write failures without swallowing exceptions.

The initial rerun contract is:

- First processing: the Bronze batch does not exist and the write succeeds.
- Same batch submitted again: existing batch output causes an explicit failure,
  no append occurs, no existing Bronze data is deleted, and no duplicate
  records are published.

This is duplicate-safe batch publication, not true idempotent replacement.
Automatic overwrite, deletion, and true retry/replacement semantics require a
separate deliberate design and are deferred. A failed distributed write can
leave partial objects in S3; operators must remove or quarantine that exact
failed batch prefix before retrying. The job must never delete it automatically.

### `spark/bronze/job.py`

Provide the portable orchestration entry point. Its responsibilities are:

1. Parse and validate arguments.
2. Build the Spark session.
3. Verify S3A availability.
4. Resolve the requested Raw batch.
5. Read Raw Parquet.
6. Transform to Bronze.
7. Validate Bronze and reconcile counts.
8. Write Bronze Parquet.
9. Log the result and stop Spark in `finally`.

Required CLI arguments:

```text
--table <source-table>
--batch-id <validated-raw-batch-id>
```

Optional overrides should be limited to:

```text
--bucket <bucket>
--raw-prefix <prefix>
--bronze-prefix <prefix>
```

Explicit arguments take precedence over environment settings. The job must not
accept arbitrary source SQL or make a PostgreSQL connection.

## 7. Configuration Changes

Change `config/settings.py` narrowly:

- make `SPARK_MASTER` optional rather than defaulting production code to
  `local[*]`;
- retain S3 prefixes, region, Spark application name, log level, and optional
  Maven package settings;
- remove unused `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
  `DATABRICKS_TOKEN`, `CATALOG`, and `SCHEMA` settings.

Change `env.example` narrowly:

- describe `SPARK_MASTER=local[*]` as an optional local-development setting;
- retain a safe placeholder for a Hadoop-compatible `SPARK_JARS_PACKAGES`;
- remove the stale Databricks section;
- add no credentials or environment-specific real values.

Do not rename existing AWS S3 settings.

## 8. Planned File Changes

### Create

```text
spark/bronze/transform.py
spark/bronze/validate.py
spark/bronze/writer.py
spark/bronze/job.py
tests/spark/bronze/test_transform.py
tests/spark/bronze/test_validate.py
tests/spark/bronze/test_reader.py
tests/spark/bronze/test_writer.py
```

Create package `__init__.py` files under `tests/` only if required for reliable
standard-library test discovery in the supported Python environment.

### Change

```text
spark/utilities.py
spark/bronze/reader.py
spark/bronze/__init__.py
config/settings.py
env.example
requirements.txt                 # only if version compatibility must be pinned
README.md                        # status and verified usage only
docs/architecture/data-flow.md   # implemented boundary only
docs/architecture/repository-structure.md
docs/architecture/system-architecture.md
```

No change is planned for `main.py`, `etl/`, `quality/raw_extraction_validation.py`,
the Raw layout, database code, simulator code, or reference data.

Documentation changes occur only after the implementation and tests pass, and
must distinguish implemented local Bronze behavior from planned EMR execution.

## 9. Test Strategy

Use Python's standard `unittest` framework unless the repository deliberately
adopts another test runner in a separate decision. This avoids adding a test
dependency solely for Bronze.

### Reader unit tests

- Parse extraction date, shared batch ID, and extraction ID independently.
- Do not overwrite batch ID with extraction ID.
- Group multiple `part-*.parquet` objects in the same extraction.
- Select the requested batch deterministically.
- Reject missing, ambiguous, or inconsistent batch metadata.
- Capture `_source_file` with `input_file_name()` during a file-backed Spark
  read and expose the physical Raw file URI on each non-empty row.
- Construct the existing Raw and proposed Bronze S3A paths exactly.
- Mock Boto3; unit tests must not access AWS.

### Transformation unit tests

- Preserve all input columns and values.
- Add every required Bronze metadata column.
- Preserve Raw lineage values without mutation.
- Preserve reader-provided `_source_file` values without reconstruction or
  replacement.
- Produce identical hashes when input column order changes.
- Produce different hashes when a business value changes.
- Keep hashes stable when only batch or ingestion metadata changes.
- Verify canonical handling of nulls and representative supported data types.
- Reject input with no business columns.

### Validation unit tests

- Pass a valid non-empty Bronze DataFrame.
- Pass a valid zero-row DataFrame with the required schema.
- Fail missing metadata, null metadata, invalid hash format, lineage mismatch,
  source-column loss, and count mismatch.
- Confirm validation failure occurs before the writer is invoked.

### Writer unit tests

- Build the exact batch-specific Bronze path.
- Use Parquet, Snappy, and `errorifexists`.
- Verify duplicate-safe batch publication: the first write to an absent batch
  succeeds, while submitting the same batch again fails explicitly without
  append, deletion, or duplicate publication.
- Do not write when validation has failed.
- Propagate Spark write exceptions.

### Local integration test

Use one already validated and uploaded `business_units` Raw batch. Run local
Spark against S3, then verify:

- the resolved Raw batch ID is the requested shared batch ID;
- Raw and Bronze counts match;
- the output schema contains all Raw and Bronze columns;
- hashes and required metadata are populated;
- output exists only below the expected Bronze batch prefix;
- resubmitting the same batch fails explicitly and leaves the existing Bronze
  batch unchanged, with no duplicate records published;
- no Raw object or existing CLI behavior changed.

This test performs AWS reads and writes and therefore must be run explicitly
only in a configured development environment.

## 10. Validation Commands

Run the smallest checks first from the repository root.

### Syntax/import validation

```powershell
python -m compileall config spark tests
```

### Bronze unit and local integration tests

```powershell
docker compose run --rm --build spark-tests
```

### Existing CLI regression check

```powershell
python main.py --help
```

Database-backed Raw regression commands should be run only when the configured
development database is available:

```powershell
python main.py validate-raw
```

Do not automatically run `extract`, `upload-s3`, `raw-pipeline`, or
`full-refresh` merely to test Bronze; they mutate data and/or access AWS.

### First local `business_units` Bronze run

After setting `AWS_REGION`, `AWS_S3_BUCKET`, `AWS_S3_RAW_PREFIX`,
`AWS_S3_BRONZE_PREFIX`, AWS credentials or an approved AWS profile, and any
locally required Hadoop-compatible S3A package:

```powershell
.\venv\Scripts\spark-submit.cmd --master "local[*]" spark\bronze\job.py --table business_units --batch-id <validated-raw-batch-id>
```

The batch ID must come from an existing successfully validated and uploaded Raw
manifest. Using an explicit ID makes the test reproducible and matches the
future event-driven job contract.

## 11. Implementation Sequence

1. Add tests that expose the current batch-ID parsing defect and specify
   explicit batch selection.
2. Correct the reader while retaining existing Raw path compatibility.
3. Make Spark master selection runtime-controlled and test local session
   creation.
4. Implement and unit-test the deterministic Bronze transformation and hash
   contract using `business_units` data shapes.
5. Implement and unit-test Bronze structural, lineage, hash, and count
   validation.
6. Implement and unit-test batch-specific Bronze path construction and safe
   write behavior.
7. Assemble the portable `spark/bronze/job.py` entry point.
8. Run syntax checks and all Bronze unit tests.
9. Run `python main.py --help` and available non-mutating regression checks.
10. Run one explicit local S3 Raw-to-Bronze `business_units` batch.
11. Inspect its schema, lineage, record hashes, counts, and exact S3 output
    prefix.
12. Update implementation-status documentation only after the run succeeds.
13. Generalize and test the job across the remaining Raw tables as a subsequent
    task.
14. Prepare manual Amazon EMR execution as a separate task after local
    generalization succeeds.

## 12. Failure and Retry Behavior

- Missing configuration, missing input, invalid batch identity, S3A failure,
  transformation failure, validation failure, reconciliation failure, and
  write failure must terminate the job with a non-zero exit status.
- Validation and reconciliation must finish before a write begins.
- Existing output must cause an explicit failure rather than append or
  overwrite.
- The job must stop Spark in a `finally` block.
- Exceptions may add table, batch, and path context but must not expose AWS
  credentials or other secrets.
- The initial job will not automatically clean partial S3 output because that
  is destructive and cannot be made safely atomic through a simple Spark
  Parquet write.

## 13. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Local Spark and `hadoop-aws` versions are incompatible | Determine Spark's bundled Hadoop version and configure an exactly compatible connector; do not select an arbitrary latest version. |
| Runtime master is accidentally forced to local mode on EMR | Apply `.master()` only for an explicit development override; otherwise inherit `spark-submit` configuration. |
| Batch lineage is confused with extraction lineage | Model and test `batch_id` and `extraction_id` separately and require both to agree with Raw metadata. |
| Hashes change between runs | Fix canonical column selection, ordering, null handling, JSON representation, SHA-256, and UTC behavior in tests. |
| A repeated batch submission duplicates or replaces published data | Provide duplicate-safe batch publication with one batch-specific output path and `errorifexists`; repeated submission fails without append, deletion, or duplicate publication. |
| Failed Spark write leaves partial S3 objects | Fail visibly and require explicit operator cleanup of only the affected batch prefix. |
| Driver performs unbounded work | Use S3 pagination and Spark aggregates; never collect full records. |
| Documentation overstates cloud readiness | Describe only verified local behavior as implemented; retain EMR and Lambda as planned. |
| Existing user documentation changes are overwritten | Patch only directly affected sections after tests, preserving unrelated worktree changes. |

## 14. Definition of Done

The local Bronze foundation is complete when:

1. An explicit validated `business_units` Raw batch can be read from Amazon S3
   by a local `spark-submit` job.
2. Every Raw source column and value is preserved.
3. Required Bronze metadata and deterministic record hashes are present.
4. Structural, lineage, hash, and count validation pass before publication.
5. Raw and Bronze counts reconcile exactly, including zero-row behavior.
6. Parquet is written to the expected batch-specific Amazon S3 Bronze prefix.
7. Duplicate-safe batch publication is verified: first processing writes an
   absent batch successfully, while the same batch submitted again fails
   explicitly without append, deletion, or duplicate publication.
8. Reader, transformation, validation, and writer unit tests pass.
9. The same job code contains no local-only master, Databricks, notebook, or
   EMR-specific transformation assumptions.
10. Existing root CLI behavior and the complete operational-to-Raw pipeline
    remain unchanged.
11. No credentials, tokens, or secrets are introduced.
12. Documentation accurately distinguishes the implemented local foundation
    from future EMR deployment and Lambda orchestration.

Completion of this plan does not claim Amazon EMR readiness until the separate
manual EMR execution phase has been performed and validated. It also does not
claim true idempotent replacement; automatic overwrite, deletion, and true
retry/replacement semantics remain deferred.

## 15. Implementation Progress

Steps 1–8 were implemented without changing the existing Raw pipeline or root
CLI. The original assumption that the complete suite would run directly on
native Windows was refined after physical Parquet tests exercised Hadoop's
local filesystem permission handling. Native Windows passes ordinary Python,
mocked-infrastructure and in-memory Spark coverage, but Hadoop-backed local
writes expect Windows-native support such as `winutils.exe`.

The project deliberately does not distribute unofficial Windows Hadoop
binaries or add Windows-specific workarounds to production Spark code. The
complete Bronze suite now runs in a reproducible Linux Docker runtime using
Python 3.12.10, JDK 21 and pinned PySpark 4.2.0. This retains native Windows for
normal development while giving physical Spark filesystem integration tests
closer parity with the future Linux/EMR runtime.

Syntax compilation, the existing root CLI, the Bronze job CLI, and the full
Docker Bronze suite are locally verified. No live Amazon S3 read/write or
Amazon EMR execution has been performed; those remain separate milestones.
