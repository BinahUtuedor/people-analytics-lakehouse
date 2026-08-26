# End-to-End Platform Runbook

## Scope

This runbook describes the currently implemented workflow from a fresh clone
through PostgreSQL simulation, validated Raw publication, and multi-table
Amazon S3 Bronze processing. Amazon EMR, Lambda, Silver, and Gold remain outside
the implemented boundary.

Commands are shown for PowerShell from the repository root. Cloud-mutating
steps require an authorised development bucket and AWS identity.

## 1. Prerequisites

Install:

- Git;
- Python 3.12;
- Docker Desktop using Linux containers;
- Java 21 for native Spark development only.

Docker supplies the authoritative Linux Spark runtime, including Python 3.12,
Java 21, and PySpark 4.2.0. Do not install unofficial `winutils.exe` binaries.

## 2. Clone and Install

```powershell
git clone <repository-url>
Set-Location people-analytics-lakehouse-platform
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item env.example .env
```

Populate `.env` locally. At minimum, configure PostgreSQL, `AWS_REGION`,
`AWS_S3_BUCKET`, the Raw and Bronze prefixes, and an approved AWS credential
source. Never commit `.env`. Prefer temporary credentials or IAM roles over
long-lived access keys.

## 3. Start and Initialise PostgreSQL

Start only the operational database unless pgAdmin is also needed:

```powershell
docker compose up -d postgres
python -m database.create_schema
python -m database.seed
```

`database.seed` validates governed YAML reference data and is safe to rerun.
Schema creation and reference seeding are deliberately explicit; `full-refresh`
does not perform them automatically.

## 4. Generate and Validate Operational Data

For a fresh, reproducible synthetic population:

```powershell
python main.py simulate --full-refresh
python main.py validate
```

Validation is a gate. Do not extract or publish when it fails.

## 5. Extract, Validate, and Publish Raw

Run the implemented Raw pipeline:

```powershell
python main.py raw-pipeline
```

This performs extraction, PostgreSQL-to-Raw reconciliation, and S3 upload in
order. It writes a shared batch ID to:

```text
data/raw/postgres/_manifests/batch_id=<batch-id>.json
data/raw/postgres/_upload_manifests/batch_id=<batch-id>.json
```

Record that batch ID. Confirm the extraction manifest has no failed tables and
the upload manifest reports `SUCCESS` for every supported dataset before
running Bronze.

To rebuild simulation and execute the complete implemented operational-to-Raw
flow in one command after schema creation and seeding:

```powershell
python main.py full-refresh
```

## 6. Run the Bronze Test Gate

```powershell
docker compose run --rm --build spark-tests
```

All tests must pass before a live Bronze publication. This suite does not
connect to AWS.

## 7. Publish All Supported Datasets to Bronze

For a new validated Raw batch whose Bronze paths are absent:

```powershell
docker compose run --rm spark-bronze `
  --all-tables `
  --batch-id <validated-batch-id>
```

The supported dataset registry is `config/datasets.py`; both Raw extraction and
Bronze processing consume it. Each table is independently read, transformed,
validated, reconciled, and written with `errorifexists`. The job fails fast if
any dataset fails. Outputs already published before a failure remain intact.

## 8. Resume or Verify a Batch

Use the explicit verification mode after a partial run or to verify a complete
batch:

```powershell
docker compose run --rm spark-bronze `
  --all-tables `
  --batch-id <validated-batch-id> `
  --verify-existing
```

This is not a blind skip. For each existing output, the job reads Bronze and
rechecks schema preservation, lineage, metadata, deterministic record hashes,
and Raw-to-Bronze row counts. Missing outputs are published only after the same
checks pass. Without `--verify-existing`, an existing output fails explicitly
and is never appended, overwritten, or deleted.

The initial all-table verified batch is:

```text
fc4e3604-70f2-43f8-96ff-419e9d3046e5
```

It contains 17 datasets and reconciles 885,037 Raw rows to 885,037 Bronze rows.

## 9. Expected S3 Layout

```text
s3://<bucket>/
├── raw/postgresql/<table>/
│   └── extraction_date=<date>/batch_id=<batch>/extraction_id=<id>/
└── bronze/postgresql/<table>/
    └── extraction_date=<date>/batch_id=<batch>/
```

Each successful Bronze batch path contains Parquet output and `_SUCCESS`.

## 10. Failure and Recovery

- Never delete, overwrite, or repair Raw data in place.
- Validation failures prevent publication of the affected dataset.
- A failed distributed write may leave a partial Bronze prefix. Inspect that
  exact batch path before retrying; the job never deletes it automatically.
- `--verify-existing` succeeds only for a complete, valid existing dataset. It
  fails on partial, inconsistent, corrupt, or incorrectly hashed output.
- Destructive cleanup of a failed Bronze prefix requires a separate, explicit
  operator decision and is not part of this runbook.

## 11. Stop Local Services

```powershell
docker compose down
```

Omit `-v` unless intentionally deleting PostgreSQL and pgAdmin volumes.

## 12. Current Boundary and Next Step

The implemented and verified boundary is:

```text
Governed YAML → PostgreSQL → Validation → Raw Parquet → S3 Raw → S3 Bronze
```

The repository now includes non-live EMR packaging and a Spark 3.5.6
compatibility gate. Follow `docs/operations/emr-manual-runbook.md` to build and
review the artifacts and runtime requirements. Manual EMR execution still
requires separate explicit approval. Do not add Lambda orchestration before
that manual EMR run succeeds.
