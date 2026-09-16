# Repository Structure

## Overview

The **People Analytics Lakehouse Platform** uses a modular repository
structure that separates operational simulation, reference-data
management, data quality, extraction, Spark processing, analytics
engineering, governance, orchestration, infrastructure and consumption.

The repository is designed around the current implementation and the
planned AWS architecture using **Amazon S3, Amazon EMR, AWS Lambda,
PySpark, Spark SQL, dbt and Power BI**.

------------------------------------------------------------------------

# Repository Layout

This tree combines existing and planned components; it is not a filesystem inventory.
Implemented: `PostgreSQL -> Parquet Extraction -> S3 Raw -> Spark Bronze -> Spark Silver`.
Gold Phase 2 complete; [Gold contract](gold-layer.md) is authoritative.
`spark/gold/` contains Phase 0 contracts, Phase 1 core dimensions, Phase 2
assignment/movement builders and local immutable writer proof. Phase 3 has not
started. No dbt implementation, Gold job, CLI or production release exists.

``` text
people-analytics-lakehouse-platform/
│
├── config/
│   ├── __init__.py
│   ├── settings.py
│   ├── constants.py
│   └── logger.py
│
├── reference_data/
│   ├── __init__.py
│   ├── loader.py
│   ├── business_units.yml
│   ├── departments.yml
│   ├── locations.yml
│   ├── job_roles.yml
│   ├── attendance_statuses.yml
│   ├── genders.yml
│   ├── leave_types.yml
│   ├── employment_types.yml
│   ├── exit_reasons.yml
│   ├── training_categories.yml
│   ├── public_holidays.yml
│   └── absence_reasons.yml
│
├── database/
│   ├── __init__.py
│   ├── connection.py
│   ├── models/
│   └── seed.py
│
├── simulator/
│   ├── __init__.py
│   ├── simulator.py
│   ├── effective_dates.py
│   ├── employees.py
│   ├── recruitment.py
│   ├── attendance.py
│   ├── payroll.py
│   ├── leave.py
│   ├── training.py
│   ├── promotions.py
│   ├── transfers.py
│   ├── performance.py
│   ├── surveys.py
│   ├── manager_feedback.py
│   ├── employee_exits.py
│   └── exit_interviews.py
│
├── quality/
│   ├── __init__.py
│   ├── business_rules.py
│   ├── reference_data_checks.py
│   ├── raw_checks.py
│   └── ...
│
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── export_s3.py
│   └── ...
│
├── spark/
│   ├── __init__.py
│   ├── session.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── paths.py
│   │   ├── metadata.py
│   │   └── validation.py
│   │
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── job.py
│   │   ├── transform.py
│   │   └── validate.py
│   │
│   └── silver/
│       ├── __init__.py
│       ├── job.py
│       ├── transform.py
│       └── validate.py
│
├── dbt/
│   └── people_analytics/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       │       ├── workforce/
│       │       ├── recruitment/
│       │       ├── learning/
│       │       ├── performance/
│       │       ├── payroll/
│       │       └── attrition/
│       ├── tests/
│       ├── macros/
│       └── seeds/
│
├── orchestration/
│   ├── lambda/
│   │   ├── raw_object_created.py
│   │   └── requirements.txt
│   ├── schedules/
│   └── scripts/
│       ├── submit_emr_job.sh
│       └── run_pipeline.sh
│
├── integrations/
│   ├── __init__.py
│   └── ...
│
├── metadata/
│   ├── __init__.py
│   └── ...
│
├── catalogue/
│   ├── __init__.py
│   └── ...
│
├── data_sharing/
│   ├── __init__.py
│   └── ...
│
├── api/
│   ├── __init__.py
│   ├── main.py
│   ├── routes/
│   ├── schemas/
│   ├── services/
│   └── security/
│
├── analytics/
│   ├── attrition_prediction.py
│   ├── burnout_prediction.py
│   ├── promotion_prediction.py
│   └── workforce_forecasting.py
│
├── dashboards/
│   ├── workforce/
│   ├── recruitment/
│   ├── learning/
│   ├── performance/
│   ├── payroll/
│   ├── attrition/
│   ├── semantic_models/
│   └── screenshots/
│
├── notebooks/
│   ├── data_generation.ipynb
│   ├── eda.ipynb
│   └── machine_learning.ipynb
│
├── data/
│   └── raw/
│       └── postgres/
│
├── sql/
│   ├── postgres/
│   └── spark/
│
├── tests/
│   ├── database/
│   ├── simulator/
│   ├── reference_data/
│   ├── quality/
│   ├── etl/
│   ├── spark/
│   ├── dbt/
│   ├── orchestration/
│   ├── metadata/
│   ├── catalogue/
│   ├── data_sharing/
│   └── api/
│
├── terraform/
│   ├── modules/
│   │   ├── s3/
│   │   ├── iam/
│   │   ├── lambda/
│   │   ├── emr/
│   │   ├── networking/
│   │   ├── secrets/
│   │   ├── database/
│   │   └── api/
│   ├── environments/
│   │   ├── dev/
│   │   ├── staging/
│   │   └── production/
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── versions.tf
│
├── deployment/
│   ├── docker/
│   ├── configs/
│   └── scripts/
│
├── .github/
│   └── workflows/
│       ├── lint.yml
│       ├── test.yml
│       ├── security-scan.yml
│       ├── spark-ci.yml
│       ├── dbt-ci.yml
│       ├── terraform.yml
│       └── deploy.yml
│
├── docs/
│   ├── README.md
│   ├── architecture/
│   │   ├── repository-structure.md
│   │   ├── system-architecture.md
│   │   ├── database-architecture.md
│   │   └── data-flow.md
│   ├── development/
│   ├── operations/
│   └── governance/
│
├── docker/
│   └── spark-tests/
│       └── Dockerfile
│
├── .dockerignore
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── main.py
├── requirements.txt
├── requirements-spark-tests.txt
├── README.md
└── LICENSE
```

The structure above is the **target repository layout**. Directories for
planned capabilities should be introduced when implementation reaches
those capabilities rather than created solely to mirror the target tree.

------------------------------------------------------------------------

# Component Responsibilities

## `config/`

Central application configuration, constants and logging.

Environment-specific secrets remain outside source control.

------------------------------------------------------------------------

## `reference_data/`

Contains governed YAML reference datasets and their loader.

Reference data supplies controlled values for organisational structures
and simulator domains.

------------------------------------------------------------------------

## `database/`

Contains SQLAlchemy connectivity, ORM models and database seeding.

PostgreSQL remains the operational source system.

------------------------------------------------------------------------

## `simulator/`

Generates synthetic workforce entities and events.

The simulator consumes governed reference values from PostgreSQL and
preserves employee lifecycle dependencies.

------------------------------------------------------------------------

## `quality/`

Contains reusable validation logic and pipeline quality gates.

Responsibilities include:

-   reference-data validation;
-   operational business rules;
-   duplicate and integrity checks;
-   lifecycle reconciliation;
-   Raw validation;
-   implemented Bronze and Silver validation in `spark/`.

------------------------------------------------------------------------

## `etl/`

Owns operational extraction and Raw data movement.

Current responsibilities include:

``` text
PostgreSQL
    ↓
Parquet
    ↓
Raw validation
    ↓
Amazon S3
```

------------------------------------------------------------------------

## `spark/`

Owns implemented Bronze/Silver processing and Gold Phase 1 dimensions and
Phase 2 assignment/movement local proofs. Later Gold models and production
publication remain planned.

Spark code should be portable between local development and Amazon EMR.

Recommended separation:

``` text
spark/
├── common/
├── bronze/
└── silver/
```

Each layer should separate job entry points, transformation logic and
validation where practical.

------------------------------------------------------------------------

## `dbt/`

Planned: reporting marts, semantic presentation, lightweight aggregations,
BI-facing views and approved KPI presentation over Spark Gold. Do not duplicate
Spark transformations. The SQL serving engine remains undecided.

Gold is organised by analytical domain:

``` text
marts/
├── workforce/
├── recruitment/
├── learning/
├── performance/
├── payroll/
└── attrition/
```

dbt owns analytical SQL dependencies, tests and documentation where
appropriate.

------------------------------------------------------------------------

## `orchestration/`

Contains event-driven and scheduled workflow control code.

The first target event-driven pattern is:

``` text
S3 Raw ObjectCreated
        ↓
AWS Lambda
        ↓
Amazon EMR Spark Job
```

Transformation logic must remain in `spark/`, not in Lambda.

Shell scripts support repeatable job submission and operational
execution.

------------------------------------------------------------------------

## `integrations/`

Provides a future framework for external enrichment data.

External data should enter governed ingestion paths rather than
bypassing Raw and quality controls.

------------------------------------------------------------------------

## `metadata/`

Contains platform metadata definitions and publication logic.

Metadata should be added as real Bronze, Silver and Gold assets are
implemented.

------------------------------------------------------------------------

## `catalogue/`

Represents the planned enterprise metadata catalogue and
business-governance layer.

------------------------------------------------------------------------

## `data_sharing/`

Owns policy and governance for approved data products, consumers, access
and audit records.

------------------------------------------------------------------------

## `api/`

Provides the planned FastAPI consumption layer.

The API exposes curated data products rather than operational or
low-level lakehouse tables.

------------------------------------------------------------------------

## `analytics/`

Contains advanced workforce analytics and machine-learning workloads.

Models should consume governed Silver or Gold datasets.

------------------------------------------------------------------------

## `dashboards/`

Contains Power BI artefacts and portfolio outputs organised by
analytical domain.

------------------------------------------------------------------------

## `sql/`

Contains SQL assets that are useful outside dbt.

``` text
sql/
├── postgres/
└── spark/
```

Spark SQL belongs here when it is maintained as standalone SQL rather
than embedded in transformation modules.

------------------------------------------------------------------------

## `tests/`

Mirrors the main architectural capabilities so each subsystem can be
tested independently.

Spark tests should focus on deterministic transformation logic that can
run locally without requiring an EMR environment.

------------------------------------------------------------------------

## `terraform/`

Provides AWS Infrastructure as Code.

Target modules include:

-   S3;
-   IAM;
-   Lambda;
-   EMR;
-   networking;
-   secrets;
-   database infrastructure;
-   API infrastructure.

------------------------------------------------------------------------

## `.github/`

Contains CI/CD workflows for code quality, tests, Spark validation, dbt,
Terraform and deployment.

------------------------------------------------------------------------

# Current Implementation Boundary

The currently implemented core is:

``` text
config/
reference_data/
database/
simulator/
quality/
etl/
data/raw/
main.py
```

The project now has a portable Bronze code foundation under `spark/bronze/`.
It provides explicit Raw batch discovery and reading, transformation, validation,
reconciliation, duplicate-safe writing and a `spark-submit` entry point. The
`spark-tests` Docker Compose service provides the Linux runtime for complete
Bronze testing, including physical Parquet filesystem coverage. All 17
supported datasets in one shared Raw batch have been published and verified in
S3 Bronze.

`scripts/build_emr_bundle.py` now creates an ignored, checksummed deployment
bundle for that same entry point. `docker/emr-compat/` provides a non-live
Python 3.11, Java 17 and Spark 3.5.6 compatibility gate for the initial EMR
7.13 target. Neither component provisions infrastructure or submits a cloud
job.

Cloud compatibility preparation remains available in:

``` text
spark/bronze/
tests/spark/
docker/spark-tests/
docker/emr-compat/
scripts/build_emr_bundle.py
docs/development/
```

before introducing cloud orchestration. The existing operational-to-Raw
pipeline and root CLI remain separate and unchanged.

------------------------------------------------------------------------

# Implementation Sequence and Status

``` text
1. Local Bronze PySpark processing complete
2. Bronze validation and reconciliation complete
3. Portable Bronze entry point complete
4. Run Bronze manually on Amazon EMR
5. Add Terraform for S3 / IAM / EMR
6. Add S3 event → Lambda → EMR orchestration
7. Silver implemented in commit 7caa41f
8. Gold Phase 2 complete; Phase 3 has not started and needs separate approval
9. Add dbt analytical models and tests
10. Add metadata and lineage publication
11. Add Power BI
12. Add governed FastAPI sharing
13. Add advanced analytics / ML
```

The original sequence now includes completed local Silver work. Cloud execution
and orchestration remain deferred; use the Gold phased plan for the next local
implementation review.

------------------------------------------------------------------------

# Architecture Principles

## Clear Ownership

``` text
Python / SQLAlchemy    Operational simulation
PostgreSQL             Operational persistence
Python ETL             Extraction and Raw movement
Amazon S3              Durable analytical storage
PySpark / Spark SQL    Bronze/Silver and planned reusable Gold engineering
Amazon EMR             Managed Spark execution
AWS Lambda             Event-trigger control plane
dbt                    Future reporting marts and KPI presentation
Power BI               Business intelligence
FastAPI                Governed delivery
Terraform              Infrastructure provisioning
GitHub Actions         CI/CD
```

## Portability

Spark transformation code should not depend on notebook-only or
vendor-specific APIs.

## Event-Driven Where Appropriate

Data-arrival workflows can use S3 events and Lambda, while recurring and
dependency-heavy workloads may use scheduled orchestration.

## Domain-Oriented Gold

Gold datasets are grouped into workforce, recruitment, learning,
performance, payroll and attrition products.

## No Direct Raw Consumption

Business users, APIs and ML consumers use curated Silver or Gold assets
rather than Raw data.

## Traceability

Batch IDs, source files, extraction metadata and record hashes provide
end-to-end lineage.

------------------------------------------------------------------------

# Summary

The repository structure supports a staged evolution from the already
working operational-to-Raw platform into an AWS-based distributed data
platform.

The implemented processing boundary is:

```text
PostgreSQL -> Parquet Extraction -> S3 Raw -> Spark Bronze -> Spark Silver
```

Gold Phase 2 complete locally. Phase 3 has not started; review the validation evidence and follow
[the phased plan](../plans/gold-implementation-plan.md). Cloud execution and
event-driven orchestration remain separately approval-gated.
