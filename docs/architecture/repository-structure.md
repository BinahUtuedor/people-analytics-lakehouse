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
-   future Bronze and Silver validation.

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

Owns distributed Bronze and Silver processing.

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

Contains SQL-based analytical models for selected Gold outputs.

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
Bronze testing, including physical Parquet filesystem coverage. The first live
S3 integration run remains pending.

The next verification work should focus on:

``` text
spark/bronze/
tests/spark/
docker/spark-tests/
docs/development/
```

before introducing cloud orchestration. The existing operational-to-Raw
pipeline and root CLI remain separate and unchanged.

------------------------------------------------------------------------

# Planned Implementation Sequence

``` text
1. Complete local Bronze PySpark processing
2. Add Bronze validation and reconciliation
3. Make Spark entry points portable with spark-submit
4. Run Bronze manually on Amazon EMR
5. Add Terraform for S3 / IAM / EMR
6. Add S3 event → Lambda → EMR orchestration
7. Implement Silver with PySpark and Spark SQL
8. Create Gold domain data products
9. Add dbt analytical models and tests
10. Add metadata and lineage publication
11. Add Power BI
12. Add governed FastAPI sharing
13. Add advanced analytics / ML
```

This order keeps infrastructure and orchestration behind proven
transformation logic.

------------------------------------------------------------------------

# Architecture Principles

## Clear Ownership

``` text
Python / SQLAlchemy    Operational simulation
PostgreSQL             Operational persistence
Python ETL             Extraction and Raw movement
Amazon S3              Durable analytical storage
PySpark / Spark SQL    Bronze and Silver engineering
Amazon EMR             Managed Spark execution
AWS Lambda             Event-trigger control plane
dbt                    Gold analytical modelling
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

The immediate engineering focus is:

``` text
S3 Raw
   ↓
Portable PySpark Bronze
   ↓
Amazon EMR
```

followed by event-driven orchestration, Silver processing and
domain-oriented Gold data products.
