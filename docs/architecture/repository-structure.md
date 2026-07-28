# Repository Structure

## Overview

The **People Analytics Lakehouse Platform** follows a modular, production-oriented architecture designed around modern data engineering, lakehouse, analytics engineering and data-governance practices.

The repository separates the operational HR system, reference-data management, data quality, ingestion, lakehouse processing, analytics engineering, governance, orchestration and data consumption into clearly defined components.

The platform architecture is centred on:

* **Python and SQLAlchemy** for operational data simulation and ingestion;
* **PostgreSQL** as the operational HR source system;
* **Amazon S3** as the immutable Raw data lake;
* **Databricks on AWS** for scalable lakehouse processing;
* **Delta Lake** for Bronze and Silver managed datasets;
* **PySpark** for Bronze and Silver engineering;
* **dbt-databricks** for Gold analytical modelling;
* **Unity Catalog** for lakehouse governance;
* **Airflow** for workflow orchestration;
* **Power BI** for business intelligence;
* **FastAPI** for governed data-product delivery;
* **Terraform** for infrastructure provisioning;
* **GitHub Actions** for CI/CD.

---

# Repository Layout

```text
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
│   ├── employment_types.yml
│   ├── genders.yml
│   ├── attendance_statuses.yml
│   ├── absence_reasons.yml
│   ├── public_holidays.yml
│   ├── leave_types.yml
│   ├── training_categories.yml
│   └── exit_reasons.yml
│
├── database/
│   ├── __init__.py
│   ├── base.py
│   ├── connection.py
│   ├── create_schema.py
│   ├── seed.py
│   │
│   └── models/
│       ├── __init__.py
│       ├── business_unit.py
│       ├── department.py
│       ├── location.py
│       ├── job_role.py
│       ├── employment_type.py
│       ├── gender.py
│       ├── attendance_status.py
│       ├── absence_reason.py
│       ├── public_holiday.py
│       ├── leave_type.py
│       ├── training_category.py
│       ├── exit_reason.py
│       ├── employee.py
│       ├── employee_exit.py
│       ├── attendance.py
│       ├── payroll.py
│       ├── leave.py
│       ├── recruitment.py
│       ├── promotion.py
│       ├── transfer.py
│       ├── training.py
│       ├── performance_review.py
│       ├── employee_survey.py
│       ├── manager_feedback.py
│       └── exit_interview.py
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
│   ├── performance.py
│   ├── promotion.py
│   ├── transfer.py
│   ├── surveys.py
│   ├── manager_feedback.py
│   ├── exits.py
│   └── exit_interviews.py
│
├── quality/
│   ├── __init__.py
│   ├── validation.py
│   ├── raw_extraction_validation.py
│   ├── reference_data_checks.py
│   ├── duplicate_checks.py
│   ├── integrity_checks.py
│   ├── business_rules.py
│   ├── workforce_lifecycle_checks.py
│   ├── validate_promotion_salary.py
│   ├── expectations.py
│   ├── metrics.py
│   ├── report.py
│   └── exceptions.py
│
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   └── export_s3.py
│
├── spark/
│   ├── __init__.py
│   │
│   ├── common/
│   │   ├── __init__.py
│   │   ├── spark_session.py
│   │   ├── delta.py
│   │   ├── quality.py
│   │   └── utilities.py
│   │
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── reader.py
│   │   ├── transformer.py
│   │   ├── writer.py
│   │   └── processor.py
│   │
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── employees.py
│   │   ├── attendance.py
│   │   ├── payroll.py
│   │   ├── leave.py
│   │   ├── recruitment.py
│   │   ├── training.py
│   │   └── workforce_events.py
│   │
│   └── jobs/
│       ├── __init__.py
│       ├── bronze_job.py
│       └── silver_job.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── dimensions/
│   │   ├── facts/
│   │   └── marts/
│   │       ├── workforce/
│   │       ├── attendance/
│   │       ├── finance/
│   │       ├── recruitment/
│   │       └── learning/
│   │
│   ├── macros/
│   ├── snapshots/
│   ├── tests/
│   ├── seeds/
│   ├── analyses/
│   ├── docs/
│   │   ├── exposures.yml
│   │   ├── metrics.yml
│   │   └── groups.yml
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── metadata/
│   ├── __init__.py
│   ├── loader.py
│   │
│   ├── schemas/
│   │   └── ...
│   │
│   ├── lineage/
│   │   ├── source_to_raw.yml
│   │   ├── raw_to_bronze.yml
│   │   ├── bronze_to_silver.yml
│   │   ├── silver_to_gold.yml
│   │   └── gold_to_consumers.yml
│   │
│   ├── ownership.yml
│   ├── classifications.yml
│   └── glossary.yml
│
├── catalogue/
│   ├── __init__.py
│   ├── base.py
│   ├── models.py
│   ├── create_schema.py
│   ├── register_assets.py
│   ├── register_columns.py
│   ├── register_lineage.py
│   ├── register_quality_results.py
│   ├── sync_postgres.py
│   ├── sync_databricks.py
│   ├── sync_dbt.py
│   └── report.py
│
├── data_sharing/
│   ├── __init__.py
│   ├── base.py
│   ├── create_schema.py
│   ├── seed.py
│   ├── repositories.py
│   ├── services.py
│   │
│   └── models/
│       ├── __init__.py
│       ├── api_consumer.py
│       ├── api_key.py
│       ├── access_policy.py
│       ├── data_product.py
│       ├── access_log.py
│       └── export_request.py
│
├── api/
│   ├── __init__.py
│   ├── main.py
│   ├── dependencies.py
│   ├── exceptions.py
│   │
│   ├── config/
│   │   ├── __init__.py
│   │   └── api_settings.py
│   │
│   └── v1/
│       ├── routes/
│       │   ├── __init__.py
│       │   ├── health.py
│       │   ├── workforce.py
│       │   ├── attendance.py
│       │   ├── payroll.py
│       │   ├── recruitment.py
│       │   └── data_products.py
│       │
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── workforce.py
│       │   ├── attendance.py
│       │   ├── payroll.py
│       │   ├── recruitment.py
│       │   └── common.py
│       │
│       ├── services/
│       │   ├── __init__.py
│       │   ├── workforce_service.py
│       │   ├── attendance_service.py
│       │   ├── payroll_service.py
│       │   └── export_service.py
│       │
│       ├── repositories/
│       │   ├── __init__.py
│       │   ├── workforce_repository.py
│       │   ├── attendance_repository.py
│       │   ├── payroll_repository.py
│       │   └── recruitment_repository.py
│       │
│       ├── security/
│       │   ├── __init__.py
│       │   ├── authentication.py
│       │   ├── authorization.py
│       │   ├── api_keys.py
│       │   ├── permissions.py
│       │   └── rate_limiting.py
│       │
│       └── middleware/
│           ├── __init__.py
│           ├── audit_logging.py
│           ├── request_id.py
│           └── security_headers.py
│
├── integrations/
│   ├── __init__.py
│   ├── base_client.py
│   ├── schemas.py
│   ├── exceptions.py
│   │
│   └── providers/
│       ├── holidays.py
│       ├── labour_market.py
│       ├── exchange_rates.py
│       └── geocoding.py
│
├── analytics/
│   ├── __init__.py
│   ├── attrition_prediction.py
│   ├── burnout_prediction.py
│   ├── promotion_prediction.py
│   └── workforce_forecasting.py
│
├── airflow/
│   ├── dags/
│   │   ├── operational_to_raw.py
│   │   ├── process_bronze.py
│   │   ├── process_silver.py
│   │   ├── run_dbt.py
│   │   └── publish_metadata.py
│   │
│   ├── plugins/
│   └── requirements.txt
│
├── dashboards/
│   ├── reports/
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
│   └── databricks/
│
├── tests/
│   ├── database/
│   ├── simulator/
│   ├── reference_data/
│   ├── quality/
│   ├── etl/
│   ├── spark/
│   ├── dbt/
│   ├── metadata/
│   ├── catalogue/
│   ├── data_sharing/
│   └── api/
│
├── terraform/
│   ├── modules/
│   │   ├── s3/
│   │   ├── iam/
│   │   ├── networking/
│   │   ├── secrets/
│   │   ├── databricks/
│   │   ├── database/
│   │   ├── airflow/
│   │   └── api/
│   │
│   ├── environments/
│   │   ├── dev/
│   │   ├── staging/
│   │   └── production/
│   │
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── versions.tf
│
├── deployment/
│   ├── docker/
│   │   └── api.Dockerfile
│   │
│   ├── configs/
│   │   ├── dev.env.example
│   │   ├── staging.env.example
│   │   └── production.env.example
│   │
│   └── scripts/
│       ├── deploy.sh
│       ├── rollback.sh
│       └── health_check.sh
│
├── .github/
│   └── workflows/
│       ├── lint.yml
│       ├── test.yml
│       ├── security-scan.yml
│       ├── dbt-ci.yml
│       ├── terraform.yml
│       └── deploy.yml
│
├── docs/
│   ├── README.md
│   │
│   ├── architecture/
│   │   ├── repository-structure.md
│   │   ├── system-architecture.md
│   │   ├── database-architecture.md
│   │   ├── data-flow.md
│   │   ├── deployment-architecture.md
│   │   └── technology-stack.md
│   │
│   ├── data-governance/
│   │   ├── data-dictionary.md
│   │   ├── data-lineage.md
│   │   ├── reference-data.md
│   │   ├── data-quality-framework.md
│   │   ├── metadata-framework.md
│   │   ├── data-catalogue.md
│   │   └── security-and-data-sharing.md
│   │
│   ├── implementation/
│   │   ├── local-development.md
│   │   ├── postgres.md
│   │   ├── simulator.md
│   │   ├── raw-ingestion.md
│   │   ├── databricks.md
│   │   ├── bronze.md
│   │   ├── silver.md
│   │   ├── dbt.md
│   │   ├── airflow.md
│   │   ├── api.md
│   │   └── deployment.md
│   │
│   ├── decisions/
│   │   ├── adr-001-layered-architecture.md
│   │   ├── adr-002-reference-data.md
│   │   ├── adr-003-aws-databricks.md
│   │   ├── adr-004-medallion-architecture.md
│   │   ├── adr-005-dbt-gold-layer.md
│   │   ├── adr-006-metadata-governance.md
│   │   └── adr-007-data-sharing.md
│   │
│   └── diagrams/
│       ├── repository-structure.mmd
│       ├── system-architecture.mmd
│       ├── data-flow.mmd
│       ├── database-architecture.mmd
│       ├── lakehouse-architecture.mmd
│       └── metadata-lineage.mmd
│
├── scripts/
│   ├── setup_local.py
│   └── set_spark_java.ps1
│
├── docker/
│   └── local/
│       ├── postgres/
│       └── airflow/
│
├── logs/
│
├── releases/
│   ├── CHANGELOG.md
│   └── roadmap.md
│
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── pyproject.toml
├── main.py
├── LICENSE
├── CONTRIBUTING.md
└── README.md
```

# Top-Level Modules

## `config/`

Provides centralised application configuration.

It contains:

* environment-variable loading;
* PostgreSQL connection settings;
* AWS configuration;
* Amazon S3 prefixes;
* Databricks configuration;
* Spark configuration;
* global constants;
* structured application logging.

Application modules consume central configuration rather than independently reading environment variables.

---

## `reference_data/`

Provides the platform's governed reference-data layer.

Reference datasets are maintained as YAML files and loaded into PostgreSQL through the database seeding process.

Current reference datasets include:

* business units;
* departments;
* locations;
* job roles;
* employment types;
* genders;
* attendance statuses;
* absence reasons;
* public holidays;
* leave types;
* training categories;
* employee exit reasons.

The simulator consumes governed reference values rather than maintaining independent hard-coded vocabularies.

---

## `database/`

Implements the PostgreSQL operational HR information system.

Responsibilities include:

* SQLAlchemy ORM models;
* database connection management;
* schema creation;
* reference-data persistence;
* database seeding;
* workforce master data;
* workforce lifecycle events.

PostgreSQL represents the operational source system and is deliberately separated from the analytical lakehouse.

---

## `simulator/`

Implements the synthetic HRIS workload.

The simulator generates realistic interconnected workforce data covering:

* employees;
* recruitment;
* attendance;
* payroll;
* leave;
* training;
* promotions;
* transfers;
* performance reviews;
* surveys;
* manager feedback;
* employee exits;
* exit interviews.

Business rules ensure that generated records respect employment periods, organisational hierarchy and workforce lifecycle dependencies.

---

## `quality/`

Contains the platform data-quality framework.

Current controls include:

* duplicate detection;
* referential-integrity validation;
* workforce lifecycle rules;
* employee hierarchy validation;
* salary validation;
* payroll validation;
* promotion reconciliation;
* transfer reconciliation;
* termination reconciliation;
* effective-date validation;
* reference-data validation;
* PostgreSQL-to-Raw extraction reconciliation.

Quality checks are designed as pipeline gates rather than passive reporting controls.

---

## `etl/`

Owns operational source extraction and Raw data ingestion.

The current pipeline is:

```text
PostgreSQL
     │
     ▼
etl/extract.py
     │
     ▼
Local Parquet
     │
     ▼
Raw Validation
     │
     ▼
etl/export_s3.py
     │
     ▼
Amazon S3 Raw
```

The ETL package does not own Bronze, Silver or Gold transformations.

---

## `spark/`

Contains the Databricks/PySpark lakehouse engineering layer.

PySpark owns:

```text
Raw
 ↓
Bronze
 ↓
Silver
```

### Bronze

Bronze preserves source fidelity while introducing technical lakehouse controls such as:

* schema enforcement;
* source-file tracking;
* ingestion timestamps;
* batch lineage;
* controlled schema evolution;
* structural quality validation.

### Silver

Silver produces trusted and reusable workforce entities through:

* deduplication;
* data-type standardisation;
* reference-data conformity;
* null handling;
* business-key enforcement;
* validated joins;
* reusable domain transformations.

Spark jobs are designed to execute on Databricks while remaining modular and testable.

---

## `dbt/`

Implements analytics engineering from Silver to Gold.

The transformation boundary is:

```text
Silver Delta
     │
     ▼
dbt-databricks
     │
     ▼
Gold
```

Gold contains:

* dimension tables;
* fact tables;
* analytical marts;
* reporting models;
* documented business metrics.

Example domains include:

* workforce;
* attendance;
* payroll and finance;
* recruitment;
* learning and development.

dbt also provides analytical tests, lineage and model documentation.

---

## `metadata/`

Defines cross-platform technical and business metadata.

The metadata framework supports:

* schemas;
* ownership;
* classifications;
* business glossary definitions;
* cross-platform lineage.

The intended lineage chain is:

```text
PostgreSQL
    ↓
S3 Raw
    ↓
Bronze
    ↓
Silver
    ↓
dbt Gold
    ↓
Power BI / ML / API
```

---

## `catalogue/`

Provides the enterprise metadata catalogue.

The catalogue complements Databricks Unity Catalog by providing cross-platform business governance.

It is designed to register:

* datasets;
* columns;
* business ownership;
* classifications;
* quality results;
* lineage;
* data products.

Metadata can be synchronised from PostgreSQL, Databricks and dbt.

---

## `data_sharing/`

Implements governance for internal and external data sharing.

Responsibilities include:

* approved API consumers;
* API keys;
* access policies;
* data-product registration;
* export-request governance;
* audit history.

This layer determines **who is authorised to receive a data product and under which policy**.

---

## `api/`

Provides the secure FastAPI consumption layer.

The API exposes curated data products rather than operational or low-level lakehouse tables.

Responsibilities include:

* HTTP routing;
* request validation;
* authentication;
* authorisation;
* API-key management;
* rate limiting;
* audit logging;
* response schemas;
* export services.

The intended flow is:

```text
Gold Data Product
       │
       ▼
data_sharing/
       │
       ▼
FastAPI
       │
       ▼
Approved Consumer
```

---

## `integrations/`

Provides a common framework for external data enrichment.

Potential providers include:

* public holidays;
* labour-market statistics;
* exchange rates;
* geospatial information.

External datasets enter the same governed ingestion architecture as internal datasets rather than bypassing Raw and lakehouse controls.

---

## `analytics/`

Contains advanced workforce analytics and machine-learning workloads.

Planned capabilities include:

* attrition prediction;
* burnout analysis;
* promotion prediction;
* workforce forecasting.

Analytical models consume governed Silver or Gold datasets rather than Raw operational data.

---

## `airflow/`

Provides production workflow orchestration.

The target workflow is:

```text
Generate Operational Data
        │
        ▼
Validate Operational Data
        │
        ▼
Extract PostgreSQL
        │
        ▼
Validate Raw
        │
        ▼
Upload Raw to S3
        │
        ▼
Process Bronze
        │
        ▼
Process Silver
        │
        ▼
Run dbt
        │
        ▼
Run Analytical Tests
        │
        ▼
Publish Metadata
```

Airflow coordinates jobs but does not contain transformation business logic.

---

## `dashboards/`

Contains Power BI artefacts including:

* reports;
* semantic models;
* screenshots and portfolio outputs.

Power BI consumes curated Gold datasets.

---

## `terraform/`

Provides Infrastructure as Code for the AWS and Databricks environment.

Target infrastructure includes:

* Amazon S3;
* IAM;
* networking;
* secrets;
* Databricks resources;
* database infrastructure;
* Airflow infrastructure;
* API infrastructure.

Environment-specific configuration is maintained for development, staging and production.

---

## `.github/`

Contains GitHub Actions CI/CD workflows.

The delivery pipeline supports:

* linting;
* automated testing;
* security scanning;
* dbt validation;
* Terraform validation;
* deployment automation.

---

## `docs/`

Contains the project's technical documentation.

Documentation is organised into:

### Architecture

* system architecture;
* database architecture;
* lakehouse architecture;
* repository structure;
* data flow;
* deployment architecture;
* technology stack.

### Data Governance

* data dictionary;
* data lineage;
* reference-data governance;
* data-quality framework;
* metadata framework;
* data catalogue;
* data-sharing security.

### Implementation

* local development;
* PostgreSQL;
* simulator;
* Raw ingestion;
* Databricks;
* Bronze;
* Silver;
* dbt;
* Airflow;
* API;
* deployment.

### Architecture Decisions

Architecture Decision Records document significant technology and design decisions throughout the project.

---

# Root Files

| File                 | Purpose                                                                    |
| -------------------- | -------------------------------------------------------------------------- |
| `.env`               | Local environment variables and credentials. Excluded from source control. |
| `.env.example`       | Safe template showing required configuration values.                       |
| `.gitignore`         | Defines files excluded from Git.                                           |
| `docker-compose.yml` | Defines local development infrastructure.                                  |
| `Makefile`           | Developer and CI task shortcuts.                                           |
| `requirements.txt`   | Python dependencies.                                                       |
| `pyproject.toml`     | Python project and development-tool configuration.                         |
| `main.py`            | Primary local platform CLI.                                                |
| `README.md`          | Project overview and implementation guide.                                 |
| `CONTRIBUTING.md`    | Contribution and development standards.                                    |
| `LICENSE`            | Project licence.                                                           |

---

# Local Platform CLI

The root `main.py` provides a consistent command interface for the currently implemented pipeline.

Examples include:

```bash
python main.py simulate
python main.py simulate --full-refresh

python main.py validate

python main.py extract

python main.py validate-raw

python main.py upload-s3

python main.py raw-pipeline

python main.py full-refresh
```

The complete implemented Raw workflow is:

```text
Simulation
    │
    ▼
Operational Validation
    │
    ▼
PostgreSQL Extraction
    │
    ▼
Raw Validation
    │
    ▼
Amazon S3 Upload
```

Additional commands are introduced as the lakehouse layers become operational.

---

# Makefile

The Makefile provides a concise developer and CI task interface over the project commands.

For example:

```bash
make simulate-full
make validate
make raw-pipeline
```

The Makefile delegates to the application's Python CLI and does not contain business logic.

---

# Local Data

The `data/` directory is used only for local development artefacts.

```text
data/
└── raw/
    └── postgres/
```

The authoritative Raw cloud dataset resides in Amazon S3.

Bronze, Silver and Gold are cloud lakehouse assets rather than local filesystem datasets.

---

# Lakehouse Layer Ownership

Technology ownership is deliberately explicit.

```text
PostgreSQL
    │
    │ Operational source
    ▼
Python ETL
    │
    │ Source ingestion
    ▼
Amazon S3 Raw
    │
    │ Immutable archive
    ▼
Databricks / PySpark
    │
    ├── Bronze
    │
    └── Silver
    │
    ▼
dbt-databricks
    │
    ▼
Gold
```

This avoids duplicated transformation responsibilities.

---

# Governance Architecture

The platform uses complementary governance capabilities.

## Unity Catalog

Unity Catalog provides technical governance of Databricks assets including:

* catalogs;
* schemas;
* Delta tables;
* views;
* permissions;
* lakehouse lineage.

## Enterprise Metadata Catalogue

The project catalogue provides broader governance including:

* business definitions;
* data ownership;
* classification;
* quality results;
* cross-platform lineage;
* data-product metadata;
* external sharing metadata.

Together they provide both platform-level and enterprise-level governance.

---

# Data Sharing Architecture

Third-party and internal data sharing is based on governed **data products**.

Consumers do not receive direct access to:

* PostgreSQL operational tables;
* Raw data;
* Bronze data;
* unrestricted Silver datasets.

The sharing path is:

```text
Silver
   │
   ▼
dbt
   │
   ▼
Gold
   │
   ▼
Approved Data Product
   │
   ▼
Access Policy
   │
   ▼
FastAPI
   │
   ▼
Approved Consumer
```

Security controls include:

* authentication;
* authorisation;
* API keys;
* access policies;
* rate limiting;
* audit logging;
* controlled exports.

---

# Repository Layer Model

The repository directly reflects the platform's architectural layers.

```text
REFERENCE DATA
──────────────

reference_data/
       │
       ▼

OPERATIONAL SYSTEM
──────────────────

database/
   │
   ▼
simulator/
   │
   ▼
quality/


RAW INGESTION
─────────────

etl/
 │
 ▼
Amazon S3 Raw


LAKEHOUSE ENGINEERING
─────────────────────

spark/bronze/
      │
      ▼
spark/silver/


ANALYTICS ENGINEERING
─────────────────────

dbt/
 │
 ▼
Gold


GOVERNANCE
──────────

Unity Catalog
      +
metadata/
      +
catalogue/


CONSUMPTION
───────────

dashboards/
analytics/
data_sharing/
api/


ORCHESTRATION & DELIVERY
────────────────────────

airflow/
terraform/
.github/
deployment/
```

---

# Design Principles

## Separation of Concerns

Each module owns a clearly defined responsibility.

Operational simulation, source ingestion, lakehouse transformation, analytical modelling, governance and delivery are independently implemented.

---

## Operational and Analytical Separation

PostgreSQL represents the operational HR system.

Databricks represents the analytical lakehouse.

```text
Operational
───────────

Python Simulator
      ↓
PostgreSQL


Analytical
──────────

S3 Raw
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

---

## Governed Reference Data

Controlled business vocabularies are maintained centrally.

```text
YAML
 ↓
Validation
 ↓
PostgreSQL
 ↓
Simulator
```

This creates a single authoritative source for reusable reference values.

---

## Data Quality as a Pipeline Gate

Quality validation occurs at multiple boundaries.

```text
Reference Data
      ↓
Reference Validation

PostgreSQL
      ↓
Operational Validation

Raw
      ↓
Source-to-Raw Reconciliation

Bronze
      ↓
Structural Validation

Silver
      ↓
Business Conformity

Gold
      ↓
dbt Tests
```

Invalid data should fail processing before reaching downstream consumers.

---

## Immutable Raw Data

Amazon S3 Raw provides a durable, source-aligned and replayable archive.

Raw data retains:

* source identity;
* extraction date;
* batch identity;
* source schema;
* technical extraction metadata.

All downstream lakehouse layers can be rebuilt from Raw.

---

## Medallion Architecture

The lakehouse progressively increases data quality and business value.

```text
Raw
 │
 ▼
Bronze
 │
 ▼
Silver
 │
 ▼
Gold
```

### Raw

Immutable source archive.

### Bronze

Technically governed source-conformed data.

### Silver

Cleaned, validated and reusable business entities.

### Gold

Business-facing facts, dimensions and marts.

---

## Clear Transformation Ownership

```text
etl/
    PostgreSQL → Raw

spark/
    Raw → Bronze → Silver

dbt/
    Silver → Gold
```

Each transformation technology has a distinct responsibility.

---

## Metadata and Lineage

Data is traceable through the full platform lifecycle.

```text
Reference Data
      │
      ▼
PostgreSQL
      │
      ▼
S3 Raw Batch
      │
      ▼
Bronze Delta
      │
      ▼
Silver Delta
      │
      ▼
dbt Gold
      │
      ▼
Power BI / ML / API
```

---

## Security by Design

Security is applied at every architectural boundary.

```text
PostgreSQL
    Database roles and credentials

Amazon S3
    AWS IAM

Databricks
    Unity Catalog

Gold
    Approved analytical datasets

data_sharing/
    Consumer access policies

FastAPI
    Authentication and authorisation
```

External consumers only access approved curated data products.

---

## Idempotency and Reproducibility

The platform supports controlled reruns through:

* idempotent reference-data seeding;
* simulator full refresh;
* shared extraction batch identity;
* partitioned Raw datasets;
* source-to-Raw reconciliation;
* batch-aware downstream processing.

Lakehouse processing is designed so rerunning a batch does not create duplicate analytical data.

---

## Testability

Each major architectural component has a dedicated testing boundary.

Tests cover:

* ORM models;
* reference data;
* simulator behaviour;
* lifecycle rules;
* quality checks;
* extraction;
* S3 ingestion;
* Spark transformations;
* dbt models;
* metadata;
* APIs;
* data-sharing governance.

---

## Infrastructure as Code

Cloud infrastructure is managed through Terraform rather than manual configuration.

This supports:

* reproducible environments;
* reviewable infrastructure changes;
* environment separation;
* disaster recovery;
* CI/CD integration.

---

## Orchestration Without Business Logic

Workflow orchestration is kept separate from processing logic.

```text
main.py
    Local workflow interface

Makefile
    Developer shortcuts

Airflow
    Production orchestration

Domain modules
    Business and transformation logic
```

---

# End-to-End Repository Flow

```text
reference_data/
        │
        ▼
database/
        │
        ▼
simulator/
        │
        ▼
quality/
        │
        ▼
etl/
        │
        ▼
Amazon S3 Raw
        │
        ▼
spark/bronze/
        │
        ▼
spark/silver/
        │
        ▼
dbt/
        │
        ▼
Gold Data Products
        │
        ├───────────────┬────────────────┐
        ▼               ▼                ▼
 dashboards/        analytics/      data_sharing/
                                         │
                                         ▼
                                        api/
                                         │
                                         ▼
                                  Approved Consumers
```

---

# Summary

The repository structure mirrors the architecture of the complete People Analytics Lakehouse Platform while keeping technology responsibilities clearly separated.

The core data lifecycle is:

```text
Governed Reference Data
        ↓
Synthetic HRIS
        ↓
PostgreSQL
        ↓
Quality Validation
        ↓
Raw Parquet
        ↓
Amazon S3
        ↓
Databricks Bronze
        ↓
Databricks Silver
        ↓
dbt Gold
        ↓
Governed Data Products
        ↓
Power BI / Analytics / FastAPI
```

This architecture provides a scalable foundation for workforce analytics while demonstrating modern practices across data engineering, lakehouse architecture, analytics engineering, governance, infrastructure automation, orchestration and secure data sharing.
