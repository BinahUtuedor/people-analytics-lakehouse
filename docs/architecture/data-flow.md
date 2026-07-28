# Data Flow

## Overview

The **People Analytics Lakehouse Platform** is an end-to-end data engineering and analytics platform for generating, validating, ingesting, transforming, governing and sharing workforce data.

The platform currently implements the operational and Raw ingestion layers and is being extended into an **AWS-hosted Databricks Lakehouse**.

The target architecture uses:

* PostgreSQL as the operational source system
* Python and SQLAlchemy for synthetic HR data generation
* YAML-backed governed reference data
* Python-based data-quality controls
* Apache Parquet for immutable Raw storage
* Amazon S3 as the cloud data-lake storage layer
* Databricks on AWS for distributed lakehouse processing
* Delta Lake for Bronze, Silver and Gold tables
* PySpark for Bronze and Silver engineering transformations
* dbt-databricks for analytical modelling and Gold data products
* Unity Catalog for lakehouse governance
* Power BI for business intelligence
* FastAPI for governed data sharing
* Airflow for workflow orchestration
* Terraform and GitHub Actions for infrastructure and CI/CD

---

# 1. Current Implementation Status

The following data flow is currently operational:

```text
YAML Reference Data
        │
        ▼
Reference Data Loader
        │
        ▼
Reference Data Validation
        │
        ▼
PostgreSQL Reference Tables
        │
        ▼
Synthetic HR Simulator
        │
        ▼
PostgreSQL Operational Database
        │
        ▼
Operational Data Quality Validation
        │
        ▼
Python Extraction
        │
        ▼
Local Raw Parquet
        │
        ▼
Raw Extraction Validation
        │
        ▼
Amazon S3 Raw Zone
```

The implemented Raw pipeline can be executed through the platform CLI:

```powershell
python main.py simulate --full-refresh
python main.py validate
python main.py extract
python main.py validate-raw
python main.py upload-s3
```

The complete currently implemented workflow can also be executed with:

```powershell
python main.py full-refresh
```

---

# 2. Target End-to-End Data Flow

The completed platform will extend the existing Raw pipeline into a governed Databricks Lakehouse.

```text
                    REFERENCE DATA
                    ──────────────

              reference_data/*.yml
                       │
                       ▼
                Reference Loader
                       │
                       ▼
              Reference Validation
                       │
                       ▼
              PostgreSQL Lookups

                       │
                       ▼

                OPERATIONAL HRIS
                ────────────────

                Python Simulator
                       │
                       ▼
             PostgreSQL Operational DB
                       │
                       ▼
            Operational Data Quality
                       │
                       ▼

                 RAW INGESTION
                 ─────────────

                Python Extraction
                       │
                       ▼
                 Raw Parquet
                       │
                       ▼
            Raw Extraction Validation
                       │
                       ▼
                  Amazon S3
                Raw / PostgreSQL
                       │
                       ▼

          ┌──────────────────────────────┐
          │      DATABRICKS ON AWS      │
          │                              │
          │       Raw Ingestion          │
          │             │                │
          │             ▼                │
          │          BRONZE              │
          │        Delta Tables          │
          │             │                │
          │             ▼                │
          │          SILVER              │
          │        Delta Tables          │
          │             │                │
          │             ▼                │
          │       dbt-databricks         │
          │             │                │
          │             ▼                │
          │           GOLD               │
          │   Facts / Dimensions / Marts │
          │                              │
          └──────────────┬───────────────┘
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
          Power BI    Analytics   Data Products
                         / ML          │
                                      ▼
                               Governed FastAPI
                                      │
                                      ▼
                             Approved Consumers
```

---

# 3. Reference Data Flow

Reference data is managed separately from simulated transactional data.

Reference datasets are stored as YAML files under:

```text
reference_data/
```

Current governed reference datasets include:

```text
business_units.yml
departments.yml
locations.yml
job_roles.yml
attendance_statuses.yml
genders.yml
leave_types.yml
employment_types.yml
exit_reasons.yml
training_categories.yml
public_holidays.yml
absence_reasons.yml
```

The processing flow is:

```text
YAML
 │
 ▼
reference_data/loader.py
 │
 ▼
quality/reference_data_checks.py
 │
 ▼
database/seed.py
 │
 ▼
PostgreSQL Reference Tables
 │
 ▼
Simulator Modules
```

The simulator no longer owns controlled reference lists for areas that have been migrated.

For example:

```text
employment_types.yml
        ↓
EmploymentType table
        ↓
employees.py
        ↓
Employee.employment_type
```

The same pattern is used for gender, attendance status, absence reason, public holidays, leave types, training categories and employee exit reasons.

The operational tables continue storing their existing descriptive values so downstream schemas remain stable.

---

# 4. Operational Data Generation

The simulator represents a synthetic enterprise HR information system.

It generates interconnected datasets covering the workforce lifecycle.

Current simulated domains include:

* Employees
* Recruitment
* Attendance
* Payroll
* Leave Requests
* Training
* Promotions
* Transfers
* Performance Reviews
* Employee Surveys
* Manager Feedback
* Employee Exits
* Exit Interviews

Generation order is controlled so lifecycle dependencies remain valid.

A simplified sequence is:

```text
Reference Data
      │
      ▼
Employees
      │
      ▼
Recruitment
      │
      ▼
Updated Employee Population
      │
      ▼
Employee Exits
      │
      ├───────────────┐
      ▼               ▼
Attendance         Payroll
Leave              Training
Performance        Promotions
Transfers          Surveys
Manager Feedback
      │
      ▼
Exit Interviews
```

Employee exits are generated before employment-window-dependent facts so attendance, payroll, leave, training and similar records cannot be generated outside an employee's employment period.

---

# 5. PostgreSQL Operational Database

PostgreSQL acts as the source operational database.

It represents the transactional HR system rather than the analytical platform.

The database contains:

### Master and reference entities

* Business Units
* Departments
* Locations
* Job Roles
* Employment Types
* Genders
* Attendance Statuses
* Absence Reasons
* Leave Types
* Training Categories
* Exit Reasons
* Public Holidays

### Operational entities

* Employees
* Attendance
* Payroll
* Leave Requests
* Recruitment
* Promotions
* Transfers
* Training
* Performance Reviews
* Employee Surveys
* Manager Feedback
* Employee Exits
* Exit Interviews

The analytical platform does not query PostgreSQL directly for reporting workloads.

---

# 6. Operational Data Quality

Before operational data is extracted, it passes through the project's quality framework.

Implemented validation includes:

* duplicate detection;
* referential integrity;
* employee hierarchy checks;
* salary validation;
* payroll validation;
* employment-date validation;
* recruitment lifecycle reconciliation;
* promotion-state reconciliation;
* transfer-state reconciliation;
* employee exit lifecycle consistency;
* effective-date validation across operational facts.

The quality gate follows this pattern:

```text
PostgreSQL
     │
     ▼
Quality Checks
     │
     ├── PASS → extraction permitted
     │
     └── FAIL → pipeline stopped
```

Critical failures raise a data-quality exception rather than allowing invalid data to silently continue downstream.

---

# 7. Raw Ingestion Layer

The Raw layer is already implemented.

Operational PostgreSQL tables are extracted using:

```text
etl/extract.py
```

Each extraction is written as Parquet and carries technical extraction metadata.

The local structure follows a partitioned pattern similar to:

```text
data/raw/postgres/
└── employees/
    └── extraction_date=YYYY-MM-DD/
        └── extraction_id=<id>/
            └── part-00000.parquet
```

Raw data is then uploaded through:

```text
etl/export_s3.py
```

to:

```text
s3://<bucket>/raw/postgresql/
```

---

# 8. Raw Data Principles

Raw represents the immutable ingestion and replay layer.

Raw data should remain:

* source aligned;
* immutable;
* batch traceable;
* minimally transformed;
* replayable;
* stored as Parquet;
* partitioned by extraction metadata.

Raw is not intended for direct business reporting.

It provides the recovery point from which downstream lakehouse layers can be rebuilt.

---

# 9. Raw Extraction Validation

Before Raw data is treated as successfully ingested, the platform reconciles PostgreSQL and extracted Parquet.

The implemented validation checks source and Raw record counts across all configured source tables.

The flow is:

```text
PostgreSQL
     │
     ├─────────────┐
     │             │
     ▼             ▼
Source Count   Raw Parquet Count
     │             │
     └──────┬──────┘
            ▼
        Reconcile
            │
       ┌────┴────┐
       ▼         ▼
     PASS       FAIL
```

Only validated Raw batches should continue to downstream lakehouse processing.

---

# 10. Amazon S3

Amazon S3 is the persistent cloud storage layer.

The planned logical layout is:

```text
s3://<people-analytics-bucket>/

├── raw/
│   └── postgresql/
│
├── bronze/
│
├── silver/
│
└── gold/
```

The physical implementation may evolve as Databricks and Unity Catalog are introduced.

The Raw S3 area will remain the immutable archive.

Bronze and Silver will be managed through Databricks using Delta Lake.

Gold will contain curated analytical models produced primarily through dbt.

---

# 11. Databricks Lakehouse

Databricks on AWS is the target lakehouse processing platform.

Databricks will provide:

* Apache Spark processing;
* Delta Lake tables;
* scalable Bronze and Silver transformations;
* SQL workloads;
* Unity Catalog governance;
* lineage;
* machine-learning integration;
* managed job execution.

The platform will access Raw Parquet stored in Amazon S3 and transform it into governed Delta tables.

---

# 12. Bronze Layer

## Status

**Foundation started; not yet production-complete.**

Bronze will be the first managed lakehouse layer.

The intended flow is:

```text
S3 Raw Parquet
      │
      ▼
Databricks / PySpark
      │
      ▼
Bronze Delta Tables
```

Bronze responsibilities will include:

* preserving source business values;
* schema enforcement;
* ingestion metadata;
* source-file tracking;
* extraction/batch lineage;
* controlled schema evolution;
* basic structural validation.

Typical technical columns may include:

```text
_bronze_ingested_at
_source_system
_source_table
_source_file
_batch_id
_extraction_date
```

Bronze should not contain significant business transformations.

---

# 13. Silver Layer

## Status

**Planned.**

Silver will contain cleansed and standardised workforce data.

The intended responsibilities include:

* type standardisation;
* deduplication;
* null handling;
* code/value standardisation;
* validated joins;
* reference-data conformity;
* business-key enforcement;
* date standardisation;
* reusable analytical entities.

The flow will be:

```text
Bronze Delta
      │
      ▼
PySpark Transformations
      │
      ▼
Silver Delta
```

Silver will become the primary trusted input for analytical modelling.

---

# 14. Gold and dbt

## Status

**Planned.**

dbt will transform trusted Silver data into business-facing analytical models.

The intended architecture is:

```text
Silver Delta
      │
      ▼
dbt-databricks
      │
      ▼
Gold
```

Gold will contain:

### Dimensions

Examples:

```text
dim_employee
dim_department
dim_job_role
dim_location
dim_date
```

### Facts

Examples:

```text
fct_attendance
fct_payroll
fct_leave
fct_recruitment
fct_training
fct_promotions
```

### Business marts

Examples:

```text
workforce
recruitment
attendance
finance
learning
employee_performance
```

dbt will own:

* analytical business logic;
* dimensional modelling;
* model dependencies;
* analytical tests;
* model documentation;
* reusable business metrics.

This creates a clear separation:

```text
PySpark
Raw → Bronze → Silver

dbt
Silver → Gold
```

---

# 15. Unity Catalog

## Status

**Planned as part of Databricks implementation.**

Unity Catalog will provide technical governance for lakehouse assets.

It will govern:

* catalogs;
* schemas;
* Delta tables;
* views;
* permissions;
* technical lineage;
* access to S3-backed data.

A target namespace could follow:

```text
people_analytics
│
├── bronze
├── silver
└── gold
```

Environment separation may later use separate catalogs or storage locations for:

```text
development
staging
production
```

The final design will be established when the Databricks AWS environment is provisioned.

---

# 16. Metadata and Enterprise Catalogue

## Status

**Planned.**

The custom metadata and catalogue components remain separate from Unity Catalog.

Unity Catalog will govern technical Databricks assets.

The project catalogue will provide broader enterprise metadata such as:

* business definitions;
* ownership;
* classifications;
* quality results;
* cross-platform lineage;
* business glossary;
* data products;
* external sharing policies.

The intended relationship is:

```text
Unity Catalog
     │
     ├── technical governance
     │
     ▼
Databricks Assets

Custom Catalogue
     │
     ├── business metadata
     ├── ownership
     ├── quality
     ├── classification
     └── cross-platform lineage
```

The catalogue should be implemented after the core Bronze/Silver/Gold assets exist so metadata describes real platform objects rather than hypothetical structures.

---

# 17. Third-Party Enrichment

## Status

**Planned.**

The repository contains an `integrations/` architecture for external data enrichment, but external API ingestion should not yet be represented as operational functionality.

Potential sources include:

* public holiday APIs;
* labour-market statistics;
* exchange rates;
* geospatial information;
* learning-provider metadata;
* weather data.

Future flow:

```text
External API
     │
     ▼
integrations/
     │
     ▼
Raw External Data
     │
     ▼
Bronze
     │
     ▼
Silver
```

External data should enter the same governed ingestion model as internally generated data rather than bypassing Raw and Bronze controls.

---

# 18. Analytics and Machine Learning

## Status

**Planned.**

Trusted Silver and Gold data can support analytical and machine-learning workloads such as:

* attrition prediction;
* workforce forecasting;
* burnout analysis;
* promotion analysis;
* sentiment analysis;
* workforce segmentation.

Machine-learning features should be created only from governed Silver or Gold datasets.

Raw data should not normally be consumed directly by ML models.

---

# 19. Power BI

## Status

**Planned.**

Power BI will consume curated Gold models rather than querying PostgreSQL, Raw or Bronze datasets.

The preferred path is:

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
Power BI Semantic Model
   │
   ▼
Dashboards
```

This keeps BI logic separated from ingestion and operational systems.

---

# 20. Governed Data Sharing

## Status

**Architecture defined; implementation planned.**

The platform is designed to support governed data sharing with approved third parties and internal consumers through FastAPI.

External consumers must never receive direct access to:

* PostgreSQL operational tables;
* Raw datasets;
* Bronze tables;
* unrestricted Silver datasets.

Approved data products should instead be produced from Gold or specifically governed views.

```text
Gold / Curated Views
       │
       ▼
Data Product Definition
       │
       ▼
Access Policy
       │
       ▼
FastAPI
       │
       ├── Authentication
       ├── Authorisation
       ├── API Keys
       ├── Rate Limiting
       ├── Audit Logging
       └── Request Tracking
       │
       ▼
Approved Consumer
```

Potential consumers may include:

* internal business functions;
* research partners;
* approved vendors;
* universities;
* government or public-sector bodies.

Access will depend on explicit policy rather than consumer category alone.

---

# 21. Data Sharing Governance

The planned `data_sharing/` layer will manage:

```text
API Consumers
API Keys
Access Policies
Data Products
Access Logs
Export Requests
```

This separates **data-delivery mechanics** from **access governance**.

The API answers:

> How is the data delivered?

The data-sharing layer answers:

> Who is allowed to receive which data product and under what conditions?

---

# 22. Orchestration

## Current State

The current pipeline can be orchestrated locally through `main.py`.

Implemented commands include:

```text
simulate
validate
extract
validate-raw
upload-s3
raw-pipeline
full-refresh
```

## Target State

Airflow will eventually coordinate production workflows.

The target DAG sequence is:

```text
Seed / Validate Reference Data
        │
        ▼
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
Trigger Bronze Processing
        │
        ▼
Validate Bronze
        │
        ▼
Run Silver Processing
        │
        ▼
Validate Silver
        │
        ▼
Run dbt
        │
        ▼
Run dbt Tests
        │
        ▼
Publish Metadata
        │
        ▼
Refresh Downstream Products
```

Airflow will orchestrate workloads rather than contain transformation business logic.

---

# 23. Data Quality Across the Lakehouse

Quality controls will operate at multiple boundaries.

```text
Operational DB
     │
     ├── Operational quality
     ▼
Raw
     │
     ├── Source-to-Raw reconciliation
     ▼
Bronze
     │
     ├── Structural / schema quality
     ▼
Silver
     │
     ├── Business conformity
     ▼
Gold
     │
     ├── dbt tests / analytical quality
     ▼
Data Products
```

This provides defence in depth rather than relying on a single validation stage.

---

# 24. Data Lineage

The target lineage chain is:

```text
Reference Data
      │
      ▼
PostgreSQL
      │
      ▼
Raw S3 Object
      │
      ▼
Bronze Delta Table
      │
      ▼
Silver Delta Table
      │
      ▼
dbt Gold Model
      │
      ▼
Power BI / ML / API Data Product
```

Batch and source metadata should allow a downstream record or dataset to be traced back to its source extraction.

---

# 25. Security Boundaries

Security will be applied by platform layer.

### Operational

PostgreSQL roles and credentials protect the source database.

### Raw Storage

AWS IAM controls access to S3 Raw objects.

### Lakehouse

Databricks and Unity Catalog control table and schema permissions.

### Analytical Consumption

Gold datasets expose approved business views.

### External Sharing

FastAPI and the data-sharing service enforce:

* authentication;
* authorisation;
* data-product policy;
* rate limiting;
* logging;
* auditability.

Secrets must not be stored in source code or committed configuration files.

---

# 26. Current vs Target Capability

| Capability                         | Status       |
| ---------------------------------- | ------------ |
| Synthetic HR simulator             | Implemented  |
| PostgreSQL operational database    | Implemented  |
| YAML reference-data framework      | Implemented  |
| Reference-data validation          | Implemented  |
| Governed simulator reference usage | Implemented  |
| Operational data-quality framework | Implemented  |
| PostgreSQL → Parquet extraction    | Implemented  |
| Shared extraction/batch identity   | Implemented  |
| Raw extraction reconciliation      | Implemented  |
| Amazon S3 Raw upload               | Implemented  |
| Root platform CLI                  | Implemented  |
| Bronze Spark foundation            | In progress  |
| Databricks on AWS                  | Planned next |
| Delta Lake Bronze                  | Planned      |
| Delta Lake Silver                  | Planned      |
| dbt-databricks Gold                | Planned      |
| Unity Catalog                      | Planned      |
| Metadata catalogue                 | Planned      |
| Airflow production orchestration   | Planned      |
| Third-party enrichment APIs        | Planned      |
| Power BI                           | Planned      |
| Machine learning                   | Planned      |
| Governed FastAPI data sharing      | Planned      |
| Terraform cloud provisioning       | Planned      |
| CI/CD deployment workflows         | Planned      |

---

# 27. Architecture Principles

The platform follows the following principles.

### Separation of operational and analytical workloads

PostgreSQL represents the HR operational system. Analytical workloads execute against the lakehouse.

### Immutable Raw data

S3 Raw provides a replayable source-aligned archive.

### Governed transformation

Bronze, Silver and Gold progressively increase structure, quality and business meaning.

### Clear technology ownership

```text
Python
    Operational simulation and ingestion

Amazon S3
    Durable Raw storage

Databricks / PySpark
    Bronze and Silver engineering

Delta Lake
    Lakehouse table format

dbt
    Gold analytical modelling

Unity Catalog
    Lakehouse technical governance

Custom Metadata Catalogue
    Enterprise business governance

Power BI
    Business intelligence

FastAPI
    Governed data-product delivery

Airflow
    Workflow orchestration
```

### No direct Raw consumption

Business users, APIs and machine-learning consumers should use curated data products rather than Raw or operational datasets.

### Traceability

Every downstream dataset should ultimately be traceable to its originating source and processing batch.

---

# 28. Final Target Flow

```text
                     REFERENCE DATA
                           │
                           ▼
                    Synthetic HRIS
                           │
                           ▼
                      PostgreSQL
                           │
                           ▼
                 Operational Quality
                           │
                           ▼
                 Python Extraction
                           │
                           ▼
                    Raw Parquet
                           │
                           ▼
                  Raw Reconciliation
                           │
                           ▼
                      AWS S3
                        RAW
                           │
                           ▼
                Databricks / PySpark
                           │
                           ▼
                       BRONZE
                    Delta Tables
                           │
                           ▼
                       SILVER
                    Delta Tables
                           │
                           ▼
                  dbt-databricks
                           │
                           ▼
                        GOLD
             Facts / Dimensions / Marts
                           │
           ┌───────────────┼────────────────┐
           ▼               ▼                ▼
       Power BI      ML / Analytics    Data Products
                                             │
                                             ▼
                                      Data-Sharing Policy
                                             │
                                             ▼
                                          FastAPI
                                             │
                                             ▼
                                    Approved Consumers
```

---

## Summary

The People Analytics Lakehouse Platform currently has a working operational-to-Raw pipeline and governed reference-data framework.

The next implementation phase will extend this foundation into an **AWS-hosted Databricks Lakehouse**, with:

```text
S3 Raw
   ↓
Databricks Bronze
   ↓
Databricks Silver
   ↓
dbt Gold
   ↓
Governed Data Products
```

This architecture preserves the working components already implemented while providing a clear path toward scalable transformation, governance, analytics and controlled third-party data sharing.
