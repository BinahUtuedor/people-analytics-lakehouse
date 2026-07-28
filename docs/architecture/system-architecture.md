# System Architecture

## Overview

The **People Analytics Lakehouse Platform** is an end-to-end enterprise data platform designed to simulate, validate, ingest, transform, govern, analyse and securely share workforce data.

The architecture separates the operational HR source system from analytical processing and consumption workloads.

The platform is built around:

* Python and SQLAlchemy for synthetic HR data generation
* PostgreSQL as the operational HR source system
* Amazon S3 as the immutable Raw data lake
* Databricks on AWS for scalable lakehouse processing
* Delta Lake for Bronze and Silver managed tables
* PySpark for Raw-to-Bronze and Bronze-to-Silver engineering
* dbt-databricks for Silver-to-Gold analytical modelling
* Unity Catalog for lakehouse technical governance
* Airflow for production workflow orchestration
* Power BI for business intelligence
* FastAPI for governed data-product delivery
* Terraform for infrastructure provisioning
* GitHub Actions for CI/CD

---

# End-to-End System Architecture

```text
                         GOVERNED REFERENCE DATA
                                  │
                                  ▼
                         reference_data/*.yml
                                  │
                                  ▼
                        Reference Data Loader
                                  │
                                  ▼
                      Reference Data Validation
                                  │
                                  ▼
                       PostgreSQL Lookup Tables
                                  │
                                  ▼

─────────────────────────────────────────────────────────────

                         OPERATIONAL HRIS
                                  │
                                  ▼
                         Python Simulator
                                  │
                                  ▼
                    PostgreSQL Operational DB
                        people_analytics
                          public schema
                                  │
                                  ▼
                   Operational Data Quality
                                  │
                                  ▼

─────────────────────────────────────────────────────────────

                           RAW INGESTION
                                  │
                                  ▼
                     Python Extraction Layer
                                  │
                                  ▼
                       Local Raw Parquet
                                  │
                                  ▼
                  Source-to-Raw Reconciliation
                                  │
                                  ▼
                         Amazon S3 Raw
                       Immutable Parquet
                                  │
                                  ▼

─────────────────────────────────────────────────────────────

                       DATABRICKS ON AWS
                                  │
                                  ▼
                           Bronze Layer
                         Delta Lake Tables
                                  │
                                  ▼
                           Silver Layer
                         Delta Lake Tables
                                  │
                                  ▼
                         dbt-databricks
                                  │
                                  ▼
                            Gold Layer
                  Facts / Dimensions / Marts
                                  │
                   ┌──────────────┼──────────────┐
                   ▼              ▼              ▼
               Power BI      Analytics / ML   Data Products
                                                  │
                                                  ▼
                                           Access Governance
                                                  │
                                                  ▼
                                               FastAPI
                                                  │
                                                  ▼
                                         Approved Consumers

─────────────────────────────────────────────────────────────

                         GOVERNANCE PLANE

                   Unity Catalog
                        +
                   Metadata Framework
                        +
                 Enterprise Catalogue
                        +
                    Data Quality
                        +
                 Lineage & Ownership
```

---

# Architecture Layers

## 1. Reference Data Layer

The platform uses centrally governed reference data to provide consistent business values across the operational simulator and downstream processing.

Reference datasets include:

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

The processing flow is:

```text
YAML
  │
  ▼
Reference Loader
  │
  ▼
Reference Validation
  │
  ▼
PostgreSQL Reference Tables
  │
  ▼
Simulator
```

This ensures controlled business values are maintained independently from simulation logic.

---

## 2. Operational Layer

The operational layer simulates an enterprise Human Resources Information System.

The simulator produces interconnected workforce data covering the employee lifecycle.

Current operational domains include:

* employees;
* recruitment;
* attendance;
* payroll;
* leave;
* training;
* promotions;
* transfers;
* performance reviews;
* employee surveys;
* manager feedback;
* employee exits;
* exit interviews.

PostgreSQL stores the current workforce state together with historical workforce events.

```text
Simulator
    │
    ▼
PostgreSQL
    │
    ├── Reference Data
    ├── Employee Master
    ├── Workforce Events
    └── Operational Facts
```

PostgreSQL is treated as the operational source system rather than the analytical warehouse.

---

## 3. Data Quality Layer

Data quality operates as a set of pipeline gates rather than a reporting-only capability.

Operational validation currently includes:

* duplicate detection;
* referential integrity;
* employee hierarchy checks;
* salary validation;
* payroll validation;
* recruitment lifecycle reconciliation;
* promotion-state reconciliation;
* transfer-state reconciliation;
* employee-exit reconciliation;
* employment-window validation;
* reference-data validation.

The flow is:

```text
PostgreSQL
     │
     ▼
Quality Validation
     │
     ├── PASS → extraction continues
     │
     └── FAIL → pipeline stops
```

Quality controls are extended at later lakehouse boundaries.

---

## 4. Raw Ingestion Layer

The Raw ingestion layer moves validated operational data from PostgreSQL into Amazon S3.

The implemented flow is:

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
quality/raw_extraction_validation.py
     │
     ▼
etl/export_s3.py
     │
     ▼
Amazon S3 Raw
```

Raw datasets carry extraction and batch metadata to support lineage and reproducibility.

The Raw layer is:

* source aligned;
* immutable;
* replayable;
* minimally transformed;
* batch traceable.

---

## 5. Amazon S3 Data Lake

Amazon S3 provides durable cloud storage for the platform.

The Raw zone acts as the persistent source archive.

A logical layout is:

```text
s3://people-analytics-lakehouse/

└── raw/
    └── postgresql/
        ├── employees/
        ├── attendance/
        ├── payroll/
        ├── leave_requests/
        ├── recruitment/
        └── ...
```

Each source table is organised by extraction metadata and batch identity.

The Raw zone provides the recovery point from which downstream lakehouse tables can be rebuilt.

---

## 6. Databricks Lakehouse Layer

Databricks on AWS provides the scalable processing environment for the analytical platform.

Databricks is responsible for:

* Apache Spark processing;
* Delta Lake table management;
* Bronze processing;
* Silver processing;
* managed SQL workloads;
* lakehouse technical governance;
* lineage integration;
* machine-learning integration.

The transformation boundary is:

```text
Amazon S3 Raw
      │
      ▼
Databricks
      │
      ▼
Bronze
      │
      ▼
Silver
```

---

# 7. Bronze Layer

The Bronze layer is the first governed lakehouse representation of the Raw data.

Bronze preserves source business values while introducing technical controls.

Typical Bronze responsibilities include:

* source schema preservation;
* schema enforcement;
* source-file tracking;
* ingestion timestamps;
* batch identity;
* extraction metadata;
* structural validation;
* controlled schema evolution.

The flow is:

```text
Raw Parquet
     │
     ▼
PySpark
     │
     ▼
Bronze Delta
```

Typical lineage columns include:

```text
_source_system
_source_table
_source_file
_extraction_date
_batch_id
_bronze_ingested_at
```

Bronze does not perform significant business transformation.

---

# 8. Silver Layer

The Silver layer provides trusted, standardised and reusable business entities.

Silver processing is implemented through PySpark and Databricks.

Typical responsibilities include:

* data-type standardisation;
* deduplication;
* null handling;
* controlled-value conformity;
* validated joins;
* business-key validation;
* date standardisation;
* reusable workforce entities;
* conformed event structures.

The flow is:

```text
Bronze Delta
      │
      ▼
PySpark
      │
      ▼
Silver Delta
```

Silver becomes the primary trusted source for downstream analytical modelling.

---

# 9. Analytics Engineering Layer

dbt-databricks owns the transformation from Silver to Gold.

```text
Silver
   │
   ▼
dbt-databricks
   │
   ▼
Gold
```

Gold contains analytical structures such as:

### Dimensions

* employee;
* department;
* job role;
* location;
* date.

### Facts

* attendance;
* payroll;
* leave;
* recruitment;
* promotions;
* training.

### Business Marts

* workforce;
* attendance;
* finance;
* recruitment;
* learning and development.

dbt provides:

* modular SQL transformations;
* dependency management;
* analytical tests;
* documentation;
* lineage;
* reusable business logic.

---

# 10. Gold Layer

Gold represents business-ready data products.

Gold datasets are designed around business consumption rather than source-system structure.

Typical consumers include:

* Power BI;
* analytical notebooks;
* machine-learning models;
* internal data products;
* governed APIs.

```text
Silver
   │
   ▼
dbt
   │
   ▼
Gold
   │
   ├── Dimensions
   ├── Facts
   └── Marts
```

Gold is the default analytical consumption layer.

---

# 11. Governance Layer

Governance operates across the platform rather than existing as a single downstream step.

The governance architecture combines:

* Unity Catalog;
* metadata definitions;
* enterprise catalogue;
* data quality;
* ownership;
* classification;
* lineage;
* data-product governance.

---

## Unity Catalog

Unity Catalog provides technical governance for Databricks assets.

Responsibilities include:

* catalogs;
* schemas;
* tables;
* views;
* permissions;
* technical lineage.

A logical namespace may follow:

```text
people_analytics
│
├── bronze
├── silver
└── gold
```

---

## Enterprise Metadata Catalogue

The custom metadata catalogue provides cross-platform business governance.

It covers:

* business definitions;
* ownership;
* classification;
* quality metrics;
* cross-platform lineage;
* data products;
* external access metadata.

This complements Unity Catalog rather than duplicating it.

---

# 12. Metadata and Lineage

The platform maintains traceability from source to consumption.

The target lineage chain is:

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
      ├──────────────┬──────────────┐
      ▼              ▼              ▼
   Power BI       Analytics       FastAPI
```

Batch identity and source-file metadata support operational traceability.

---

# 13. Consumption Layer

The consumption layer exposes trusted analytical outputs to internal and approved external consumers.

Internal consumption includes:

* HR reporting;
* Finance reporting;
* leadership dashboards;
* workforce planning;
* learning analytics;
* recruitment analysis;
* machine learning.

External or controlled consumption is provided through governed data products.

---

# 14. Power BI

Power BI consumes Gold datasets and semantic models.

The flow is:

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

Power BI does not query PostgreSQL or Raw data for analytical reporting.

---

# 15. Analytics and Machine Learning

The analytics layer supports workforce modelling such as:

* attrition prediction;
* burnout analysis;
* promotion modelling;
* workforce forecasting.

ML workloads consume governed Silver or Gold datasets.

```text
Silver / Gold
      │
      ▼
Feature Engineering
      │
      ▼
ML / Analytics
```

This prevents model development from depending directly on unstable Raw data.

---

# 16. Governed Data Sharing

The platform supports controlled data sharing with internal and third-party consumers.

Consumers never receive direct access to:

* PostgreSQL operational tables;
* Raw S3 objects;
* Bronze tables;
* unrestricted Silver data.

Approved products are derived from Gold or purpose-built curated views.

```text
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

The `data_sharing/` layer governs who can access which product, while the API provides the delivery mechanism.

---

# 17. External Integrations

External data can be introduced through a governed integration framework.

Potential enrichment datasets include:

* public holidays;
* labour-market statistics;
* exchange rates;
* geographical data.

External datasets follow the same data-engineering principles as operational data.

```text
External Source
      │
      ▼
Integration Client
      │
      ▼
Raw
      │
      ▼
Bronze
      │
      ▼
Silver
```

External data does not bypass lakehouse governance.

---

# 18. Workflow Orchestration

The platform separates transformation logic from workflow orchestration.

Current local orchestration is exposed through:

```text
main.py
```

Production orchestration is designed around Airflow.

The target production sequence is:

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
Validate Bronze
        │
        ▼
Process Silver
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
Refresh Consumers
```

Airflow coordinates jobs without embedding transformation business logic.

---

# 19. Infrastructure Architecture

Cloud infrastructure is provisioned through Terraform.

Target infrastructure includes:

* Amazon S3;
* IAM;
* networking;
* secrets management;
* Databricks resources;
* database infrastructure;
* Airflow;
* API infrastructure.

Terraform supports:

* repeatable environment provisioning;
* environment separation;
* controlled infrastructure changes;
* CI/CD integration.

---

# 20. CI/CD

GitHub Actions provides continuous integration and delivery.

The CI/CD architecture covers:

* linting;
* automated tests;
* security scanning;
* dbt validation;
* Terraform validation;
* controlled deployment.

Changes are validated before being promoted to target environments.

---

# 21. Security Architecture

Security is applied at multiple boundaries.

```text
PostgreSQL
    │
    └── Database roles / credentials

Amazon S3
    │
    └── AWS IAM

Databricks
    │
    └── Unity Catalog

Gold
    │
    └── Approved analytical datasets

data_sharing/
    │
    └── Consumer access policies

FastAPI
    │
    ├── Authentication
    ├── Authorisation
    ├── API keys
    ├── Rate limiting
    └── Audit logging
```

Secrets are managed outside source code and are not committed to version control.

---

# 22. Quality Architecture

Quality controls are implemented across the complete lifecycle.

```text
Reference Data
      │
      ▼
Reference Validation
      │
      ▼
PostgreSQL
      │
      ▼
Operational Validation
      │
      ▼
Raw
      │
      ▼
Source-to-Raw Reconciliation
      │
      ▼
Bronze
      │
      ▼
Structural Validation
      │
      ▼
Silver
      │
      ▼
Business Conformity
      │
      ▼
Gold
      │
      ▼
dbt Tests
```

This creates defence in depth rather than relying on a single quality stage.

---

# Architecture Principles

## Separation of Operational and Analytical Workloads

The operational HR source system and analytical lakehouse are separate.

```text
Operational

Simulator
   ↓
PostgreSQL


Analytical

S3 Raw
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

---

## Immutable Raw Layer

Raw data is preserved as the replay and audit layer.

It is minimally transformed and carries technical extraction metadata.

---

## Medallion Architecture

Data becomes progressively more governed and analytically useful:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

---

## Clear Technology Ownership

```text
Python / ETL
    PostgreSQL → Raw

PySpark / Databricks
    Raw → Bronze → Silver

dbt
    Silver → Gold
```

Each processing technology owns a clearly defined transformation boundary.

---

## Governance by Design

Governance is integrated throughout the platform rather than added after analytical development.

Technical governance, business metadata, ownership, classification, quality and data-sharing policies are managed as first-class capabilities.

---

## Security by Design

Consumers access only the level of data required for their use case.

Operational and low-level lakehouse data remain protected from direct external consumption.

---

## Idempotency and Reproducibility

The platform supports repeatable execution through:

* idempotent reference-data seeding;
* controlled simulator full refresh;
* extraction batch identity;
* partitioned Raw datasets;
* source-to-Raw reconciliation;
* batch-aware lakehouse processing.

---

## Traceability

Every downstream data product can be traced back through the transformation chain to its originating source and extraction batch.

---

# Current Platform Flow

The implemented platform currently supports:

```text
Reference YAML
      │
      ▼
Reference Validation
      │
      ▼
PostgreSQL Reference Tables
      │
      ▼
Synthetic HR Simulator
      │
      ▼
PostgreSQL Operational DB
      │
      ▼
Operational Quality
      │
      ▼
Parquet Extraction
      │
      ▼
Raw Reconciliation
      │
      ▼
Amazon S3 Raw
```

The lakehouse processing layer extends this foundation through Databricks, Delta Lake and dbt.

---

# Target Platform Flow

```text
Governed Reference Data
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
Raw Extraction
        │
        ▼
Amazon S3 Raw
        │
        ▼
Databricks Bronze
        │
        ▼
Databricks Silver
        │
        ▼
dbt Gold
        │
        ├────────────────┬─────────────────┐
        ▼                ▼                 ▼
    Power BI       Analytics / ML     Data Products
                                            │
                                            ▼
                                    Sharing Governance
                                            │
                                            ▼
                                         FastAPI
                                            │
                                            ▼
                                    Approved Consumers
```

---

# Summary

The People Analytics Lakehouse Platform separates the complete data lifecycle into distinct operational, ingestion, lakehouse, analytics, governance and consumption layers.

The core architecture is:

```text
PostgreSQL
    ↓
Amazon S3 Raw
    ↓
Databricks Bronze
    ↓
Databricks Silver
    ↓
dbt Gold
    ↓
Governed Consumption
```

This architecture provides a scalable foundation for enterprise workforce analytics while demonstrating modern practices in data engineering, lakehouse architecture, analytics engineering, data governance, orchestration, infrastructure automation and secure data sharing.
