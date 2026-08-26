# System Architecture

## Overview

The **People Analytics Lakehouse Platform** is an end-to-end enterprise
data platform designed to simulate, validate, ingest, transform, govern,
analyse and securely share synthetic workforce data.

The architecture separates the operational HR source system from
analytical processing and consumption workloads.

The platform is built around:

-   Python and SQLAlchemy for synthetic HR data generation;
-   PostgreSQL as the operational HR source system;
-   governed YAML reference data;
-   Python-based data-quality controls;
-   Parquet for portable analytical storage;
-   Amazon S3 as the cloud data lake;
-   Apache Spark, PySpark and Spark SQL for distributed processing;
-   Amazon EMR as the managed Spark execution environment;
-   Amazon S3 events and AWS Lambda for event-driven processing;
-   dbt for selected Gold analytical models and tests;
-   Power BI for business intelligence;
-   FastAPI for governed data-product delivery;
-   Terraform for AWS infrastructure provisioning;
-   GitHub Actions for CI/CD.

------------------------------------------------------------------------

# End-to-End System Architecture

``` text
                    GOVERNED REFERENCE DATA
                              │
                              ▼
                    reference_data/*.yml
                              │
                              ▼
                 Loader → Validation → Seed
                              │
                              ▼
                    PostgreSQL Lookups
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
                              │
                              ▼
                  Operational Data Quality
                              │
                              ▼
─────────────────────────────────────────────────────────────
                         RAW INGESTION
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
                       Amazon S3 Raw
                              │
─────────────────────────────────────────────────────────────
                  DISTRIBUTED DATA PROCESSING
                              │
                 S3 ObjectCreated event
                              ▼
                         AWS Lambda
                              │
                              ▼
                         Amazon EMR
                              │
                              ▼
                     PySpark Bronze
                              │
                              ▼
                PySpark + Spark SQL Silver
                              │
                              ▼
─────────────────────────────────────────────────────────────
                    ANALYTICAL DATA PRODUCTS
                              │
                              ▼
                              Gold
                              │
     ┌────────────────────────┼────────────────────────┐
     ▼                        ▼                        ▼
 Workforce / Recruitment   Learning / Performance   Payroll / Attrition
     │                        │                        │
     └────────────────────────┼────────────────────────┘
                              ▼
                        dbt Models
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
         Power BI        Analytics / ML    Governed API
                                               │
                                               ▼
                                      Approved Consumers
```

------------------------------------------------------------------------

# 1. Operational Layer

The operational layer simulates an enterprise HRIS.

It includes:

-   the synthetic workforce simulator;
-   PostgreSQL;
-   governed reference data;
-   workforce lifecycle events;
-   employee current state.

The simulator generates realistic relationships and temporal behaviour
across recruitment, attendance, leave, payroll, training, performance,
movement and employee exits.

------------------------------------------------------------------------

# 2. Reference Data Layer

Controlled vocabularies are managed centrally.

``` text
YAML
  ↓
Loader
  ↓
Validation
  ↓
PostgreSQL Reference Tables
  ↓
Simulator
```

This removes duplicated hard-coded controlled values from migrated
simulator modules and provides a single governed source.

------------------------------------------------------------------------

# 3. Data Quality Layer

Quality controls operate as pipeline gates.

Current operational checks include:

-   duplicate detection;
-   referential integrity;
-   hierarchy validation;
-   salary and payroll rules;
-   lifecycle reconciliation;
-   employment-date validation;
-   reference-data validation.

Future Bronze, Silver and Gold controls extend validation across the
analytical layers.

------------------------------------------------------------------------

# 4. Raw Ingestion Layer

PostgreSQL is extracted to Parquet through Python.

``` text
PostgreSQL
    ↓
Python Extraction
    ↓
Local Raw Parquet
    ↓
Source-to-Raw Reconciliation
    ↓
Amazon S3 Raw
```

Raw is immutable, source-aligned, batch-traceable and replayable.

------------------------------------------------------------------------

# 5. Bronze Layer

The portable Bronze code foundation is implemented. It includes explicit Raw
batch selection, physical source-file lineage, technical metadata, deterministic
record hashing, structural validation, Raw-to-Bronze reconciliation and
duplicate-safe Parquet publication. The complete local suite is verified in a
Linux Docker Spark runtime. All 17 supported datasets in one shared Raw batch
are published and verified in S3 Bronze.

The same Spark job should run locally and through `spark-submit` on
Amazon EMR.

``` text
S3 Raw
   ↓
PySpark
   ↓
Technical Metadata
   ↓
Record Hashing
   ↓
Structural Validation
   ↓
S3 Bronze
```

Bronze preserves source business values and adds technical lineage.

------------------------------------------------------------------------

# 6. Silver Layer

Silver creates trusted analytical entities.

Processing uses both PySpark DataFrame APIs and Spark SQL.

Responsibilities include:

-   schema enforcement;
-   type standardisation;
-   deduplication;
-   null handling;
-   reference-data conformity;
-   validated joins;
-   business rules;
-   effective-date logic;
-   integrated workforce entities.

------------------------------------------------------------------------

# 7. Gold Layer

Gold is explicitly data-product oriented.

``` text
Gold
├── Workforce Analytics
├── Recruitment Analytics
├── Learning Analytics
├── Performance Analytics
├── Payroll Analytics
└── Attrition Analytics
```

Gold provides business-ready datasets for reporting, analytical
applications and governed sharing.

dbt can be used to manage SQL-based Gold models, dependencies, tests,
documentation and reusable metrics.

------------------------------------------------------------------------

# 8. Amazon EMR

Amazon EMR provides the managed Spark runtime for cloud processing.

The architecture keeps Spark code portable:

``` text
Local PySpark
      │
      │ same job entry point
      ▼
spark-submit
      │
      ▼
Amazon EMR
```

This allows transformation logic to be developed and tested locally
before cloud execution.

EMR is responsible for compute, not permanent storage. Persistent
datasets remain in Amazon S3.

------------------------------------------------------------------------

# 9. Event-Driven Architecture

The target platform includes event-driven scheduling for Raw-to-Bronze
processing.

``` text
New Raw Object
      │
      ▼
Amazon S3 Event
      │
      ▼
AWS Lambda
      │
      ▼
Submit EMR Spark Job
      │
      ▼
Bronze Output
```

The Lambda function acts as a lightweight control-plane component.
Transformation logic remains inside Spark jobs.

This architecture supports near-automatic processing when new Raw
batches arrive.

------------------------------------------------------------------------

# 10. Scheduled Orchestration

Not every workload should be event-driven.

Scheduled orchestration remains appropriate for:

-   recurring operational extraction;
-   periodic quality reporting;
-   payroll cycles;
-   Gold refreshes;
-   metadata publication;
-   maintenance;
-   dependent multi-stage workflows.

A workflow orchestrator can be introduced when scheduling and dependency
management become sufficiently complex.

------------------------------------------------------------------------

# 11. Amazon S3

S3 provides durable storage for the analytical platform.

``` text
s3://<bucket>/
├── raw/
├── bronze/
├── silver/
└── gold/
```

S3 also provides the event source for the target Lambda-triggered Bronze
workflow.

------------------------------------------------------------------------

# 12. Analytics Engineering

Analytics engineering sits between trusted Silver data and business
consumption.

``` text
Silver
   ↓
Gold Data Products
   ↓
dbt Models / Tests
   ↓
Reporting Models
```

dbt is used where declarative SQL modelling provides clear value; it
does not replace PySpark processing.

------------------------------------------------------------------------

# 13. Power BI

Power BI consumes curated Gold products or reporting models rather than
PostgreSQL, Raw or Bronze data.

This keeps reporting logic isolated from ingestion and operational
systems.

------------------------------------------------------------------------

# 14. Advanced Analytics and Machine Learning

Governed Silver and Gold datasets can support:

-   attrition prediction;
-   workforce forecasting;
-   burnout analysis;
-   promotion analysis;
-   workforce segmentation.

Raw data should not normally be consumed directly by ML workloads.

------------------------------------------------------------------------

# 15. Governed Data Sharing

Approved analytical products can be exposed through FastAPI.

``` text
Gold Data Product
       │
       ▼
Sharing Policy
       │
       ▼
FastAPI
       │
       ├── Authentication
       ├── Authorisation
       ├── API Keys
       ├── Rate Limiting
       └── Audit Logging
       │
       ▼
Approved Consumer
```

Third parties do not receive direct access to PostgreSQL, Raw, Bronze or
unrestricted Silver datasets.

------------------------------------------------------------------------

# 16. Metadata and Lineage

The metadata capability will document implemented platform assets
including:

-   dataset schemas;
-   ownership;
-   classifications;
-   quality results;
-   lineage;
-   business definitions;
-   data products.

The target lineage chain is:

``` text
Reference YAML
      ↓
PostgreSQL
      ↓
Raw Batch
      ↓
Bronze
      ↓
Silver
      ↓
Gold Data Product
      ↓
Power BI / ML / API
```

------------------------------------------------------------------------

# 17. Infrastructure Architecture

Target AWS infrastructure includes:

-   Amazon S3;
-   IAM;
-   AWS Lambda;
-   Amazon EMR;
-   networking where required;
-   secrets management;
-   optional EC2 operational tooling;
-   API infrastructure.

Terraform will provide repeatable Infrastructure as Code.

------------------------------------------------------------------------

# 18. CI/CD

GitHub Actions will support:

-   linting;
-   automated tests;
-   security scanning;
-   Spark validation;
-   dbt validation;
-   Terraform validation;
-   deployment automation.

------------------------------------------------------------------------

# 19. Security Architecture

Security is applied at each boundary.

``` text
PostgreSQL
    └── Database credentials / roles

Amazon S3
    └── IAM policies

AWS Lambda / EMR
    └── IAM execution roles

Gold
    └── Approved analytical datasets

FastAPI
    ├── Authentication
    ├── Authorisation
    ├── Rate limiting
    └── Audit logging
```

Secrets must not be committed to source control.

------------------------------------------------------------------------

# 20. Quality Architecture

``` text
Reference Data
      ↓
Reference Validation
      ↓
PostgreSQL
      ↓
Operational Validation
      ↓
Raw
      ↓
Source-to-Raw Reconciliation
      ↓
Bronze
      ↓
Structural Validation
      ↓
Silver
      ↓
Business Conformity
      ↓
Gold
      ↓
Analytical Tests
      ↓
Data Products
```

This provides defence in depth.

------------------------------------------------------------------------

# 21. Current and Target State

  Capability                       Status
  -------------------------------- ------------------------
  Operational simulator            Implemented
  PostgreSQL                       Implemented
  Reference-data framework         Implemented
  Operational quality              Implemented
  Parquet extraction               Implemented
  Raw reconciliation               Implemented
  S3 Raw upload                    Implemented
  Local PySpark foundation         Implemented; Linux Docker suite verified
  Bronze                           Implemented; full 17-dataset S3 batch verified
  Amazon EMR                       Planned next
  Event-driven S3 → Lambda → EMR   Planned
  Silver                           Planned
  Gold data products               Planned
  dbt                              Planned
  Metadata / lineage               Planned
  Power BI                         Planned
  ML                               Planned
  FastAPI sharing                  Planned
  Terraform                        Planned
  GitHub Actions                   Planned

------------------------------------------------------------------------

# Summary

The target system architecture is:

``` text
PostgreSQL
    ↓
Amazon S3 Raw
    ↓
Amazon EMR / PySpark Bronze
    ↓
PySpark + Spark SQL Silver
    ↓
Gold Data Products
    ↓
Power BI / Analytics / API
```

Event-driven S3 and Lambda integration automates data-arrival processing
while keeping Spark transformation logic portable and independently
testable.
