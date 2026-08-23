# Data Flow

## Overview

The **People Analytics Lakehouse Platform** is an end-to-end data
engineering and analytics platform for generating, validating,
ingesting, transforming, governing and sharing synthetic workforce data.

The implemented platform currently supports the operational-to-Raw
pipeline. The next implementation phase extends that foundation through
**Apache Spark / PySpark on Amazon EMR**, using Amazon S3 for Raw,
Bronze, Silver and Gold storage.

The target architecture supports both **event-driven processing** and
scheduled workflows.

------------------------------------------------------------------------

# 1. Current Implementation Status

The following flow is currently operational:

``` text
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
Amazon S3 Raw
```

Implemented CLI workflows include:

``` powershell
python main.py simulate --full-refresh
python main.py validate
python main.py extract
python main.py validate-raw
python main.py upload-s3
python main.py raw-pipeline
python main.py full-refresh
```

------------------------------------------------------------------------

# 2. Target End-to-End Data Flow

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
                    SYNTHETIC HRIS
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
                       ObjectCreated event
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
                    Gold Data Products
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
           dbt             Power BI        Analytics / ML
             │
             ▼
       Reporting Models
             │
             ▼
        Governed FastAPI
             │
             ▼
       Approved Consumers
```

The S3 event → Lambda → EMR path is the target event-driven
implementation. The immediate next step is to complete the local Bronze
Spark foundation and then run the same portable Spark workload on Amazon
EMR.

------------------------------------------------------------------------

# 3. Reference Data Flow

Reference data is maintained as governed YAML configuration rather than
duplicated inside simulator modules.

``` text
reference_data/*.yml
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

Governed datasets include:

``` text
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

The simulator consumes governed values from PostgreSQL while operational
tables retain descriptive business values, preserving downstream
compatibility.

------------------------------------------------------------------------

# 4. Operational Data Generation

The simulator represents a synthetic enterprise HR information system
and generates interconnected workforce lifecycle data including:

-   employees;
-   recruitment;
-   attendance;
-   payroll;
-   leave requests;
-   training;
-   promotions;
-   transfers;
-   performance reviews;
-   employee surveys;
-   manager feedback;
-   employee exits;
-   exit interviews.

Generation order preserves lifecycle dependencies and employment-date
boundaries.

``` text
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
Attendance          Payroll
Leave               Training
Performance         Promotions
Transfers           Surveys
Manager Feedback
      │
      ▼
Exit Interviews
```

------------------------------------------------------------------------

# 5. PostgreSQL Operational Database

PostgreSQL is the operational source system, not the analytical
warehouse.

It stores:

-   governed reference tables;
-   employee master data;
-   workforce lifecycle events;
-   transactional workforce facts.

The analytical platform does not depend on direct PostgreSQL queries for
reporting workloads.

------------------------------------------------------------------------

# 6. Operational Data Quality

Operational data passes through quality gates before extraction.

Implemented checks include:

-   duplicate detection;
-   referential integrity;
-   employee hierarchy validation;
-   salary and payroll validation;
-   employment-date validation;
-   recruitment lifecycle reconciliation;
-   promotion-state reconciliation;
-   transfer-state reconciliation;
-   employee-exit reconciliation;
-   effective-date validation.

``` text
PostgreSQL
     │
     ▼
Quality Checks
     │
     ├── PASS → extraction permitted
     │
     └── FAIL → pipeline stopped
```

------------------------------------------------------------------------

# 7. Raw Ingestion

Operational PostgreSQL tables are extracted to partitioned Parquet files
and enriched with extraction metadata.

Typical local layout:

``` text
data/raw/postgres/
└── employees/
    └── extraction_date=YYYY-MM-DD/
        └── extraction_id=<id>/
            └── part-00000.parquet
```

Validated Raw data is uploaded to:

``` text
s3://<bucket>/raw/postgresql/
```

Raw remains source-aligned, immutable, batch-traceable and replayable.

------------------------------------------------------------------------

# 8. Raw Reconciliation

Before a Raw batch is accepted, source and extracted record counts are
reconciled.

``` text
PostgreSQL Count
       │
       ├──────────────┐
       │              │
       ▼              ▼
Source Count     Raw Parquet Count
       │              │
       └──────┬───────┘
              ▼
          Reconcile
          │       │
        PASS     FAIL
```

Only validated Raw batches should continue to Bronze processing.

------------------------------------------------------------------------

# 9. Amazon S3 Data Lake

Amazon S3 is the persistent analytical storage layer.

``` text
s3://<people-analytics-bucket>/
├── raw/
│   └── postgresql/
├── bronze/
├── silver/
└── gold/
```

Responsibilities are separated by layer:

-   **Raw** --- immutable source archive;
-   **Bronze** --- source-conformed records with technical metadata;
-   **Silver** --- cleansed, standardised and integrated entities;
-   **Gold** --- business-ready analytical data products.

Initial Bronze, Silver and Gold storage uses Parquet on S3.

------------------------------------------------------------------------

# 10. Bronze Flow

Bronze is the next implementation priority.

``` text
S3 Raw Parquet
      │
      ▼
PySpark
      │
      ├── schema handling
      ├── technical metadata
      ├── source tracking
      ├── batch lineage
      └── record hashing
      │
      ▼
S3 Bronze Parquet
```

Typical technical columns include:

``` text
_bronze_ingested_at
_source_system
_source_table
_source_file
_batch_id
_extraction_date
_record_hash
```

Bronze preserves source business values and avoids significant business
transformations.

------------------------------------------------------------------------

# 11. Silver Flow

Silver will use PySpark and Spark SQL to create trusted analytical
entities.

``` text
S3 Bronze
    │
    ▼
PySpark + Spark SQL
    │
    ▼
S3 Silver
```

Responsibilities include:

-   explicit schema enforcement;
-   data-type standardisation;
-   null handling;
-   deduplication;
-   reference-data conformity;
-   validated joins;
-   business-rule enforcement;
-   date and key standardisation;
-   reusable analytical entities.

------------------------------------------------------------------------

# 12. Gold Data Products

Gold is organised around analytical domains.

``` text
Gold
├── Workforce Analytics
├── Recruitment Analytics
├── Learning Analytics
├── Performance Analytics
├── Payroll Analytics
└── Attrition Analytics
```

Gold products provide governed datasets for Power BI, advanced
analytics, machine learning and approved API consumers.

dbt will be introduced where SQL-based analytical modelling, tests,
documentation and reusable reporting models add value.

------------------------------------------------------------------------

# 13. Event-Driven Processing

The target cloud architecture supports processing triggered by data
arrival.

``` text
Raw object written to S3
          │
          ▼
    S3 ObjectCreated
          │
          ▼
       Lambda
          │
          ▼
   Submit EMR job
          │
          ▼
      Bronze
```

The Lambda function should identify the source dataset and batch,
validate the event and submit the appropriate portable Spark job to
Amazon EMR.

Event-driven execution complements rather than replaces scheduled
orchestration.

------------------------------------------------------------------------

# 14. Scheduled Processing

Scheduled workflows remain appropriate for recurring activities such as:

-   operational extraction;
-   periodic quality reporting;
-   payroll cycles;
-   Gold refreshes;
-   metadata publication;
-   maintenance jobs.

A workflow orchestrator can be introduced when cross-stage dependencies
and recurring schedules justify it.

------------------------------------------------------------------------

# 15. Metadata and Lineage

Metadata should describe implemented assets and be expanded as Bronze,
Silver and Gold are created.

The target lineage chain is:

``` text
Reference YAML
      │
      ▼
PostgreSQL
      │
      ▼
Raw Extraction
      │
      ▼
S3 Raw Batch
      │
      ▼
S3 Bronze
      │
      ▼
S3 Silver
      │
      ▼
Gold Data Product
      │
      ▼
Power BI / ML / API
```

Batch identifiers, extraction metadata, source files and record hashes
support traceability.

------------------------------------------------------------------------

# 16. Governed Data Sharing

External consumers do not receive direct access to PostgreSQL, Raw,
Bronze or unrestricted Silver data.

``` text
Gold Data Product
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
       └── Audit Logging
       │
       ▼
Approved Consumer
```

------------------------------------------------------------------------

# 17. Current and Target Capability

  Capability                                 Status
  ------------------------------------------ ------------------------
  Synthetic HR simulator                     Implemented
  PostgreSQL operational database            Implemented
  YAML reference-data framework              Implemented
  Reference-data validation                  Implemented
  Governed simulator reference usage         Implemented
  Operational data-quality framework         Implemented
  PostgreSQL → Parquet extraction            Implemented
  Shared extraction/batch identity           Implemented
  Raw extraction reconciliation              Implemented
  Amazon S3 Raw upload                       Implemented
  Root platform CLI                          Implemented
  Local PySpark foundation                   Implemented / evolving
  Bronze processing                          Next implementation
  Amazon EMR execution                       Planned next
  S3 → Lambda → EMR event trigger            Planned
  Silver PySpark / Spark SQL                 Planned
  Gold domain data products                  Planned
  dbt analytical modelling                   Planned
  Metadata catalogue / lineage publication   Planned
  Power BI                                   Planned
  Machine learning                           Planned
  FastAPI governed sharing                   Planned
  Terraform                                  Planned
  GitHub Actions CI/CD                       Planned

------------------------------------------------------------------------

# Summary

The platform currently has a working operational-to-Raw pipeline. The
next phase extends it without replacing the existing foundation:

``` text
PostgreSQL
    ↓
S3 Raw
    ↓
PySpark Bronze
    ↓
PySpark + Spark SQL Silver
    ↓
Gold Data Products
    ↓
Governed Consumption
```

Amazon EMR provides the managed Spark execution environment, while S3
events and Lambda provide a path to event-driven processing.
