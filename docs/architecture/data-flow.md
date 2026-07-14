# Data Flow

## Overview

This document describes how workforce data moves through the People Analytics Lakehouse Platform from generation to business consumption.

The platform supports both inbound data ingestion and outbound governed data sharing.

---

# End-to-End Data Flow

```text
               THIRD-PARTY DATA SOURCES
               ────────────────────────

      Public Holidays API
      Labour Market API
      Exchange Rate API
      Weather API
      Learning Provider API
      Geocoding API
                │
                ▼
         integrations/
                │
                ▼
       Reference Data Enrichment
                │
                ▼

──────────────────────────────────────────────────────────

             OPERATIONAL DATA GENERATION

     Simulator
        │
        ├── Employees
        ├── Attendance
        ├── Payroll
        ├── Recruitment
        ├── Promotions
        ├── Leave
        ├── Training
        ├── Performance Reviews
        ├── Employee Surveys
        ├── Manager Feedback
        └── Exit Interviews

                │
                ▼

──────────────────────────────────────────────────────────

          PostgreSQL Operational Database

                 public schema

                │
                ▼

──────────────────────────────────────────────────────────

         Enterprise Data Quality Framework

        Duplicate Checks
        Integrity Checks
        Business Rules
        Metadata Validation
        Reference Data Validation

                │
                ▼

──────────────────────────────────────────────────────────

              ETL Extraction Layer

      Extract
            │
      Transform
            │
      Export

                │
                ▼

──────────────────────────────────────────────────────────

             AWS S3 Data Lake

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

                │
                ▼

──────────────────────────────────────────────────────────

          Analytics Engineering

              dbt Models

       Staging
           │
           ▼
     Intermediate
           │
           ▼
        Data Marts
           │
           ▼
       Reporting Models

                │
                ▼

──────────────────────────────────────────────────────────

          Business Consumption

      Power BI Dashboards
      Machine Learning Models
      Workforce Forecasting
      Attrition Prediction

                │
                ▼

──────────────────────────────────────────────────────────

        Secure Data Sharing API

          Authentication
          Authorisation
          API Keys
          Audit Logging
          Rate Limiting

                │
                ▼

──────────────────────────────────────────────────────────

       Approved External Consumers

       Government
       Universities
       Research Partners
       HR Vendors
       Internal Business Units
```

---

# Inbound Data Flow

The platform receives data from two primary sources.

## 1. Simulated Operational Data

The simulator generates realistic workforce transactions that populate the PostgreSQL operational database.

Examples include:

- Employees
- Attendance
- Payroll
- Promotions
- Recruitment
- Performance Reviews

---

## 2. Third-Party Data

External APIs provide enrichment data that enhances operational datasets.

Examples include:

- Public holidays
- Labour market statistics
- Exchange rates
- Geographical information
- Weather conditions

These datasets are integrated through the `integrations/` package before entering downstream analytical pipelines.

---

# Internal Processing Flow

Operational data follows a structured processing pipeline.

```text
Simulation
      │
      ▼
PostgreSQL
      │
      ▼
Quality Validation
      │
      ▼
ETL
      │
      ▼
AWS S3
      │
      ▼
Bronze
      │
      ▼
Silver
      │
      ▼
Gold
      │
      ▼
dbt
      │
      ▼
Reporting
```

Each stage progressively improves data quality, consistency and analytical value.

---

# Metadata Flow

Metadata is captured throughout the platform.

```text
Metadata YAML
        │
        ▼
Metadata Loader
        │
        ▼
Metadata Catalogue

        Assets
        Columns
        Lineage
        Ownership
        Quality
```

The catalogue provides centralised governance for all analytical assets.

---

# Outbound Data Sharing

The platform exposes approved analytical products through a secure REST API.

The API never exposes raw operational tables directly.

Instead, it provides curated datasets built from reporting models and Gold-layer data products.

```text
Gold Layer
      │
      ▼
Reporting Models
      │
      ▼
Secure REST API
      │
      ▼
Approved Consumers
```

Security controls include:

- Authentication
- API Keys
- Role-Based Access Control (RBAC)
- Rate Limiting
- Audit Logging
- Access Policies

Only authorised consumers can access approved data products.

---

# Data Governance

Governance is embedded throughout the platform.

Each dataset is accompanied by:

- Business metadata
- Technical metadata
- Data lineage
- Ownership
- Data classifications
- Data quality metrics

This ensures that analytical outputs remain trusted, traceable and reusable across the organisation.