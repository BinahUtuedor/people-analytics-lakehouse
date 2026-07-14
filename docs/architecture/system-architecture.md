# System Architecture

## Overview

The People Analytics Lakehouse Platform is an end-to-end enterprise data platform designed to simulate, ingest, validate, transform, govern, analyse and securely share workforce data.

The platform follows a layered architecture inspired by modern cloud data engineering practices and separates operational systems from analytical workloads.

---

# End-to-End System Architecture

```text
                           Third-Party APIs
                                   │
                                   ▼
                         Inbound Integrations
                                   │
                                   ▼
                      Reference Data Management
                                   │
                                   ▼
                   Operational Data Simulation
                                   │
                                   ▼
                     PostgreSQL Operational Database
                           (public schema)
                                   │
                                   ▼
                    Enterprise Data Quality Framework
                                   │
                                   ▼
                     Metadata & Data Catalogue
                                   │
                                   ▼
                           ETL Pipelines
                                   │
                                   ▼
                           AWS S3 Data Lake
                                   │
                     ┌─────────────┴─────────────┐
                     ▼                           ▼
                Bronze Layer               Raw Archive
                     │
                     ▼
                Silver Layer
                     │
                     ▼
                 Gold Layer
                     │
                     ▼
              dbt Transformations
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
 Power BI      Machine Learning   Metadata Catalogue
 Dashboards      & Analytics
        │
        ▼
 Secure REST API
        │
        ▼
 Third-Party Consumers
```

---

# Architecture Layers

## 1. Operational Layer

The operational layer simulates an enterprise Human Resources Information System (HRIS). It generates realistic workforce events such as recruitment, attendance, payroll, promotions, performance reviews, training and employee surveys.

Primary components:

- Simulator
- PostgreSQL
- Reference Data

---

## 2. Data Quality Layer

The quality framework validates operational data before it enters the analytical platform.

Capabilities include:

- Business rule validation
- Duplicate detection
- Referential integrity
- Metadata validation
- Reference data validation
- Quality reporting

---

## 3. Data Engineering Layer

This layer extracts operational data, stages it within AWS S3 and prepares datasets for analytical consumption.

Components include:

- ETL
- AWS S3
- PySpark
- Bronze
- Silver
- Gold

---

## 4. Analytics Engineering Layer

dbt transforms cleansed datasets into dimensional models and reporting marts suitable for analytics and business intelligence.

Outputs include:

- Fact tables
- Dimension tables
- Reporting marts

---

## 5. Governance Layer

Enterprise governance capabilities ensure trust in organisational data.

Components include:

- Metadata
- Data Catalogue
- Lineage
- Ownership
- Data Classification
- Data Quality

---

## 6. Consumption Layer

Business users consume curated data through dashboards, machine learning models and secure APIs.

Consumers include:

- HR
- Finance
- Leadership
- External organisations
- Research partners