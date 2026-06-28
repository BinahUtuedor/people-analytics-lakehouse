# People Analytics Lakehouse Platform

### Enterprise Data Engineering, Analytics Engineering & People Analytics Project

![Python](https://img.shields.io/badge/Python-3.13-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![dbt](https://img.shields.io/badge/dbt-Analytics%20Engineering-orange)
![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)
![Docker](https://img.shields.io/badge/Docker-Container-blue)
![License](https://img.shields.io/badge/License-MIT-green)

---

# Project Overview

The **People Analytics Lakehouse Platform** is an enterprise-scale end-to-end data engineering project that simulates the operations of a fictional multinational organisation and transforms operational HR data into strategic business insights.

Unlike traditional HR analytics projects that rely on static CSV files, this platform includes a **company simulation engine** capable of generating realistic workforce events over multiple years.

The platform combines **structured quantitative HR data** (attendance, payroll, performance, recruitment, learning, promotions and engagement scores) with **unstructured qualitative HR data** (employee surveys, manager feedback and exit interviews).

The project demonstrates how a modern **Lakehouse Architecture** can be used to support executive decision-making through scalable analytics, Natural Language Processing (NLP), and predictive modelling.

---

# Business Problem

Modern organisations collect employee data from multiple operational systems including:

* Human Resource Information Systems (HRIS)
* Payroll Systems
* Learning Management Systems (LMS)
* Recruitment Platforms (ATS)
* Performance Management Systems
* Employee Engagement Platforms

These systems often operate independently, making it difficult for leadership teams to gain a unified understanding of workforce performance.

The objective of this project is to build a modern analytics platform capable of answering strategic business questions such as:

* Why are employees leaving?
* Which departments are experiencing burnout?
* Which managers consistently achieve high engagement?
* How effective are learning programmes?
* Which teams are likely to require additional recruitment?
* How does employee sentiment influence attrition?
* Can future employee turnover be predicted?

---

# Project Objectives

This project demonstrates practical experience in:

* Enterprise Python Development
* Data Engineering
* Analytics Engineering
* Data Lakehouse Architecture
* Apache Spark
* Delta Lake
* ETL Development
* Data Quality Engineering
* Data Modelling
* Natural Language Processing
* Machine Learning
* Dashboard Development
* Cloud Data Engineering

---

# High-Level Architecture

```text
                    Company Simulation Engine
                              │
                              ▼
                 Raw Landing Files (CSV / Parquet)
                              │
                              ▼
                 PostgreSQL Operational Database
                              │
                              ▼
                     Python ETL Pipeline
                              │
                              ▼
                         AWS S3 Data Lake
                              │
                              ▼
                 Databricks Bronze Layer
                              │
                              ▼
                 Databricks Silver Layer
                              │
                              ▼
                  Databricks Gold Layer
                              │
                              ▼
                         dbt Transformations
                              │
                              ▼
                    Executive Analytics Layer
                              │
                              ▼
                           Power BI
```

---

# Technology Stack

| Layer                       | Technology                |
| --------------------------- | ------------------------- |
| Programming Language        | Python 3.13               |
| Operational Database        | PostgreSQL                |
| Object Storage              | AWS S3                    |
| Lakehouse Platform          | Databricks                |
| Distributed Processing      | Apache Spark              |
| Storage Format              | Delta Lake                |
| Analytics Engineering       | dbt                       |
| Data Processing             | Pandas                    |
| Data Generation             | Faker                     |
| Numerical Simulation        | NumPy                     |
| Database ORM                | SQLAlchemy                |
| Natural Language Processing | spaCy                     |
| Machine Learning            | Spark MLlib, Scikit-learn |
| Dashboard                   | Power BI                  |
| Workflow Orchestration      | Apache Airflow            |
| Containerisation            | Docker                    |
| Version Control             | Git                       |
| Testing                     | Pytest                    |

---

# Project Structure

```text
people-analytics-lakehouse-platform/
│
├── config/                              # Application configuration
│   ├── __init__.py
│   ├── settings.py                      # Environment configuration
│   ├── constants.py                     # Global constants
│   └── logger.py                        # Centralised logging
│
├── simulator/                           # Company simulation engine
│   ├── __init__.py
│   ├── company.py
│   ├── departments.py
│   ├── locations.py
│   ├── job_roles.py
│   ├── managers.py
│   ├── employees.py
│   ├── payroll.py
│   ├── attendance.py
│   ├── leave.py
│   ├── recruitment.py
│   ├── promotions.py
│   ├── transfers.py
│   ├── training.py
│   ├── performance.py
│   ├── engagement.py
│   ├── surveys.py
│   ├── manager_feedback.py
│   ├── exit_interviews.py
│   ├── attrition.py
│   └── simulator.py                     # Simulation orchestrator
│
├── database/                            # Operational database
│   ├── __init__.py
│   ├── postgres.py
│   ├── models.py
│   ├── base.py
│   ├── connection.py
│   ├── create_schema.py
│   └── seed.py
│
├── quality/                             # Data quality framework
│   ├── __init__.py
│   ├── validation.py
│   ├── expectations.py
│   ├── duplicate_checks.py
│   ├── integrity_checks.py
│   └── business_rules.py
│
├── etl/                                 # ETL pipelines
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── load_postgres.py
│   ├── export_s3.py
│   ├── bronze_loader.py
│   ├── silver_loader.py
│   └── gold_loader.py
│
├── spark/                               # PySpark jobs
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utilities.py
│
├── analytics/                           # Advanced analytics
│   ├── __init__.py
│   ├── sentiment_analysis.py
│   ├── topic_modelling.py
│   ├── attrition_prediction.py
│   ├── burnout_prediction.py
│   ├── promotion_prediction.py
│   └── workforce_forecasting.py
│
├── dbt/                                 # dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── marts/
│   │   └── reporting/
│   ├── snapshots/
│   ├── tests/
│   ├── macros/
│   ├── seeds/
│   ├── analyses/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── airflow/                             # Workflow orchestration
│   ├── dags/
│   ├── plugins/
│   └── requirements.txt
│
├── dashboards/                          # Power BI assets
│   ├── reports/
│   ├── datasets/
│   └── screenshots/
│
├── notebooks/                           # Exploratory notebooks
│   ├── data_generation.ipynb
│   ├── eda.ipynb
│   ├── nlp.ipynb
│   └── machine_learning.ipynb
│
├── data/
│   ├── raw/                             # Generated CSV / Parquet
│   ├── processed/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── sql/
│   ├── postgres/
│   ├── databricks/
│   └── reporting/
│
├── tests/
│   ├── simulator/
│   ├── etl/
│   ├── analytics/
│   └── database/
│
├── logs/
│
├── docs/
│   ├── architecture/
│   ├── diagrams/
│   └── images/
│
├── docker/
│   ├── postgres/
│   ├── airflow/
│   └── spark/
│
├── .github/
│   └── workflows/
│       ├── lint.yml
│       ├── tests.yml
│       └── deploy.yml
│
├── .env
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
├── main.py
├── LICENSE
└── README.md
```

## Repository Overview

| Folder         | Purpose                                                    |
| -------------- | ---------------------------------------------------------- |
| **config**     | Application configuration, logging and constants           |
| **simulator**  | Generates realistic organisational and HR operational data |
| **database**   | PostgreSQL operational database connection and models      |
| **quality**    | Data validation and business rule enforcement              |
| **etl**        | Extract, transform and load pipelines                      |
| **spark**      | PySpark jobs for Bronze, Silver and Gold transformations   |
| **analytics**  | NLP, predictive modelling and workforce analytics          |
| **dbt**        | Analytics engineering and semantic modelling               |
| **airflow**    | Workflow orchestration                                     |
| **dashboards** | Power BI reports and assets                                |
| **notebooks**  | Exploratory analysis and experimentation                   |
| **data**       | Raw, Bronze, Silver and Gold datasets                      |
| **sql**        | SQL scripts for PostgreSQL and Databricks                  |
| **tests**      | Automated unit and integration tests                       |
| **docs**       | Architecture documentation and diagrams                    |
| **docker**     | Docker configuration files                                 |
| **.github**    | CI/CD workflows using GitHub Actions                       |


# Medallion Architecture

The platform follows the Medallion Architecture used in modern enterprise lakehouses.

## Bronze Layer

Stores raw operational data exactly as received.

Examples include:

* Employee Master
* Attendance
* Payroll
* Recruitment
* Performance Reviews
* Training Records
* Employee Surveys
* Exit Interviews
* Manager Feedback

---

## Silver Layer

Data is cleaned, validated and standardised.

Typical transformations include:

* Duplicate removal
* Data validation
* Null handling
* Standardised data types
* Derived business attributes
* Slowly Changing Dimensions (SCD)

---

## Gold Layer

Business-ready datasets optimised for analytics.

Examples include:

* Workforce Analytics
* Attrition Analytics
* Performance Analytics
* Recruitment Analytics
* Learning Analytics
* Executive KPI Tables

---

# Project Modules

## Company Simulation Engine

The simulation engine creates a realistic organisation consisting of:

* Departments
* Locations
* Business Units
* Job Roles
* Managers
* Employees

It continuously generates operational events including:

* Recruitment
* Promotions
* Transfers
* Attendance
* Leave
* Overtime
* Payroll
* Performance Reviews
* Employee Surveys
* Manager Feedback
* Exit Interviews

Relationships between datasets are realistic rather than random.

Examples include:

* Higher overtime reduces engagement.
* Higher engagement lowers attrition probability.
* High-performing employees receive promotions.
* Promotions increase salary.
* Effective managers improve team engagement.

---

## Operational Database

Operational data is stored within PostgreSQL.

Representative tables include:

* employees
* departments
* managers
* attendance
* payroll
* recruitment
* performance_reviews
* promotions
* employee_surveys
* manager_feedback
* exit_interviews
* training_records

---

## ETL Pipeline

```text
PostgreSQL
      │
      ▼
Extract
      │
      ▼
Transform
      │
      ▼
AWS S3
      │
      ▼
Databricks Bronze
      │
      ▼
Databricks Silver
      │
      ▼
Databricks Gold
```

---

# Data Quality Framework

Validation includes:

* Duplicate Employees
* Invalid Departments
* Missing Managers
* Invalid Salaries
* Future Hire Dates
* Duplicate Promotions
* Attendance Errors
* Invalid Performance Ratings

---

# Natural Language Processing

Qualitative HR data includes:

* Employee Survey Responses
* Manager Feedback
* Exit Interviews

The NLP pipeline extracts:

* Sentiment
* Topics
* Keywords
* Emotion
* Theme Frequency

These insights are combined with quantitative HR metrics to support workforce decision-making.

---

# Machine Learning

Predictive models include:

* Employee Attrition Prediction
* Burnout Prediction
* Promotion Prediction
* Performance Prediction
* Workforce Forecasting

---

# Executive Dashboards

Power BI dashboards include:

* Executive Overview
* Workforce Analytics
* Recruitment Analytics
* Attrition Dashboard
* Performance Dashboard
* Learning Dashboard
* Engagement Dashboard
* Diversity & Inclusion
* Predictive Workforce Analytics

---

# Data Generation

## Historical Initialisation

Executed once.

Generates:

* Approximately 2,000 employees
* Three years of historical operational data
* Monthly HR transactions

---

## Incremental Simulation

Executed monthly.

Generates realistic operational changes including:

* New hires
* Promotions
* Salary adjustments
* Attendance
* Leave
* Performance reviews
* Engagement surveys
* Exit interviews

This simulates the continuous evolution of an enterprise workforce.

---

# Development Roadmap

## Phase 1

Project Foundation

* Repository setup
* Docker
* Configuration
* Logging
* Environment Management

## Phase 2

Operational Database

* PostgreSQL
* SQLAlchemy
* Database Schema
* ORM Models

## Phase 3

Company Simulation Engine

* Departments
* Managers
* Employees
* Payroll
* Recruitment

## Phase 4

Historical Data Generation

Generate three years of realistic HR history.

## Phase 5

Incremental Monthly Simulation

Generate ongoing workforce events.

## Phase 6

Data Quality Framework

Validation and automated quality checks.

## Phase 7

AWS S3 Data Lake

Landing zone for raw operational data.

## Phase 8

Databricks Bronze Layer

Raw Delta tables.

## Phase 9

Databricks Silver Layer

Validated and transformed Delta tables.

## Phase 10

Databricks Gold Layer

Business-ready analytics tables.

## Phase 11

dbt

Analytics engineering and semantic modelling.

## Phase 12

Power BI

Executive dashboards and reporting.

## Phase 13

Natural Language Processing

Sentiment and topic modelling.

## Phase 14

Machine Learning

Predictive workforce analytics.

---

# Learning Outcomes

This project demonstrates expertise in:

* Python
* SQL
* PostgreSQL
* Apache Spark
* PySpark
* Delta Lake
* Databricks
* AWS S3
* Docker
* dbt
* Apache Airflow
* ETL Development
* Analytics Engineering
* Data Lakehouse Architecture
* Machine Learning
* Natural Language Processing
* Power BI
* Enterprise Data Engineering
* People Analytics

---

# Future Enhancements

Potential future improvements include:

* Delta Live Tables
* Unity Catalog
* Apache Kafka
* Change Data Capture (CDC)
* Spark Structured Streaming
* Terraform
* GitHub Actions CI/CD
* Kubernetes
* REST API
* HR Self-Service Web Portal

---

# Disclaimer

All datasets generated within this project are entirely synthetic.

No real employee information is used.

This project is intended solely for educational, research, and portfolio purposes.

---

# Author

**Utuedor Binah**

Enterprise Data Engineering • Analytics Engineering • Cloud Data Engineering • Data Lakehouse Architecture • Financial Technology • People Analytics
