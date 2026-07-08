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
│   │    ├── models/
│   │    ├── __init__.py
│   │    ├── business_unit.py
│   │    ├── department.py
│   │    ├── location.py
│   │    ├── job_role.py
│   │    ├── employee.py
│   │    ├── attendance.py
│   │    ├── payroll.py
│   │    ├── leave.py
│   │    ├── recruitment.py
│   │    ├── promotion.py
│   │    ├── transfer.py
│   │    ├── training.py
│   │    ├── performance_review.py
│   │    ├── employee_survey.py
│   │    ├── manager_feedback.py
│   │    └── exit_interview.py
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

## Operational Database ER Diagram

### Operational Database ER Diagram
                                         +----------------------+
                                         |      Locations       |
                                         +----------------------+
                                         | PK location_id       |
                                         | country             |
                                         | city                |
                                         | office_name         |
                                         | timezone            |
                                         +----------+----------+
                                                    |
                                                    |
                                                    |
                                         +----------v----------+
                                         |   Business Units    |
                                         +---------------------+
                                         | PK business_unit_id |
                                         | unit_name           |
                                         +----------+----------+
                                                    |
                                                    |
                                         +----------v----------+
                                         |    Departments      |
                                         +---------------------+
                                         | PK department_id    |
                                         | FK business_unit_id |
                                         | department_name     |
                                         | cost_center         |
                                         +----------+----------+
                                                    |
                                                    |
                         +--------------------------+-------------------------+
                         |                                                    |
                         |                                                    |
               +---------v----------+                             +-----------v-----------+
               |     Job Roles      |                             |      Managers         |
               +--------------------+                             +-----------------------+
               | PK role_id         |                             | PK manager_id         |
               | role_name          |                             | FK employee_id        |
               | grade              |                             | leadership_level      |
               | salary_band        |                             +-----------+-----------+
               +---------+----------+                                         |
                         |                                                    |
                         |                                                    |
                         +----------------------+-----------------------------+
                                                |
                                                |
                                      +---------v----------+
                                      |     Employees      |
                                      +--------------------+
                                      | PK employee_id     |
                                      | employee_number    |
                                      | first_name         |
                                      | last_name          |
                                      | email              |
                                      | gender             |
                                      | dob                |
                                      | hire_date          |
                                      | employment_status  |
                                      | FK department_id   |
                                      | FK role_id         |
                                      | FK manager_id      |
                                      | FK location_id     |
                                      +---------+----------+
                                                |
          ---------------------------------------------------------------------------------------------
          |             |              |             |            |             |            |          |
          |             |              |             |            |             |            |          |
+---------v----+ +------v------+ +-----v------+ +----v------+ +---v-------+ +---v-------+ +--v-----+ +-v----------------+
| Attendance   | | Payroll     | | Leave      | | Training | | Promotion | | Transfer | | Survey | | Performance Review|
+--------------+ +-------------+ +------------+ +-----------+ +-----------+ +-----------+ +---------+ +------------------+
| PK id        | | PK id       | | PK id      | | PK id     | | PK id     | | PK id     | | PK id  | | PK id           |
| FK employee  | | FK employee | | FK employee| | FK emp    | | FK emp    | | FK emp    | | FK emp | | FK employee     |
| work_date    | | pay_period  | | leave_type | | course    | | old_role  | | old_dept  | | score  | | review_date     |
| hours        | | gross_pay   | | start_date | | provider  | | new_role  | | new_dept  | | text   | | overall_rating  |
+--------------+ +-------------+ +------------+ +-----------+ +-----------+ +-----------+ +---------+ +------------------+

                                                                 |
                                                                 |
                           +-------------------------------------+-------------------------------------+
                           |                                     |                                     |
                           |                                     |                                     |
               +-----------v-----------+              +----------v-----------+            +-----------v------------+
               | Manager Feedback      |              | Exit Interviews      |            | Recruitment           |
               +------------------------+              +----------------------+            +-----------------------+
               | PK feedback_id         |              | PK exit_id           |            | PK recruitment_id     |
               | FK employee_id         |              | FK employee_id       |            | FK department_id      |
               | FK manager_id          |              | termination_date     |            | role_id              |
               | comments               |              | reason               |            | candidate_name       |
               | sentiment              |              | interview_text       |            | status               |
               +------------------------+              +----------------------+            +-----------------------+

---

# Relationship Summary

The following table summarises the primary relationships within the operational HR database.

| Parent Entity | Child Entity | Cardinality | Description |
|---------------|--------------|-------------|-------------|
| Business Unit | Department | 1 : Many | A business unit contains multiple departments. |
| Location | Employee | 1 : Many | A location can have many employees assigned to it. |
| Department | Employee | 1 : Many | Each department employs multiple employees. |
| Job Role | Employee | 1 : Many | Multiple employees may share the same job role. |
| Manager | Employee | 1 : Many | One manager supervises multiple employees. |
| Employee | Attendance | 1 : Many | An employee has multiple attendance records over time. |
| Employee | Payroll | 1 : Many | Each employee receives payroll records for every pay period. |
| Employee | Leave | 1 : Many | Employees can submit multiple leave requests throughout their employment. |
| Employee | Training | 1 : Many | Employees may complete numerous training courses. |
| Employee | Promotion | 1 : Many | Employees may receive multiple promotions during their career. |
| Employee | Transfer | 1 : Many | Employees may transfer between departments or locations multiple times. |
| Employee | Performance Review | 1 : Many | Employees receive periodic performance evaluations. |
| Employee | Survey | 1 : Many | Employees may complete multiple engagement or satisfaction surveys. |
| Employee | Manager Feedback | 1 : Many | Managers can provide multiple feedback records for each employee. |
| Employee | Exit Interview | 1 : 0..1 | An employee has at most one exit interview upon leaving the organisation. |
| Department | Recruitment | 1 : Many | Departments create multiple recruitment requests over time. |
| Job Role | Recruitment | 1 : Many | Recruitment campaigns are associated with specific job roles. |

---

# Database Normalisation

The operational database has been designed following the principles of **Third Normal Form (3NF)** to minimise redundancy, improve data integrity, and simplify future maintenance.

The design follows several key principles:

- **Lookup entities** such as Departments, Locations, Business Units and Job Roles are stored separately from transactional data.
- **Transactional tables** (Attendance, Payroll, Leave, Performance Reviews, Promotions, Transfers, Surveys, etc.) reference employees using foreign keys rather than duplicating employee information.
- Each table has a **single business responsibility**, ensuring a clean separation of concerns.
- Primary and foreign key constraints enforce referential integrity across the database.
- Lookup tables minimise duplicated values and improve consistency throughout the platform.
- The schema has been designed to support efficient joins for downstream ETL and analytics workloads.

This normalised operational model serves as the authoritative source system from which analytical models can later be built in the Lakehouse.

---

# Gold Layer Star Schemas

The operational PostgreSQL database is optimised for transactional processing (OLTP). During the Lakehouse transformation process, the data will be remodelled into dimensional star schemas optimised for analytical workloads (OLAP).

The following analytical marts will be created within the Gold layer.

---

## Workforce Analytics

**Fact Table**

- `fact_workforce`

**Dimension Tables**

- `dim_employee`
- `dim_department`
- `dim_location`
- `dim_job_role`
- `dim_date`

**Business Purpose**

Provides workforce headcount, demographics, organisational structure, and employee movement analytics.

---

## Payroll Analytics

**Fact Table**

- `fact_payroll`

**Dimension Tables**

- `dim_employee`
- `dim_department`
- `dim_date`

**Business Purpose**

Supports salary analysis, payroll trends, compensation reporting, overtime analysis, and labour cost forecasting.

---

## Attendance Analytics

**Fact Table**

- `fact_attendance`

**Dimension Tables**

- `dim_employee`
- `dim_department`
- `dim_date`

**Business Purpose**

Measures attendance, absenteeism, overtime, lateness, and workforce utilisation.

---

## Performance Analytics

**Fact Table**

- `fact_performance`

**Dimension Tables**

- `dim_employee`
- `dim_manager`
- `dim_department`
- `dim_date`

**Business Purpose**

Supports employee performance monitoring, manager effectiveness, KPI tracking, and performance trend analysis.

---

## Recruitment Analytics

**Fact Table**

- `fact_recruitment`

**Dimension Tables**

- `dim_department`
- `dim_job_role`
- `dim_date`

**Business Purpose**

Measures recruitment activity, hiring pipeline efficiency, time-to-hire, and recruitment success rates.

---

## Learning & Development Analytics

**Fact Table**

- `fact_training`

**Dimension Tables**

- `dim_employee`
- `dim_course`
- `dim_department`
- `dim_date`

**Business Purpose**

Tracks employee learning, certification completion, mandatory training compliance, and training effectiveness.

---

## Employee Engagement Analytics

**Fact Table**

- `fact_surveys`

**Dimension Tables**

- `dim_employee`
- `dim_department`
- `dim_manager`
- `dim_date`

**Business Purpose**

Analyses employee engagement scores, organisational sentiment, survey participation, and manager effectiveness.

---

## Attrition Analytics

**Fact Table**

- `fact_attrition`

**Dimension Tables**

- `dim_employee`
- `dim_exit_reason`
- `dim_department`
- `dim_date`

**Business Purpose**

Supports voluntary and involuntary attrition reporting, turnover analysis, retention monitoring, and predictive attrition modelling.

---

# Dimensional Modelling Strategy

The Gold Layer adopts a **Kimball-style dimensional modelling approach**.

Each analytical mart consists of:

- One central **Fact Table** containing measurable business events.
- Multiple surrounding **Dimension Tables** describing the business context.
- Shared conformed dimensions (such as Employee, Department and Date) to ensure consistent reporting across all subject areas.

This modelling approach significantly improves query performance and simplifies dashboard development within Power BI.

---

# Design Recommendation

To further align the project with enterprise data engineering best practices, the following enhancements are recommended before implementation begins.

## Introduce a Dedicated Date Dimension

Create a reusable `dim_date` table that contains:

- Calendar Date
- Day
- Week
- Month
- Quarter
- Year
- Financial Year
- Financial Quarter
- Weekday Indicator
- Public Holiday Indicator

A shared Date Dimension simplifies time-series reporting across all analytical models and is considered a standard component of modern data warehouses and Lakehouse platforms.

---

## Implement Slowly Changing Dimensions (Type 2)

Certain business entities naturally evolve over time. Rather than overwriting historical values, implement **Slowly Changing Dimension (SCD) Type 2** logic within the Silver and Gold layers to preserve historical changes.

Recommended SCD Type 2 dimensions include:

- `dim_employee`
- `dim_department`
- `dim_job_role`
- `dim_manager`
- `dim_location`

Each historical version should include fields such as:

- Effective Start Date
- Effective End Date
- Current Record Flag
- Version Number

This enables historical reporting such as:

- Department changes over time
- Salary progression
- Manager changes
- Job role evolution
- Organisational restructuring

---

## Benefits

Adopting these enhancements will provide:

- Enterprise-grade dimensional modelling
- Complete historical tracking
- Simplified analytical reporting
- Faster Power BI performance
- Better support for dbt transformations
- Improved scalability for future machine learning workloads
- Alignment with modern Lakehouse architecture and industry best practices

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
