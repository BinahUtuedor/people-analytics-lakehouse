# Repository Structure

## Overview

The **People Analytics Lakehouse Platform** follows a modular, enterprise-grade architecture that separates operational systems, data engineering, governance, analytics, metadata management and API services into independent components.

This separation of concerns makes the platform easier to maintain, extend, test and deploy while closely mirroring the architecture used within large organisations and modern cloud data platforms.

---

# Folder Structure

```text
people-analytics-lakehouse-platform/
│
├── config/                              # Application configuration
│   ├── __init__.py
│   ├── settings.py                      # Environment configuration
│   ├── constants.py                     # Global constants
│   └── logger.py                        # Centralised logging
│
├── .github/                   
│      └── workflows/                               # CI/CD pipelines
│           ├── lint.yml
│           ├── test.yml
│           ├── security-scan.yml
│           ├── build-images.yml
│           ├── deploy-dev.yml
│           ├── deploy-staging.yml
│           ├── deploy-production.yml
│           └── terraform.yml
│
├── terraform/                            # Infrastructure as Code
│   ├── modules/
│   │    ├── networking/
│   │    ├── s3/
│   │    ├── database/
│   │    ├── compute/
│   │    ├── iam/
│   │    ├── api/
│   │    ├── monitoring/
│   │    └── secrets/
│   │
│   ├── environments/
│   │    ├── dev/
│   │    │   ├── main.tf
│   │    │   ├── variables.tf
│   │    │   ├── outputs.tf
│   │    │   ├── providers.tf
│   │    │   └── terraform.tfvars.example
│   │    ├── staging/
│   │    └── production/
│   │
│   ├── backend.tf
│   ├── versions.tf
│   ├── main.tf
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── README.md
│
├── deployment/                    # Deployment configuration
│   ├── configs/
│   │   ├── dev.env
│   │   ├── staging.env
│   │   └── production.env
│   ├── docker/
│   │   ├── api.Dockerfile
│   │   ├── airflow.Dockerfile
│   │   └── spark.Dockerfile
│   ├── kubernetes/
│   │   ├── base/
│   │   └── overlays/
│   │       ├── dev/
│   │       ├── staging/
│   │       └── production/
│   ├── scripts/
│   │   ├── __init__.py
│   │   ├── deploy.sh
│   │   ├── rollback.sh
│   │   ├── health_check.sh
│   │   └── migrate_database.sh
│   └── README.md│
│
├── simulator/                           # Company simulation engine
│   ├── __init__.py
│   ├── attendance.py
│   ├── employees.py
│   ├── effective_dates.py 
│   ├── exit_interviews.py
│   ├── leave.py
│   ├── manager_feedback.py
│   ├── payroll.py
│   ├── performance.py
│   ├── promotion.py
│   ├── recruitment.py
│   ├── surveys.py
│   ├── training.py
│   ├── transfer.py
│   └── simulator.py                     # Simulation orchestrator
│
├── database/                            # Operational database
│   ├── __init__.py
│   ├── postgres.py
│   ├── models/
│   │    ├── __init__.py
│   │    ├── business_unit.py
│   │    ├── department.py
│   │    ├── location.py
│   │    ├── job_role.py
│   │    ├── employee.py
│   │    ├── employee_exit.py
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
│   │    ├── attendance_status.py       ← NEW
│   │    ├── gender.py                  ← NEW
│   │    ├── leave_type.py              ← NEW
│   │    ├── employment_type.py         ← NEW
│   │    ├── exit_reason.py             ← NEW
│   │    ├── training_category.py       ← NEW
│   │    ├── public_holiday.py          ← NEW
│   │    ├── absence_reason.py          ← NEW
│   │    └── exit_interview.py
│   ├── base.py
│   ├── connection.py
│   ├── create_schema.py
│   └── seed.py
│
├── integrations/                       # Inbound external APIs
│   ├── __init__.py
│   ├── base_client.py
│   ├── holidays_api.py
│   ├── exchange_rates_api.py
│   ├── labour_market_api.py
│   ├── learning_api.py
│   ├── geocoding_api.py
│   ├── weather_api.py
│   ├── schemas.py
│   └── exceptions.py
│
├── api/                                # Outbound HTTP API
│   ├── __init__.py
│   │
│   ├── config/
│   │    ├── __init__.py
│   │    └── api_settings.py
│   │
│   ├── openapi/
│   │    └── openapi.json
│   │
│   ├── v1/
│   │    ├── routes/
│   │    │    ├── __init__.py
│   │    │    ├── health.py
│   │    │    ├── workforce.py
│   │    │    ├── attendance.py
│   │    │    ├── payroll.py
│   │    │    ├── recruitment.py
│   │    │    └── catalogue.py
│   │    │
│   │    ├── schemas/
│   │    │    ├── __init__.py
│   │    │    ├── workforce.py
│   │    │    ├── attendance.py
│   │    │    ├── payroll.py
│   │    │    ├── recruitment.py
│   │    │    └── common.py
│   │    │
│   │    ├── services/
│   │    │    ├── __init__.py
│   │    │    ├── workforce_service.py
│   │    │    ├── attendance_service.py
│   │    │    ├── payroll_service.py
│   │    │    └── export_service.py
│   │    │
│   │    ├── repositories/
│   │    │    ├── __init__.py
│   │    │    ├── workforce_repository.py
│   │    │    ├── attendance_repository.py
│   │    │    ├── payroll_repository.py
│   │    │    └── recruitment_repository.py
│   │    │
│   │    ├── security/
│   │    │    ├── __init__.py
│   │    │    ├── authentication.py
│   │    │    ├── authorization.py
│   │    │    ├── api_keys.py
│   │    │    ├── permissions.py
│   │    │    ├── password_hashing.py
│   │    │    └── rate_limiting.py
│   │    │
│   │    └── middleware/
│   │         ├── __init__.py
│   │         ├── audit_logging.py
│   │         ├── request_id.py
│   │         └── security_headers.py
│   ├── exceptions.py
│   ├── dependencies.py
│   └── main.py
│
├── data_sharing/                           # API governance database layer
│   ├── __init__.py
│   ├── base.py
│   ├── models/
│   │    ├── __init__.py
│   │    ├── api_consumer.py
│   │    ├── api_key.py
│   │    ├── access_policy.py
│   │    ├── data_product.py
│   │    ├── access_log.py
│   │    └── export_request.py
│   ├── create_schema.py
│   ├── seed.py
│   ├── repositories.py
│   └── services.py             
│
├── shared/
│   ├── __init__.py
│   ├── exceptions.py
│   ├── enums.py
│   ├── validators.py
│   ├── serializers.py
│   ├── helpers.py
│   └── decorators.py                    
│
├── quality/                             # Data quality framework
│   ├── __init__.py
│   ├── validation.py
│   ├── exceptions.py
│   ├── expectations.py
│   ├── duplicate_checks.py
│   ├── integrity_checks.py
│   ├── metrics.py
│   ├── reference_data_checks.py
│   ├── report.py
│   ├── validate_promotion_salary.py
│   ├── workforce_lifecycle_checks.py
│   ├── metadata_checks.py
│   ├── reference_data_checks.py
│   └── business_rules.py
│
├── etl/                                 # ETL pipelines
│   ├── __init__.py
│   ├── extract.py
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
│   │
│   ├── docs/
│   │   ├── exposures.yml
│   │   ├── metrics.yml
│   │   ├── groups.yml
│   │   └── sources.yml
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
│   │   ├── ingest_reference_data.py
│   │   ├── ingest_external_apis.py
│   │   ├── refresh_catalogue.py
│   │   ├── generate_operational_data.py
│   │   ├── validate_operational_data.py
│   │   ├── export_postgres_to_s3.py
│   │   ├── process_bronze.py
│   │   ├── process_silver.py
│   │   ├── process_gold.py
│   │   ├── run_dbt_models.py
│   │   └── publish_metadata.py
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
│   ├── quality/
│   ├── reference_data/
│   ├── api/
│   ├── data_sharing/
│   ├── metadata/
│   ├── catalogue/
│   ├── integrations/ 
│   ├── etl/
│   ├── analytics/
│   └── database/
│
├── logs/
│
├── docker/
│   └── local/
│        ├── postgres/
│        ├── airflow/
│        └── spark/
│
├── reference_data/                            
│   ├── __init__.py
│   ├── business_units.yml
│   ├── departments.yml
│   ├── locations.yml
│   ├── job_roles.yml
│   ├── attendance_statuses.yml         # NEW
│   ├── genders.yml                     # NEW
│   ├── leave_types.yml                 # NEW
│   ├── employment_types.yml            # NEW
│   ├── exit_reasons.yml                # NEW
│   ├── training_categories.yml         # NEW
│   ├── public_holidays.yml             # NEW
│   ├── absence_reasons.yml             # NEW
│   └── loader.py
│
├── scripts/                            
│   ├── setup_local.py
│   ├── initialise_database.py
│   ├── full_refresh.py 
│   ├── simulate.py  
│   ├── validate.py
│   ├── build_platform.py 
│   ├── deploy.py
│   ├── seed_reference_data.py
│   ├── run_quality_checks.py
│   ├── build_catalogue.py
│   ├── publish_metadata.py
│   └── generate_documentation.py
│
├── monitoring/
│   ├── __init__.py                            
│   ├── prometheus/
│   │    └── prometheus.yml
│   ├── grafana/
│   │    ├── dashboards/
│   │    └── datasources/
│   ├── alerts/
│   └── dashboards/
│
├── metadata/                          
│   ├── __init__.py
│   ├── schemas
│   │    ├── business_units.yml
│   │    ├── departments.yml
│   │    ├── locations.yml
│   │    ├── job_roles.yml
│   │    ├── employees.yml
│   │    ├── attendance.yml
│   │    ├── payroll.yml
│   │    ├── leave_requests.yml
│   │    ├── recruitment.yml
│   │    ├── promotions.yml
│   │    ├── transfers.yml
│   │    ├── training.yml
│   │    ├── performance_reviews.yml
│   │    ├── employee_surveys.yml
│   │    ├── manager_feedback.yml
│   │    └── exit_interviews.yml
│   │
│   ├── lineage/
│   │    ├── source_to_bronze.yml
│   │    ├── bronze_to_silver.yml
│   │    ├── silver_to_gold.yml
│   │    ├── gold_to_dbt.yml
│   │    ├── dbt_to_dashboard.yml
│   │    └── dbt_to_api.yml
│   ├── ownership.yml
│   ├── classifications.yml
│   ├── glossary.yml
│   └── loader.py
│
├── catalogue/                    # Metadata catalogue database layer
│   ├── __init__.py
│   ├── base.py
│   ├── register_assets.py
│   ├── register_columns.py
│   ├── register_lineage.py
│   ├── register_quality_results.py
│   ├── sync_dbt_metadata.py
│   ├── sync_postgres_metadata.py
│   ├── models.py
│   ├── create_catalogue_schema.py
│   ├── report.py
│   └── catalogue_config.yml
│
├── docs/                   
│   ├── README.md                         # Documentation index
│   ├── architecture/
│   │    ├── repository-structure.md
│   │    ├── system-architecture.md
│   │    ├── database-architecture.md
│   │    ├── data-flow.md
│   │    ├── deployment-architecture.md
│   │    └── technology-stack.md
│   │
│   ├── data-governance/
│   │    ├── data-dictionary.md
│   │    ├── data-lineage.md
│   │    ├── metadata-framework.md
│   │    ├── data-catalogue.md
│   │    ├── data-quality-framework.md
│   │    ├── reference-data.md
│   │    ├── security-and-data-sharing.md
│   │    └── governance-overview.md
│   │
│   ├── implementation/
│   │    ├── local-development.md
│   │    ├── docker.md
│   │    ├── postgres.md
│   │    ├── simulator.md
│   │    ├── integrations.md
│   │    ├── etl.md
│   │    ├── spark.md
│   │    ├── dbt.md
│   │    ├── airflow.md
│   │    ├── api.md
│   │    ├── data-sharing.md
│   │    ├── api-reference.md
│   │    ├── authentication.md
│   │    ├── rbac.md
│   │    ├── rate-limiting.md
│   │    ├── dependencies.md
│   │    └── deployment.md
│   │
│   ├── decisions/
│   │    ├── adr-001-layered-architecture.md
│   │    ├── adr-002-reference-data.md
│   │    ├── adr-003-metadata-framework.md
│   │    ├── adr-004-data-catalogue.md
│   │    ├── adr-005-data-sharing.md
│   │    ├── adr-006-lakehouse-architecture.md
│   │    └── adr-007-api-design.md
│   │
│   ├── diagrams/
│   │    ├── repository-structure.mmd
│   │    ├── system-architecture.mmd
│   │    ├── data-flow.mmd
│   │    ├── database-architecture.mmd
│   │    ├── etl-pipeline.mmd
│   │    ├── metadata-flow.mmd
│   │    ├── api-architecture.mmd
│   │    └── airflow-pipeline.mmd
│   │
│   ├── images/
│   │    ├── architecture/
│   │    ├── database/
│   │    ├── pipeline/
│   │    ├── dashboards/
│   │    ├── metadata/
│   │    ├── screenshots/
│   │    └── logos/
│   │
│   └── templates/
│       ├── metadata-template.yml
│       ├── data-dictionary-template.md
│       ├── lineage-template.md
│       └── adr-template.md
├── releases/
│   ├── v1.0.0.md
│   └── roadmap.md
├── .env
├── .gitignore
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── pyproject.toml
├── main.py
├── LICENSE
├── CHANGELOG.md
├── CONTRIBUTING.md
└── README.md 
```

## Top-Level Modules

The repository is organised into logical modules that separate operational data generation, data engineering, analytics engineering, governance, infrastructure and application services. Each top-level folder has a clearly defined responsibility, promoting maintainability, scalability and separation of concerns.

| Folder | Purpose |
|---------|----------|
| **config/** | Central application configuration including environment variables, global constants, application settings and logging. All modules consume configuration from this package to ensure consistency across the platform. |
| **.github/** | GitHub Actions workflows providing Continuous Integration (CI), automated testing, security scanning, infrastructure validation and Continuous Deployment (CD) pipelines. |
| **terraform/** | Infrastructure as Code (IaC) defining cloud resources required by the platform, including networking, storage, compute, IAM, secrets management, APIs and monitoring across development, staging and production environments. |
| **deployment/** | Production deployment configuration including Docker images, Kubernetes manifests, Helm charts, environment-specific configuration, deployment scripts, rollback procedures and database migration utilities. |
| **simulator/** | Generates realistic synthetic HR operational data including employees, attendance, payroll, leave, recruitment, promotions, transfers, training, performance reviews, employee surveys, manager feedback and exit interviews. Simulates a complete enterprise Human Resources Information System (HRIS). |
| **database/** | SQLAlchemy ORM models, PostgreSQL connection management, schema creation and reference data seeding. Represents the operational transactional database used by the organisation. |
| **integrations/** | Connectors for consuming third-party APIs including public holidays, labour market statistics, exchange rates, weather, geocoding and learning providers to enrich internally generated workforce data. |
| **api/** | FastAPI application exposing secure REST endpoints that allow approved internal and external consumers to access curated workforce data products, analytical datasets and metadata. |
| **data_sharing/** | Database models and business services supporting secure external data sharing, including API consumers, API keys, access policies, export requests, audit logging and data product governance. |
| **shared/** | Reusable components shared across the platform including helper functions, decorators, custom exceptions, enumerations, serializers, validators and common utilities that promote code reuse and consistency. |
| **quality/** | Enterprise data quality framework responsible for business rule validation, referential integrity checks, duplicate detection, metadata validation, reference data validation, quality metrics and automated quality reporting. |
| **etl/** | Extract, Transform and Load pipelines responsible for extracting operational data, preparing datasets for the Lakehouse and orchestrating movement between PostgreSQL, AWS S3 and downstream processing layers. |
| **spark/** | PySpark processing layer implementing scalable Bronze, Silver and Gold transformations over large datasets using a modern Medallion Architecture. |
| **analytics/** | Advanced analytics and machine learning including workforce forecasting, attrition prediction, burnout prediction, promotion prediction, sentiment analysis and topic modelling. |
| **dbt/** | Analytics engineering project responsible for staging, intermediate transformations, dimensional modelling, business marts and reporting datasets consumed by Power BI, machine learning and APIs. |
| **airflow/** | Workflow orchestration platform responsible for scheduling simulation, validation, ETL, metadata publication, API ingestion, Lakehouse processing and analytical model execution. |
| **dashboards/** | Power BI reports, semantic datasets and dashboard screenshots demonstrating workforce analytics, executive reporting and business intelligence capabilities. |
| **notebooks/** | Jupyter notebooks used for exploratory data analysis, experimentation, feature engineering, machine learning development and research activities. |
| **data/** | Local development storage containing generated datasets together with Raw, Bronze, Silver and Gold files used during development before cloud storage. |
| **sql/** | SQL scripts supporting PostgreSQL administration, Databricks processing, reporting queries and database utilities. |
| **tests/** | Comprehensive automated testing covering simulation, ETL, APIs, integrations, metadata, catalogue management, analytics, database components, quality validation and infrastructure. |
| **logs/** | Centralised application logs generated during simulation, validation, ETL execution, orchestration workflows, API operations and platform monitoring. |
| **docker/** | Docker configuration supporting local development services including PostgreSQL, pgAdmin, Apache Airflow, Spark and supporting infrastructure. |
| **scripts/** | Developer automation scripts supporting local setup, database initialisation, full platform refresh, simulation execution, metadata publication, catalogue generation, documentation generation and deployment automation. |
| **monitoring/** | Platform observability components including Prometheus configuration, Grafana dashboards, metrics collection, alert definitions and operational monitoring used to track platform health, reliability and performance. |
| **reference_data/** | Centralised reusable reference datasets stored as YAML, including business units, departments, locations, leave types, employment types, job roles, exit reasons, training categories and public holidays. Acts as the application's master reference data source. |
| **metadata/** | Business and technical metadata including dataset schemas, ownership, lineage, glossary definitions, classifications and metadata loaders supporting enterprise data governance. |
| **catalogue/** | Enterprise metadata catalogue responsible for registering datasets, columns, lineage, ownership and quality metrics while synchronising metadata from PostgreSQL, dbt and platform components. |
| **docs/** | Comprehensive project documentation including architecture, implementation guides, governance documentation, data dictionaries, lineage, standards, design decisions, templates and operational documentation. |
| **releases/** | Release management documentation containing version histories, release notes, deployment milestones, feature summaries and the long-term product roadmap, providing traceability of platform evolution over time. |

---

# Root Configuration Files

| File | Purpose |
|------|----------|
| **.env** | Environment-specific configuration including database credentials, cloud configuration and API settings. |
| **.gitignore** | Specifies files and directories excluded from Git version control. |
| **docker-compose.yml** | Defines the complete local development environment including PostgreSQL, pgAdmin, Airflow, Spark and API services. |
| **requirements.txt** | Python package dependencies required to run the application. |
| **pyproject.toml** | Project metadata, dependency management and development tooling configuration. |
| **main.py** | Primary application entry point used during local development. |
| **LICENSE** | Open-source licence governing project usage. |
| **README.md** | High-level project overview, installation guide and links to detailed documentation. |

---

# Repository Layers

The repository follows a layered architecture inspired by enterprise HR platforms and modern cloud data engineering solutions.

```text
External APIs
        │
        ▼
Inbound Integrations
        │
        ▼
Operational Simulation
        │
        ▼
PostgreSQL Operational Database
        │
        ▼
Data Quality Validation
        │
        ▼
ETL Pipelines
        │
        ▼
AWS S3 Data Lake
        │
        ▼
Bronze Layer
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
        ▼
Metadata & Data Catalogue
        │
        ▼
Machine Learning & Analytics
        │
        ▼
Power BI Dashboards
        │
        ▼
Secure Data Sharing API
```

---


