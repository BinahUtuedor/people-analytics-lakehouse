# AGENTS.md

## Purpose

This file defines repository-level instructions for AI coding agents
working on the **People Analytics Lakehouse Platform**.

Agents must follow these instructions when inspecting, modifying,
testing, or documenting this repository.

The primary objective is to extend the platform safely while preserving
existing working behaviour.

------------------------------------------------------------------------

# 1. Project Overview

The People Analytics Lakehouse Platform is an end-to-end data
engineering and analytics portfolio project for generating, validating,
ingesting, transforming, governing, analysing, and securely sharing
synthetic workforce data.

The current implemented foundation includes:

-   governed YAML reference data;
-   PostgreSQL operational storage;
-   SQLAlchemy ORM models;
-   synthetic workforce simulation;
-   operational data-quality validation;
-   PostgreSQL-to-Parquet extraction;
-   Raw extraction validation and reconciliation;
-   Amazon S3 Raw upload;
-   a root CLI in `main.py`;
-   a local PySpark foundation that is being extended into Bronze
    processing.

The target analytical architecture is:

``` text
Governed Reference Data
        ↓
Synthetic HR Simulator
        ↓
PostgreSQL Operational Database
        ↓
Operational Data Quality
        ↓
Python Extraction
        ↓
Raw Parquet
        ↓
Raw Reconciliation
        ↓
Amazon S3 Raw
        ↓
PySpark Bronze
        ↓
PySpark + Spark SQL Silver
        ↓
Gold Data Products
        ↓
dbt / Power BI / Analytics / Governed API
```

The target cloud Spark runtime is **Amazon EMR**.

The target event-driven pattern is:

``` text
S3 Raw ObjectCreated
        ↓
AWS Lambda
        ↓
Amazon EMR Spark Job
        ↓
S3 Bronze
```

------------------------------------------------------------------------

# 2. Source of Truth

Before making architectural changes, inspect the relevant current files.

Use these as the primary project documentation sources where present:

``` text
README.md
docs/architecture/repository-structure.md
docs/architecture/system-architecture.md
docs/architecture/database-architecture.md
docs/architecture/data-flow.md
```

Code is the source of truth for implemented behaviour.

If documentation and implementation disagree:

1.  identify the discrepancy;
2.  do not silently redesign the system;
3.  preserve working behaviour unless the task explicitly requires a
    change;
4.  update documentation only when the implementation or approved target
    architecture justifies it.

------------------------------------------------------------------------

# 3. General Working Principles

Agents must:

-   preserve existing working functionality;
-   prefer minimal, targeted changes;
-   avoid unnecessary refactoring;
-   avoid modifying unrelated files;
-   inspect dependencies before changing public functions or interfaces;
-   maintain existing naming and project conventions where reasonable;
-   use clear Python docstrings and explanatory comments;
-   keep implementation understandable for portfolio review;
-   run relevant tests or validation commands after changes;
-   report what changed and what was tested;
-   never commit credentials, tokens, passwords, or secrets.

Do not replace working code merely because another implementation is
stylistically preferable.

Do not perform broad repository restructuring unless explicitly
requested.

------------------------------------------------------------------------

# 4. Change Workflow

Before implementing a task:

1.  inspect the relevant existing files;
2.  inspect direct callers and dependencies;
3.  identify downstream impact;
4.  determine the smallest safe implementation;
5.  implement only the requested scope;
6.  run targeted tests;
7.  run relevant regression checks;
8.  summarise files changed, behaviour changed, and validation results.

If a required file is unavailable or its contents are unknown, request
or inspect it rather than inventing an incompatible interface.

------------------------------------------------------------------------

# 5. Architecture Boundaries

## PostgreSQL

PostgreSQL is the operational HR source system.

It owns:

-   reference-data persistence;
-   employee current state;
-   workforce transactions;
-   workforce lifecycle events;
-   operational relationships.

PostgreSQL is not the analytical warehouse.

Do not move reporting or lakehouse responsibilities back into PostgreSQL
without an explicit architectural decision.

------------------------------------------------------------------------

## Amazon S3

Amazon S3 is the durable analytical storage layer.

Target layout:

``` text
s3://<bucket>/
├── raw/
├── bronze/
├── silver/
└── gold/
```

Do not collapse these layers into one storage location.

------------------------------------------------------------------------

## Apache Spark / PySpark

PySpark and Spark SQL own distributed Bronze and Silver processing.

Spark transformation code must remain portable between:

``` text
local development
        ↓
spark-submit
        ↓
Amazon EMR
```

Avoid vendor-specific Spark APIs unless explicitly approved.

Do not introduce Databricks-specific dependencies, APIs, configuration,
Unity Catalog assumptions, or notebook-only implementation patterns.

------------------------------------------------------------------------

## Amazon EMR

Amazon EMR is the target managed Spark execution environment.

EMR provides compute, not permanent analytical storage.

Persistent datasets belong in Amazon S3.

A Spark job should be proven locally before cloud orchestration is
added.

------------------------------------------------------------------------

## AWS Lambda

Lambda is intended for lightweight event-driven orchestration and
control-plane logic.

Do not place substantial data transformation logic inside Lambda.

The intended responsibility is approximately:

``` text
Receive S3 event
    ↓
Validate event/batch
    ↓
Determine Spark job parameters
    ↓
Submit EMR job
```

Transformation logic belongs in `spark/`.

------------------------------------------------------------------------

## dbt

dbt is intended for selected Gold analytical models, SQL dependencies,
tests, documentation, and reusable reporting logic.

dbt does not replace PySpark Bronze or Silver processing.

------------------------------------------------------------------------

## FastAPI

FastAPI is the planned governed data-sharing interface.

It should expose approved curated data products.

Do not expose Raw operational tables, unrestricted Bronze data, or
direct PostgreSQL access to third-party consumers.

------------------------------------------------------------------------

# 6. Medallion Layer Contracts

## Raw

Raw data is:

-   immutable;
-   source-aligned;
-   batch-traceable;
-   replayable;
-   minimally transformed.

Raw extraction must preserve source meaning.

Do not add business transformations to Raw.

Typical flow:

``` text
PostgreSQL
    ↓
Parquet
    ↓
Raw Validation
    ↓
Amazon S3 Raw
```

------------------------------------------------------------------------

## Bronze

Bronze is source-conformed analytical data with technical lineage.

Bronze should:

-   preserve source business values;
-   retain source columns unless there is a justified technical reason
    not to;
-   add ingestion metadata;
-   add source metadata;
-   add batch lineage;
-   support record hashing;
-   perform structural validation;
-   remain replayable from Raw.

Typical technical metadata may include:

``` text
_bronze_ingested_at
_source_system
_source_table
_source_file
_batch_id
_extraction_date
_record_hash
```

Bronze should avoid significant business transformations.

------------------------------------------------------------------------

## Silver

Silver creates trusted analytical entities.

Silver may perform:

-   explicit schema enforcement;
-   type standardisation;
-   deduplication;
-   null handling;
-   reference-data conformity;
-   validated joins;
-   business-rule enforcement;
-   effective-date logic;
-   key standardisation;
-   integrated entity construction.

Business cleansing belongs here rather than in Bronze.

------------------------------------------------------------------------

## Gold

Gold is business-ready and data-product oriented.

Target domains are:

``` text
Gold
├── Workforce Analytics
├── Recruitment Analytics
├── Learning Analytics
├── Performance Analytics
├── Payroll Analytics
└── Attrition Analytics
```

Gold datasets should be designed for clear analytical use cases rather
than mirroring operational tables.

------------------------------------------------------------------------

# 7. Reference Data Rules

Controlled business values are governed centrally.

Current reference-data files include:

``` text
reference_data/
├── business_units.yml
├── departments.yml
├── locations.yml
├── job_roles.yml
├── attendance_statuses.yml
├── genders.yml
├── leave_types.yml
├── employment_types.yml
├── exit_reasons.yml
├── training_categories.yml
├── public_holidays.yml
└── absence_reasons.yml
```

The intended flow is:

``` text
YAML
    ↓
reference_data/loader.py
    ↓
Reference Data Validation
    ↓
database/seed.py
    ↓
PostgreSQL Reference Tables
    ↓
Simulator
```

Do not reintroduce hard-coded controlled lists into simulator modules
where governed reference data already exists.

When migrating a simulator module away from hard-coded values:

-   preserve existing generated values unless the approved reference
    data intentionally changes them;
-   preserve function signatures where possible;
-   minimise downstream changes;
-   keep existing database field semantics intact;
-   verify simulator and validation behaviour after the migration.

------------------------------------------------------------------------

# 8. Simulator Rules

The simulator represents a synthetic operational HRIS.

Important lifecycle ordering and dependencies must be preserved.

The employee population and its lifecycle events are interconnected.

Examples include:

``` text
Employees
    ↓
Recruitment
    ↓
Employee Exits
    ↓
Attendance / Leave / Payroll / Training
    ↓
Performance / Promotions / Transfers
    ↓
Surveys / Manager Feedback
    ↓
Exit Interviews
```

Do not change generation order without inspecting downstream
dependencies.

Employment-window rules must remain enforced.

Typical rule:

``` text
employee.hire_date
    <= event_date
    <= employee.termination_date
```

For active employees, today's date may be used as the upper boundary
where the existing implementation does so.

Historical records for terminated employees must not be lost simply
because `is_active` is false.

------------------------------------------------------------------------

# 9. Current-State and Event-History Rules

The project distinguishes employee current state from lifecycle event
history.

Examples:

``` text
employees
    ↔ recruitment
    ↔ promotions
    ↔ transfers
    ↔ employee_exits
```

Event tables record what happened.

`Employee` records the resulting current state.

Quality checks reconcile the two.

Do not update one side of a lifecycle event without considering the
corresponding current-state fields and validation rules.

------------------------------------------------------------------------

# 10. Data Quality Rules

Data quality is a pipeline gate, not an optional reporting feature.

Current quality areas include:

-   duplicate detection;
-   referential integrity;
-   employee hierarchy validation;
-   salary validation;
-   payroll validation;
-   employment-date validation;
-   recruitment reconciliation;
-   promotion reconciliation;
-   transfer reconciliation;
-   exit reconciliation;
-   reference-data validation;
-   Raw extraction reconciliation.

When adding a new pipeline layer, add appropriate validation before
treating the layer as complete.

Preferred pattern:

``` text
Input
    ↓
Transform
    ↓
Validate
    ↓
Write / Publish
```

Where practical, failed validation should prevent invalid data from
advancing.

------------------------------------------------------------------------

# 11. Extraction and Batch Identity

Raw extraction is batch-aware.

Preserve:

-   extraction identifiers;
-   extraction dates;
-   source table identity;
-   partitioning conventions;
-   source-to-Raw reconciliation.

Do not create unrelated batch identifiers in downstream layers when an
existing extraction or batch identity should be propagated.

Bronze should preserve enough lineage to trace records back to their Raw
batch and source.

------------------------------------------------------------------------

# 12. Spark Implementation Rules

Spark code should be modular and testable.

Preferred target structure:

``` text
spark/
├── common/
│   ├── paths.py
│   ├── metadata.py
│   └── validation.py
│
├── bronze/
│   ├── job.py
│   ├── transform.py
│   └── validate.py
│
└── silver/
    ├── job.py
    ├── transform.py
    └── validate.py
```

Use the actual repository structure if it differs; do not restructure
solely to match this example unless requested.

Spark job entry points should accept configuration externally where
practical.

Avoid hard-coding environment-specific values such as:

``` python
.master("local[*]")
```

inside production job logic.

Local Spark configuration should be supplied by configuration, CLI
arguments, environment variables, or the calling runtime.

Prefer deterministic transformation functions that can be unit tested
with small DataFrames.

Do not require an EMR environment for ordinary transformation unit
tests.

------------------------------------------------------------------------

# 13. Bronze Implementation Sequence

The immediate engineering priority is the Bronze foundation.

Use this progression:

``` text
1. Read one Raw dataset locally
2. Add Bronze technical metadata
3. Add record hash
4. Validate Bronze structure
5. Write local Bronze Parquet
6. Reconcile Raw and Bronze counts
7. Generalise across datasets
8. Expose a spark-submit-compatible job
9. Run the same job manually on Amazon EMR
10. Add event-driven S3 → Lambda → EMR orchestration
```

Do not implement Lambda orchestration before the Spark workload runs
successfully outside Lambda.

When introducing Bronze, test with a small stable dataset such as
`business_units` before generalising.

------------------------------------------------------------------------

# 14. Event-Driven and Scheduled Orchestration

The architecture supports both event-driven and scheduled workflows.

Use event-driven execution where processing should react to data
arrival.

Example:

``` text
S3 ObjectCreated
    ↓
Lambda
    ↓
EMR
```

Use scheduled orchestration for recurring or dependency-heavy activities
such as:

-   operational extraction;
-   payroll cycles;
-   periodic quality reporting;
-   Gold refreshes;
-   metadata publication;
-   maintenance workflows.

Do not force every workflow into an event-driven pattern.

------------------------------------------------------------------------

# 15. Configuration and Secrets

Environment-specific configuration belongs in environment variables or
approved configuration files.

`.env` must not be committed.

`.env.example` may be committed and should:

-   document required variables;
-   contain safe placeholders;
-   contain no real credentials;
-   explain optional values where useful.

Never expose or commit:

-   AWS access keys;
-   AWS secret keys;
-   AWS session tokens;
-   database passwords;
-   API keys;
-   private tokens;
-   secrets.

If credentials are discovered in tracked files, flag the issue
immediately.

Do not copy secrets into documentation, tests, logs, examples, or
generated files.

------------------------------------------------------------------------

# 16. AWS Rules

Use least-privilege IAM principles.

Prefer IAM roles over embedded long-lived credentials for AWS workloads.

Infrastructure changes should eventually be represented through
Terraform rather than undocumented manual configuration.

Target Terraform concerns include:

``` text
S3
IAM
Lambda
EMR
Networking
Secrets
Database infrastructure
API infrastructure
```

Do not provision unrelated AWS services without architectural
justification.

------------------------------------------------------------------------

# 17. Testing Expectations

Run the smallest relevant test set first.

Then run appropriate regression checks.

Existing project workflows may include:

``` powershell
python main.py validate
python main.py extract
python main.py validate-raw
python main.py upload-s3
python main.py raw-pipeline
python main.py full-refresh
```

Do not run destructive or cloud-cost-incurring commands automatically
unless the task requires them and the environment is clearly configured
for that action.

For Spark work, prefer:

-   unit tests for transformation functions;
-   small local DataFrames;
-   local Raw-to-Bronze integration tests;
-   count reconciliation;
-   schema assertions;
-   metadata assertions;
-   record-hash assertions.

Report commands that were not run and why.

------------------------------------------------------------------------

# 18. Backward Compatibility

Existing working interfaces should be preserved unless the task
explicitly requires a breaking change.

Before changing:

-   function parameters;
-   ORM fields;
-   table names;
-   CLI commands;
-   environment variable names;
-   file paths;
-   Raw partition structure;
-   reference-data values;

inspect all known consumers.

If a breaking change is necessary, identify it clearly before
implementation.

------------------------------------------------------------------------

# 19. Documentation Rules

Documentation must distinguish between:

-   implemented;
-   next implementation;
-   planned target state.

Do not describe planned components as already operational.

When architecture changes, update only the documentation affected by
that change.

Primary architecture documentation should remain mutually consistent.

Do not reintroduce Databricks terminology into the current architecture.

------------------------------------------------------------------------

# 20. Repository Structure

The repository evolves incrementally.

Do not create large numbers of empty directories merely to mirror a
target architecture diagram.

Create new components when implementation reaches them.

The current core includes approximately:

``` text
config/
reference_data/
database/
simulator/
quality/
etl/
data/
main.py
```

The next major implementation area is the Spark Bronze layer.

Future areas include:

``` text
spark/
dbt/
orchestration/
metadata/
catalogue/
data_sharing/
api/
analytics/
dashboards/
terraform/
.github/workflows/
```

Use the actual repository contents as the source of truth before adding
or moving files.

------------------------------------------------------------------------

# 21. Coding Style

For Python:

-   use type hints where practical;
-   use descriptive names;
-   use docstrings for modules and non-trivial functions;
-   add comments that explain business or architectural reasoning;
-   avoid comments that merely restate obvious syntax;
-   prefer small focused functions;
-   preserve established formatting conventions;
-   keep imports organised;
-   avoid unnecessary dependencies.

For Markdown:

-   use normal Markdown syntax;
-   do not add unnecessary backslash escape characters;
-   keep diagrams readable in plain text;
-   keep headings descriptive.

For SQL:

-   favour readable, explicit transformations;
-   document non-obvious business rules;
-   avoid embedding environment-specific identifiers where configuration
    can be used.

------------------------------------------------------------------------

# 22. Dependency Management

Do not add a package unless it is necessary for the requested
implementation.

Before adding a dependency:

1.  check whether the repository already provides the capability;
2.  consider standard-library or existing-package alternatives;
3.  confirm compatibility with the current Python/Spark environment;
4.  update dependency files deliberately;
5.  explain why the dependency is needed.

Do not upgrade unrelated packages as part of a feature task.

------------------------------------------------------------------------

# 23. Performance and Scale

Avoid premature optimisation, but do not introduce patterns that
obviously prevent distributed processing.

For Spark:

-   avoid unnecessary `collect()` on large datasets;
-   avoid converting large Spark DataFrames to pandas;
-   avoid Python loops over distributed records;
-   prefer built-in Spark functions;
-   use Spark SQL or DataFrame operations for scalable transformations;
-   minimise unnecessary shuffles;
-   avoid repartitioning without a reason.

Optimisation should follow evidence from actual workloads.

------------------------------------------------------------------------

# 24. Observability

New pipeline stages should provide useful operational logging.

Log information such as:

-   dataset;
-   batch ID;
-   input path;
-   output path;
-   input count where practical;
-   output count where practical;
-   validation result;
-   elapsed stage information where useful.

Do not log secrets or sensitive credential material.

------------------------------------------------------------------------

# 25. Failure Handling

Pipeline failures should be explicit.

Do not silently swallow exceptions that indicate:

-   invalid reference data;
-   failed quality checks;
-   missing required input;
-   schema incompatibility;
-   failed reconciliation;
-   failed writes;
-   invalid configuration.

Rollback transactional database work where appropriate.

For distributed processing, fail the job when continuing would publish
invalid output.

------------------------------------------------------------------------

# 26. Git and Scope Discipline

Do not commit automatically unless explicitly instructed.

Before proposing a commit:

-   inspect changed files;
-   ensure unrelated files were not modified;
-   run relevant checks;
-   provide a concise suggested commit message.

Prefer small coherent commits.

Example:

``` text
feat: add local bronze pyspark processing
```

Do not mix broad documentation rewrites, dependency upgrades,
infrastructure changes, and transformation changes in one implementation
unless explicitly requested.

------------------------------------------------------------------------

# 27. Prohibited Architectural Drift

Unless explicitly approved, do not:

-   introduce Databricks;
-   introduce Unity Catalog;
-   introduce Delta Lake as a required dependency;
-   replace Amazon EMR with another managed Spark platform;
-   move substantial transformation logic into Lambda;
-   bypass Raw and write operational data directly to Silver or Gold;
-   expose PostgreSQL directly to external API consumers;
-   duplicate governed reference values inside simulator modules;
-   remove quality gates to simplify pipeline execution;
-   rewrite working modules without a concrete requirement;
-   introduce unnecessary microservices.

------------------------------------------------------------------------

# 28. Definition of Done

A task is complete when:

1.  the requested behaviour is implemented;
2.  existing relevant behaviour is preserved;
3.  affected tests pass;
4.  relevant validation commands pass;
5.  no secrets were introduced;
6.  documentation is updated if the change affects documented behaviour;
7.  changed files are clearly identified;
8.  any untested or deferred areas are stated explicitly.

For a new data layer, completion also requires an appropriate quality or
reconciliation check.

------------------------------------------------------------------------

# 29. Immediate Project Priority

The immediate implementation priority is:

``` text
Complete local Bronze PySpark foundation
        ↓
Validate Raw → Bronze
        ↓
Make Bronze spark-submit compatible
        ↓
Run manually on Amazon EMR
        ↓
Add S3 → Lambda → EMR orchestration
        ↓
Implement Silver
        ↓
Build Gold domain data products
```

Agents should avoid jumping ahead to later architectural layers unless
explicitly requested.

------------------------------------------------------------------------

# 30. Agent Response Expectations

When completing an implementation task, provide a concise summary
containing:

-   files inspected;
-   files changed;
-   behaviour implemented;
-   tests or commands run;
-   results;
-   any remaining risks or next step.

If asked to plan only, do not modify files.

If asked for minimal changes, treat that as a hard scope constraint.
