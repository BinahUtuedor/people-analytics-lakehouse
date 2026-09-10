# Silver Processing Runbook

Silver reads only validated Bronze partitions, standardises analytical types,
trims blank strings to null, removes exact duplicate record hashes using stable
Bronze metadata ordering, validates lineage and employee references, then writes
duplicate-safe Parquet. EMR execution is deferred. The Silver implementation is
nevertheless designed to be EMR-compatible.

## Local/Docker execution

### Command
`docker compose run --rm spark-silver --all-tables --batch-id <batch-id>`

### Prerequisites
An approved AWS credential chain, S3A connector, and an existing complete Bronze batch.

### What happens internally
The job resolves each exact Bronze partition, transforms and validates it, then
publishes only absent Silver partitions.

### Input
`s3://<bucket>/<bronze-prefix>/postgresql/<dataset>/extraction_date=<date>/batch_id=<batch-id>/`

### Output
`s3://<bucket>/<silver-prefix>/postgresql/<dataset>/extraction_date=<date>/batch_id=<batch-id>/`

### Side effects
Writes new Silver Parquet and `_SUCCESS`; never overwrites, appends, or deletes.

### Success condition
All selected datasets pass validation and write successfully.

Use `--table employees` for one dataset. Use `--verify-existing` to revalidate
an existing partition without publication. If a prior run is partial, rerun
`--all-tables --verify-existing`; absent partitions publish and existing ones
are validated. Investigate any failed partition rather than deleting it.

## Troubleshooting

Missing Bronze partitions, ambiguous batch identity, invalid lineage, duplicate
hashes after transformation, and orphan employee references fail explicitly.
Run `docker compose run --rm --build spark-tests` before any live S3 action.
The baseline batch `fc4e3604-70f2-43f8-96ff-419e9d3046e5` must only be run with
explicit approval because a Silver publication is an S3 data mutation.

The Docker Spark image resolves the S3A artifacts through Ivy. The required
artifacts were already present in the host Ivy cache, but mounting that cache
read-only prevented Ivy from writing its resolved-module descriptor. For local
Docker execution, mount the existing cache read-write when dependency
resolution is required. This is a local Docker/Ivy startup concern; it does
not require a Silver implementation change or affect the EMR design.
