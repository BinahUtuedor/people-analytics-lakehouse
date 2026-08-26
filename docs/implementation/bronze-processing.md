# Bronze Processing Implementation

## Status

The portable Bronze code foundation is implemented. The complete Bronze suite,
including real Parquet source-file lineage and duplicate-safe physical Parquet
publication, is verified in the Linux Docker Spark test runtime. One controlled
live Amazon S3 integration has also succeeded for all 17 supported datasets in
batch `fc4e3604-70f2-43f8-96ff-419e9d3046e5`, with 885,037 Raw rows
reconciled to 885,037 Bronze rows. Native Windows
remains suitable for ordinary development and in-memory Spark work, but its
Hadoop local filesystem requires Windows-native support for these physical
writes. The project deliberately does not distribute unofficial `winutils.exe`
binaries.

Amazon EMR execution, Lambda orchestration, Silver and Gold processing remain
planned or deferred.

## Processing Flow

```text
Validated Amazon S3 Raw batch
    ↓
spark/bronze/reader.py
    ↓
spark/bronze/transform.py
    ↓
spark/bronze/validate.py
    ↓
spark/bronze/writer.py
    ↓
Amazon S3 Bronze
```

`spark/bronze/job.py` coordinates these modules. It is intentionally separate
from `main.py`, `raw-pipeline` and `full-refresh`.

## Raw Batch and Lineage Contract

The reader preserves the existing Raw layout:

```text
raw/postgresql/<table>/
    extraction_date=<YYYY-MM-DD>/
        batch_id=<batch-id>/
            extraction_id=<extraction-id>/
                part-*.parquet
```

The production job requires an explicit shared batch ID plus either one table
or `--all-tables`. Batch ID and
table extraction ID are parsed and validated independently. Ambiguous or
incomplete Raw partitions fail before Spark reads the dataset.

The reader captures physical file provenance with Spark `input_file_name()` and
adds `_source_file` during the Raw read. The transformer preserves that value;
it does not reconstruct file provenance later.

Raw Parquet written by pandas/PyArrow can contain `TIMESTAMP(NANOS)` and `TIME`
logical types that Spark 4.2 cannot infer. The reader inspects Parquet footers,
converts only confirmed nanosecond timestamps to Spark's microsecond timestamp
precision, and represents confirmed time-of-day values as canonical
`HH:mm:ss.SSSSSS` strings. Immutable Raw objects are not rewritten.

## Bronze Metadata and Record Hash

The transformer preserves every Raw column and adds:

- `_bronze_ingested_at` as a UTC Spark timestamp;
- `_extraction_date` from the resolved Raw partition;
- `_record_hash` as a SHA-256 value.

The record hash uses only columns whose names do not begin with `_`. Column
names are sorted lexicographically, values are encoded with
`to_json(struct(...))`, null fields are retained, and Spark's built-in
`sha2(..., 256)` function produces the hash. Python UDFs, pandas conversion and
driver-side record loops are not used.

Because ingestion and batch metadata columns begin with `_`, changing only that
metadata does not change the business-record hash.

## Validation and Reconciliation

Before publication, Bronze validation checks:

- preservation of Raw columns and their data types;
- presence and non-nullness of required lineage and Bronze metadata;
- source system, schema, table, batch, extraction and extraction-date values;
- lowercase 64-character hexadecimal record hashes;
- populated physical source-file lineage;
- exact Raw-to-Bronze row-count reconciliation, including zero-row datasets.

Validation failures raise `BronzeValidationError` and prevent the writer from
being invoked. Validation uses Spark counts and bounded invalid-row checks; it
does not collect a complete dataset.

## Duplicate-Safe Publication

The writer publishes Snappy-compressed Parquet to:

```text
s3a://<bucket>/<bronze-prefix>/postgresql/<table>/
    extraction_date=<YYYY-MM-DD>/
        batch_id=<batch-id>/
```

Spark write mode is `errorifexists`:

- the first submission succeeds when the batch path is absent;
- submitting the same batch again fails explicitly;
- no append, automatic deletion or replacement occurs;
- duplicate records are not published into an existing batch.

This is duplicate-safe batch publication, not true idempotent replacement.
Automatic cleanup, overwrite and retry/replacement semantics are deferred.

For controlled resume, `--verify-existing` reads and fully validates an existing
Bronze partition against Raw before continuing. It checks deterministic hash
content as well as hash format. Missing partitions are processed normally.

## Configuration and Portability

`SPARK_MASTER` is an optional local override. When it is unset, the Spark
session inherits its master from `spark-submit` or a future managed runtime.
AWS credentials remain in the standard AWS credential-provider chain and are
not copied into Spark configuration.

Local S3A access may require `SPARK_JARS_PACKAGES` with a `hadoop-aws` version
that exactly matches the Hadoop version bundled with the local Spark release.

The reproducible integration-test service uses Python 3.12.10, Eclipse Temurin
JDK 21 and pinned PySpark 4.2.0 on Linux. This test-only runtime keeps Windows
filesystem concerns out of production code and provides closer operating-system
parity with future Amazon EMR execution. See
`docs/development/spark-local-development.md` for commands and prerequisites.

## Commands

Syntax and unit-test commands are:

```powershell
python -m compileall config spark tests
python -m unittest discover -s tests/spark/bronze -p "test_*.py" -v
python main.py --help
```

The authoritative complete Bronze suite is:

```powershell
docker compose run --rm --build spark-tests
```

Process a new full Raw batch:

```powershell
docker compose run --rm spark-bronze --all-tables --batch-id <validated-raw-batch-id>
```

Verify or resume a batch without blindly skipping existing output:

```powershell
docker compose run --rm spark-bronze --all-tables --batch-id <validated-raw-batch-id> --verify-existing
```

The first `business_units` run was executed in the supported Linux Docker Spark
runtime with the repository mounted at `/workspace`, `PYTHONPATH=/workspace`,
and the Hadoop-compatible S3A package supplied through
`SPARK_JARS_PACKAGES`. A reusable operational wrapper is deferred until the
manual Amazon EMR packaging milestone.

The live command shape was:

```powershell
docker compose run --rm `
  -e PYTHONPATH=/workspace `
  -e SPARK_JARS_PACKAGES=org.apache.hadoop:hadoop-aws:<matching-version> `
  spark-tests python spark/bronze/job.py `
  --table business_units `
  --batch-id <validated-raw-batch-id>
```

The batch ID must identify an existing Raw batch that passed Raw validation and
was uploaded successfully. Native Windows `spark-submit` remains unsuitable for
this physical integration because the project does not distribute
`winutils.exe`.

See `docs/operations/end-to-end-runbook.md` for the complete clone-to-Bronze
workflow.
