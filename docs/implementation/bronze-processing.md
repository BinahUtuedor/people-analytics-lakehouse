# Bronze Processing Implementation

## Status

The portable Bronze code foundation is implemented. The complete Bronze suite,
including real Parquet source-file lineage and duplicate-safe physical Parquet
publication, is verified in the Linux Docker Spark test runtime. Native Windows
remains suitable for ordinary development and in-memory Spark work, but its
Hadoop local filesystem requires Windows-native support for these physical
writes. The project deliberately does not distribute unofficial `winutils.exe`
binaries. No live Amazon S3 integration run has been performed.

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

The production job requires an explicit table and shared batch ID. Batch ID and
table extraction ID are parsed and validated independently. Ambiguous or
incomplete Raw partitions fail before Spark reads the dataset.

The reader captures physical file provenance with Spark `input_file_name()` and
adds `_source_file` during the Raw read. The transformer preserves that value;
it does not reconstruct file provenance later.

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

After local Spark DataFrame tests pass and a live S3 run is approved, the first
`business_units` command is:

```powershell
.\venv\Scripts\spark-submit.cmd --master "local[*]" spark\bronze\job.py --table business_units --batch-id <validated-raw-batch-id>
```

The batch ID must identify an existing Raw batch that passed Raw validation and
was uploaded successfully. This command has not yet been run against Amazon S3.
