# Spark Local Development

## Supported Runtime Model

The Bronze Spark code remains platform-neutral and is intended to run through
`spark-submit` locally and on a future Amazon EMR Linux runtime. Development is
split across two complementary environments:

```text
Native Windows development
        ↓
Linux Docker Spark integration tests
        ↓
Future Amazon EMR execution
```

Native Windows supports ordinary Python development, mocked AWS and
infrastructure tests, and in-memory Spark transformation tests. The complete
Bronze suite runs in Linux Docker because two tests deliberately exercise
physical Hadoop-backed Parquet reads and writes.

Hadoop's local filesystem implementation expects Windows-native Hadoop support
for some permission operations. The project does not distribute or recommend
unofficial `winutils.exe` binaries, and production Spark code contains no
Windows-specific workaround. This is a local test-runtime constraint, not a
production limitation.

## Version Contract

The Docker integration-test image deliberately uses:

- Python 3.12.10;
- Eclipse Temurin JDK 21.0.8+9;
- PySpark 4.2.0.

`PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` both resolve to the container's
Python 3.12 interpreter. The minimal test-image dependencies are locked in
`requirements-spark-tests.txt`; the main project dependency also pins PySpark
4.2.0 so local installations do not silently select a different Spark/Hadoop
runtime.

The future EMR release must be selected and compatibility-tested separately.
This local image does not claim that an EMR deployment has been implemented.

## Prerequisites

Install Docker Desktop with Linux containers and Docker Compose. No PostgreSQL
container, AWS credentials, S3 bucket, or `HADOOP_HOME` is required for the
Bronze suite. Existing tests continue to mock AWS where intended.

## Run the Complete Bronze Suite

From the repository root:

```powershell
docker compose run --rm --build spark-tests
```

After the image has been built and neither the Dockerfile nor dependency lock
has changed, the shorter repeat command is:

```powershell
docker compose run --rm spark-tests
```

The service runs:

```text
python -m unittest discover -s tests/spark/bronze -p "test_*.py" -v
```

The source tree is bind-mounted for rapid test iteration. Test temporary
directories are created inside the Linux container, so physical Parquet tests
use Linux filesystem semantics. The service is development/test infrastructure
only and does not alter the existing PostgreSQL or pgAdmin services.

## Native Windows Checks

Useful non-mutating checks remain available directly in the project virtual
environment:

```powershell
python -m compileall config spark tests
python main.py --help
python -m spark.bronze.job --help
```

Most Bronze tests also run natively. Use the Docker command as the authoritative
complete suite because it includes real source-file lineage and duplicate-safe
physical Parquet publication.

Live S3 Raw-to-Bronze integration and manual Amazon EMR execution are separate,
deferred milestones and require explicit authorised cloud configuration.
