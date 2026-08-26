# Amazon EMR Bronze Manual-Execution Runbook

## Scope and Safety Boundary

This runbook prepares the existing 17-dataset Bronze application for a later
manual Amazon EMR on EC2 execution. Repository packaging and compatibility
testing are implemented. No EMR cluster, step, IAM resource, bucket, network,
or other live AWS resource has been created or invoked by this milestone.

Commands in the **Future approved execution** section are templates only. Do
not run them until the owner explicitly approves the cost and live AWS changes.

## Validated Application Baseline

- Entry point: `spark/bronze/job.py`
- Batch argument: explicit shared Raw `--batch-id`
- Scope argument: `--all-tables` or one `--table`
- Retry mode: `--verify-existing`
- Local authoritative runtime: Python 3.12, Java 21, PySpark 4.2.0
- Initial EMR compatibility target: `emr-7.13.0`, Python 3.11, Java 17,
  Spark 3.5.6

The target follows the current Amazon EMR 7.x release documentation. Confirm
release availability in the intended AWS Region immediately before a live run.
The local compatibility container uses upstream PySpark 3.5.6; it is a strong
preflight check but cannot reproduce Amazon's patched runtime exactly.

## 1. Local Non-Live Validation

```powershell
docker compose run --rm --build spark-tests
docker compose run --rm --build emr-compat-tests
```

Neither service connects to AWS. The EMR compatibility service uses Python
3.11, Java 17, and PySpark 3.5.6 to detect API or serialization differences.

## 2. Build the EMR Application Bundle

```powershell
docker compose run --rm emr-compat-tests python scripts/build_emr_bundle.py
```

This produces ignored, local artifacts under `dist/emr/bronze/`:

```text
bronze-job.py
people-analytics-bronze.zip
manifest.json
```

The zip contains `config/`, `spark/`, and pinned pure-Python runtime
dependencies. It excludes PySpark because EMR supplies Spark. The manifest
records SHA-256 checksums and sizes. The builder does not call AWS, but its
dependency-install stage accesses Python package indexes. Building inside the
compatibility container ensures dependency resolution occurs on the intended
Linux/Python 3.11 platform rather than on the Windows host.

For an offline structural test without dependencies:

```powershell
python scripts/build_emr_bundle.py --skip-dependencies
```

Do not commit `dist/`. Build from the reviewed commit that will be executed,
retain its commit SHA with the manifest, and publish only through a later
approved deployment process.

## 3. Runtime Configuration Contract

The EMR driver requires these non-secret values:

| Setting | Purpose |
| --- | --- |
| `AWS_REGION` | S3 endpoint Region |
| `AWS_S3_BUCKET` or `--bucket` | Existing lake bucket |
| `AWS_S3_RAW_PREFIX` or `--raw-prefix` | Existing Raw prefix |
| `AWS_S3_BRONZE_PREFIX` or `--bronze-prefix` | Existing Bronze prefix |
| `SPARK_APP_NAME` | Optional application name |
| `SPARK_LOG_LEVEL` | Optional Spark log level |

Leave `SPARK_MASTER` and `SPARK_JARS_PACKAGES` unset on EMR. YARN supplies the
master and EMR supplies its supported S3 filesystem libraries. Never pass AWS
access keys through Spark arguments, configuration, bootstrap actions, or
logs. The job uses the EC2 instance profile credential chain.

## 4. Existing S3 Requirements

Choose explicit, existing prefixes before execution:

```text
s3://<lake-bucket>/<raw-prefix>/
s3://<lake-bucket>/<bronze-prefix>/
s3://<artifact-bucket-or-prefix>/bronze/<commit-sha>/
s3://<log-bucket-or-prefix>/emr/
```

The artifact and log locations may be prefixes in an existing approved bucket;
this milestone does not create them. Encryption keys, bucket policies, and
object ownership must permit the chosen EMR role.

## 5. Least-Privilege IAM Requirements

The EMR EC2 instance profile used by Spark needs only the applicable resources:

- list the lake bucket, restricted to Raw and Bronze prefixes;
- read Raw Parquet objects;
- read existing Bronze objects for `--verify-existing`;
- write new Bronze objects and `_SUCCESS` markers;
- read the reviewed application artifacts;
- use relevant KMS keys only when customer-managed encryption requires them.

Do not grant Bronze deletion for the first run. The application never deletes
or overwrites output. Do not grant access to Silver, Gold, unrelated buckets,
or unrelated prefixes. Review the EMR service role, EC2 instance profile, log
access, and optional KMS key policies separately before creation.

Conceptual S3 data actions are:

```text
s3:ListBucket
s3:GetObject
s3:PutObject
s3:AbortMultipartUpload
s3:ListBucketMultipartUploads
s3:ListMultipartUploadParts
```

Scope resources to the real bucket and prefixes; do not copy wildcard account
or bucket permissions from generic examples.

## 6. Network and Logging Prerequisites

Before a later cluster run, confirm:

- subnets and security groups are pre-approved;
- nodes can reach the existing S3 bucket, preferably through an S3 gateway
  endpoint where the network design supports it;
- no NAT gateway is created merely for this test;
- EMR step and Spark/YARN logs use an approved existing S3 log prefix;
- log retention and encryption meet the environment's requirements;
- cluster size and automatic termination policy are explicitly approved.

## 7. Future Approved Execution Template — Do Not Run Yet

After artifacts have been reviewed and uploaded through an approved process,
the eventual Spark step should be equivalent to:

```bash
spark-submit \
  --deploy-mode cluster \
  --py-files s3://<artifact-location>/people-analytics-bronze.zip \
  --conf spark.yarn.appMasterEnv.AWS_REGION=<region> \
  s3://<artifact-location>/bronze-job.py \
  --all-tables \
  --batch-id <validated-raw-batch-id> \
  --bucket <existing-lake-bucket> \
  --raw-prefix <raw-prefix> \
  --bronze-prefix <bronze-prefix> \
  --verify-existing
```

For the already published baseline batch, `--verify-existing` is mandatory so
the first EMR exercise is read/validation-only for existing Bronze partitions.
A publication test must use a separately approved Raw batch whose exact Bronze
paths are absent.

Live execution requires explicit approval for artifact upload, IAM and cluster
provisioning, step submission, monitoring, and termination. Those actions are
intentionally not automated in this repository milestone.

## 8. Later Acceptance Evidence

Capture the following when a live run is eventually approved:

- reviewed Git commit and artifact SHA-256 values;
- EMR release label, Region, applications, instance types, and role names;
- Spark step ID and terminal state;
- driver log location with secrets excluded;
- dataset count and Raw/Bronze reconciliation total;
- validation and publication status for every dataset;
- S3 inventory before and after the step;
- cluster termination evidence and estimated cost.

Do not claim EMR execution is verified until all evidence has been reviewed.

## 9. Stop Point

Repository preparation ends after local packaging, checksum generation,
Spark 3.5.6 compatibility tests, and documentation review. The next action is
an explicit go/no-go decision for the first billable manual EMR execution.

## Official Runtime References

- [Apache Spark versions in Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html)
- [Amazon EMR 7.x application versions](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-7.x.html)
- [IAM service roles used by Amazon EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html)
