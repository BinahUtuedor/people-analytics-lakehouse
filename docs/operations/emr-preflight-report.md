# AWS EMR Read-Only Preflight Report

## Executive Summary

This read-only preflight was performed on 2026-09-10. Repository configuration
identifies the intended lake bucket and prefixes, but live AWS environment
discovery could not be performed because the AWS CLI was not installed or
discoverable in the execution environment. No authenticated AWS API operation
succeeded and no AWS resource was changed.

Decision: **NOT READY — BLOCKERS IDENTIFIED** for infrastructure-design review.
The immediate blocker is an approved environment with the AWS CLI installed and
an authenticated, read-only AWS identity. This does not establish that any
AWS resource is missing.

## AWS Identity

Account: Not determined; `aws` executable unavailable.

Principal: Not determined; `aws` executable unavailable.

No credentials, access keys, session tokens, or environment variables
containing credentials were inspected or recorded.

## Region

Repository configuration resolves `AWS_REGION` to `eu-west-2`. This is a
repository/local configuration value, not confirmation of the AWS CLI's active
profile or the target account's selected Region.

## S3

Repository configuration identifies:

| Item | Configured value | Live AWS result |
| --- | --- | --- |
| Lake bucket | `people-analytics-lakehouse` | Not inspected |
| Raw prefix | `raw/postgresql` | Not inspected |
| Bronze prefix | `bronze` | Not inspected |
| Validated batch | `fc4e3604-70f2-43f8-96ff-419e9d3046e5` | Not inspected |
| Expected baseline | 17 datasets; 885,037 Raw and Bronze rows | Repository-validated only |

The report cannot confirm bucket existence, prefixes, Parquet objects,
`_SUCCESS` markers, batch partitions, object ownership, or the live 17-dataset
baseline. Object existence alone would not prove data integrity in any case.

## Networking

VPCs, subnets, route tables, VPC endpoints, and security groups were not
inspected because AWS CLI access is unavailable. Consequently, no candidate
subnet, S3 gateway endpoint, security group, route path, available IP capacity,
or NAT requirement can be determined.

## IAM

No IAM roles or policies were inspected. Future design must distinguish:

- **EMR service role:** used by the EMR service to manage the cluster and
  perform required AWS control-plane actions.
- **EC2 instance profile:** supplies credentials to the EC2 instances running
  Spark/YARN. The Bronze job must use this credential chain for S3 access,
  never hard-coded AWS keys.

Potential candidates, policy attachments, trust relationships, and permission
gaps are not determined. Future instance-profile review must cover narrowly
scoped S3 access to the approved Raw, Bronze, artifact, and log prefixes, plus
KMS access only if customer-managed encryption requires it.

## KMS and Encryption

Bucket encryption and KMS key configuration were not inspected. It is unknown
whether the bucket uses SSE-S3 or SSE-KMS, whether a customer-managed key is
involved, and whether the future EC2 instance profile requires KMS permissions.

## EMR

No EMR clusters were inspected and no reusable cluster is identified. Target
release remains `emr-7.13.0` (Spark 3.5.6, Python 3.11, Java 17); its
availability in `eu-west-2` was not verified. No alternate release was chosen.

## Cost and Tagging

No account tagging or cost-allocation convention was available for inspection.
The future approved milestone could incur charges when it creates or runs an
EMR on EC2 cluster, including its EC2 instances and any supporting billable
networking or storage operations. Required project, environment, owner,
cost-centre, and termination/TTL tags must be reviewed before that milestone.

## Permission Matrix

| Area | Read access | Result |
| --- | --- | --- |
| AWS CLI availability | No | `aws` command not found |
| STS identity | Not attempted | AWS CLI prerequisite unavailable |
| AWS CLI Region/profile | Not determined | AWS CLI prerequisite unavailable |
| S3 bucket and objects | Not attempted | AWS CLI prerequisite unavailable |
| S3 bucket configuration | Not attempted | AWS CLI prerequisite unavailable |
| EC2/VPC/networking | Not attempted | AWS CLI prerequisite unavailable |
| IAM | Not attempted | AWS CLI prerequisite unavailable |
| KMS | Not attempted | AWS CLI prerequisite unavailable |
| EMR | Not attempted | AWS CLI prerequisite unavailable |

`Not attempted` here is distinct from `Not found` and `Access denied`: neither
resource existence nor AWS authorization could be assessed.

## Read-Only Command Record

| Command | Purpose | Read-only confirmation | Result |
| --- | --- | --- | --- |
| `aws --version` | Check AWS CLI availability | Reports local CLI version only; does not call or mutate AWS | Failed: executable not found |

No AWS API command was successfully executed. In particular, `aws sts
get-caller-identity`, AWS CLI configuration inspection, S3, EC2, IAM, KMS, and
EMR discovery commands were not run after the CLI prerequisite failure.

## Gap Analysis

### Available

- Repository EMR packaging and local compatibility preparation.
- Configured repository values for Region, lake bucket, Raw prefix, and Bronze
  prefix.
- Ignored local `dist/emr/bronze/` artifacts; no deployment artifact was
  uploaded.

### Missing or Blocked

- AWS CLI availability in the approved discovery environment.
- Authenticated read-only identity verification.
- Evidence for all live AWS resources and configurations required by the
  manual-execution runbook.

### Needs Review After CLI Access Is Available

- Target account identity and intended Region.
- Bucket, Raw/Bronze baseline, encryption, ownership, policy, versioning,
  lifecycle, and public-access settings.
- VPC, candidate subnets, routes, S3 gateway endpoint, security groups, and
  subnet capacity.
- EMR service role and EC2 instance profile candidates, trust relationships,
  and least-privilege permissions.
- KMS key use and required permissions.
- Existing EMR clusters, regional `emr-7.13.0` availability, approved log and
  artifact prefixes, and cost/tagging conventions.

## Recommendation

Install or make the AWS CLI available through the approved development process,
then rerun this same read-only preflight with an authenticated read-only AWS
identity. Stop after documenting those findings and obtain a separate
infrastructure/IAM design review and explicit approval before any resource
creation, artifact upload, EMR cluster creation, or Spark submission.

Current state:

```text
EMR repository preparation: COMPLETE
AWS preflight discovery: COMPLETE (repository-only; live AWS discovery blocked)
EMR infrastructure creation: NOT STARTED
EMR execution: NOT STARTED
```

## Safety Confirmation

No EMR cluster created.
No EC2 instance created.
No EMR job submitted.
No IAM resource created or modified.
No S3 objects uploaded or modified.
No VPC/network resource created or modified.
No billable AWS compute resource was run.
No commit created.
No push performed.
