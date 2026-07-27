"""
Upload validated raw PostgreSQL extraction batches to Amazon S3.

The uploader is batch-aware and quality-gated.

Pipeline:

    PostgreSQL
        ↓
    etl.extract
        ↓
    local Parquet + batch manifest
        ↓
    quality.raw_extraction_validation
        ↓
    etl.export_s3
        ↓
    Amazon S3 raw zone

AWS credentials are expected to be available through environment variables:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN   # optional for temporary credentials

Because config.settings imports load_dotenv(), importing settings ensures
credentials stored in the project .env file are loaded into the process
environment before Boto3 resolves them.

Run latest validated batch:

    python -m etl.export_s3

Run a specific batch:

    python -m etl.export_s3 --batch-id <uuid>

Example S3 layout:

    s3://<bucket>/raw/postgresql/
        employees/
            extraction_date=2026-07-25/
                batch_id=<batch-id>/
                    extraction_id=<extraction-id>/
                        part-00000.parquet

        _manifests/
            batch_id=<batch-id>.json

        _upload_manifests/
            batch_id=<batch-id>.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)

from config.logger import logger
from config.settings import settings
from etl.extract import (
    MANIFEST_DIRECTORY,
    RAW_DATA_DIRECTORY,
    SOURCE_SYSTEM,
)
from quality.raw_extraction_validation import (
    RawExtractionValidationError,
    find_latest_manifest,
    load_manifest,
    validate_batch,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_RAW_PREFIX = "raw/postgresql"

UPLOAD_MANIFEST_DIRECTORY = (
    RAW_DATA_DIRECTORY
    / "_upload_manifests"
)

SERVER_SIDE_ENCRYPTION = "AES256"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class S3ExportError(RuntimeError):
    """
    Raised when a validated raw extraction batch cannot be exported to S3.
    """


# ---------------------------------------------------------------------------
# Result Models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class S3UploadResult:
    """
    Result for one successfully uploaded file.
    """

    batch_id: str
    table_name: str
    local_path: str
    s3_bucket: str
    s3_key: str
    s3_uri: str
    file_size_bytes: int
    etag: str | None
    uploaded_at_utc: str
    upload_status: str


@dataclass(frozen=True)
class BatchUploadResult:
    """
    Summary for one uploaded extraction batch.
    """

    batch_id: str
    bucket: str
    prefix: str
    uploaded_files: int
    uploaded_bytes: int
    upload_results: list[S3UploadResult]
    local_upload_manifest: Path
    s3_upload_manifest_uri: str


# ---------------------------------------------------------------------------
# Settings Validation
# ---------------------------------------------------------------------------

def get_aws_region() -> str:
    """
    Return the configured AWS region.
    """

    region = settings.AWS_REGION

    if not region:
        raise S3ExportError(
            "AWS region is not configured. "
            "Set AWS_REGION in .env."
        )

    return str(region)


def get_s3_bucket() -> str:
    """
    Return the configured Amazon S3 bucket.
    """

    bucket = settings.AWS_S3_BUCKET

    if not bucket:
        raise S3ExportError(
            "S3 bucket is not configured. "
            "Set AWS_S3_BUCKET in .env."
        )

    return str(bucket)


def get_raw_s3_prefix() -> str:
    """
    Return the configured raw-zone S3 prefix.
    """

    prefix = (
        settings.AWS_S3_RAW_PREFIX
        or DEFAULT_RAW_PREFIX
    )

    prefix = str(prefix).strip("/")

    if not prefix:
        return DEFAULT_RAW_PREFIX

    return prefix


def validate_aws_credentials_configured() -> None:
    """
    Validate that AWS credentials are present in the environment.

    This project currently uses environment-variable credentials loaded
    from .env via config.settings.

    AWS_SESSION_TOKEN is optional because it is only required for
    temporary credentials.
    """

    if not settings.AWS_ACCESS_KEY_ID:
        raise S3ExportError(
            "AWS_ACCESS_KEY_ID is not configured. "
            "Add it to your .env file."
        )

    if not settings.AWS_SECRET_ACCESS_KEY:
        raise S3ExportError(
            "AWS_SECRET_ACCESS_KEY is not configured. "
            "Add it to your .env file."
        )


# ---------------------------------------------------------------------------
# AWS Client
# ---------------------------------------------------------------------------

def create_boto3_session() -> boto3.Session:
    """
    Create a Boto3 session.

    Credentials are intentionally not passed directly. Boto3 resolves the
    standard AWS environment variables automatically after .env has been
    loaded by config.settings.
    """

    validate_aws_credentials_configured()

    try:
        session = boto3.Session(
            region_name=get_aws_region(),
        )

        credentials = session.get_credentials()

        if credentials is None:
            raise S3ExportError(
                "Boto3 could not resolve AWS credentials "
                "from the current environment."
            )

        logger.info(
            "AWS credentials resolved successfully "
            f"using provider: {credentials.method}"
        )

        return session

    except (
        BotoCoreError,
        ClientError,
    ) as error:
        raise S3ExportError(
            "Unable to create Boto3 session."
        ) from error


def create_s3_client():
    """
    Create an Amazon S3 client using environment-based AWS credentials.
    """

    session = create_boto3_session()

    try:
        return session.client(
            "s3"
        )

    except (
        BotoCoreError,
        ClientError,
    ) as error:
        raise S3ExportError(
            "Unable to create Amazon S3 client."
        ) from error


def create_sts_client():
    """
    Create an AWS STS client for authentication checks.
    """

    session = create_boto3_session()

    try:
        return session.client(
            "sts"
        )

    except (
        BotoCoreError,
        ClientError,
    ) as error:
        raise S3ExportError(
            "Unable to create AWS STS client."
        ) from error


# ---------------------------------------------------------------------------
# AWS Identity Validation
# ---------------------------------------------------------------------------

def validate_aws_identity() -> dict:
    """
    Confirm that the configured AWS credentials are valid.

    Returns:
        AWS caller identity response.

    Raises:
        S3ExportError:
            If credentials are missing, incomplete or rejected.
    """

    sts_client = create_sts_client()

    try:
        identity = (
            sts_client.get_caller_identity()
        )

    except NoCredentialsError as error:
        raise S3ExportError(
            "AWS credentials were not found."
        ) from error

    except PartialCredentialsError as error:
        raise S3ExportError(
            "AWS credentials are incomplete."
        ) from error

    except ClientError as error:
        raise S3ExportError(
            "AWS credentials were rejected "
            "during STS authentication."
        ) from error

    account_id = identity.get(
        "Account",
        "unknown",
    )

    arn = identity.get(
        "Arn",
        "unknown",
    )

    logger.info(
        "AWS identity confirmed | "
        f"Account={account_id} | "
        f"ARN={arn}"
    )

    return identity


# ---------------------------------------------------------------------------
# Batch Resolution
# ---------------------------------------------------------------------------

def resolve_batch_id(
    batch_id: str | None,
) -> str:
    """
    Resolve an explicitly supplied batch ID or use the latest manifest.
    """

    if batch_id:
        return batch_id

    latest_manifest = (
        find_latest_manifest()
    )

    filename = latest_manifest.name

    prefix = "batch_id="
    suffix = ".json"

    if (
        not filename.startswith(prefix)
        or not filename.endswith(suffix)
    ):
        raise S3ExportError(
            "Unexpected extraction manifest "
            f"filename: {filename}"
        )

    return filename[
        len(prefix):-len(suffix)
    ]


# ---------------------------------------------------------------------------
# S3 Key Construction
# ---------------------------------------------------------------------------

def build_data_s3_key(
    local_path: Path,
    prefix: str,
) -> str:
    """
    Preserve the raw PostgreSQL partition hierarchy in Amazon S3.

    Example:

        data/raw/postgres/employees/
            extraction_date=2026-07-25/
            batch_id=abc/
            extraction_id=xyz/
            part-00000.parquet

    becomes:

        raw/postgresql/employees/
            extraction_date=2026-07-25/
            batch_id=abc/
            extraction_id=xyz/
            part-00000.parquet
    """

    try:
        relative_path = (
            local_path
            .resolve()
            .relative_to(
                RAW_DATA_DIRECTORY.resolve()
            )
        )

    except ValueError as error:
        raise S3ExportError(
            "Attempted to upload a file "
            "outside the configured raw directory: "
            f"{local_path}"
        ) from error

    path_parts = [
        prefix,
        *relative_path.parts,
    ]

    return "/".join(
        part.strip("/")
        for part in path_parts
        if part
    )


def build_batch_manifest_s3_key(
    batch_id: str,
    prefix: str,
) -> str:
    """
    Build the S3 object key for an extraction manifest.
    """

    return (
        f"{prefix}/_manifests/"
        f"batch_id={batch_id}.json"
    )


def build_upload_manifest_s3_key(
    batch_id: str,
    prefix: str,
) -> str:
    """
    Build the S3 object key for an upload manifest.
    """

    return (
        f"{prefix}/_upload_manifests/"
        f"batch_id={batch_id}.json"
    )


# ---------------------------------------------------------------------------
# Bucket Validation
# ---------------------------------------------------------------------------

def validate_bucket_access(
    s3_client,
    bucket: str,
) -> None:
    """
    Confirm that the configured S3 bucket exists and is accessible.
    """

    logger.info(
        f"Checking access to S3 bucket: {bucket}"
    )

    try:
        s3_client.head_bucket(
            Bucket=bucket,
        )

    except NoCredentialsError as error:
        raise S3ExportError(
            "AWS credentials were not found."
        ) from error

    except PartialCredentialsError as error:
        raise S3ExportError(
            "AWS credentials are incomplete."
        ) from error

    except ClientError as error:
        response = error.response

        error_code = (
            response
            .get("Error", {})
            .get("Code", "Unknown")
        )

        raise S3ExportError(
            f"Unable to access S3 bucket "
            f"'{bucket}'. AWS error code: "
            f"{error_code}"
        ) from error

    logger.info(
        f"S3 bucket access confirmed: {bucket}"
    )


# ---------------------------------------------------------------------------
# File Upload
# ---------------------------------------------------------------------------

def upload_file(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
) -> tuple[str | None, int]:
    """
    Upload one local file to Amazon S3.

    The upload is verified using the remote object's ContentLength.

    Returns:
        Tuple:
            - ETag
            - file size in bytes
    """

    if not local_path.exists():
        raise S3ExportError(
            "Upload file does not exist: "
            f"{local_path}"
        )

    if not local_path.is_file():
        raise S3ExportError(
            "Upload path is not a file: "
            f"{local_path}"
        )

    local_file_size = (
        local_path.stat().st_size
    )

    logger.info(
        f"Uploading | "
        f"{local_path} "
        f"→ s3://{bucket}/{s3_key}"
    )

    try:
        s3_client.upload_file(
            Filename=str(
                local_path
            ),
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "ServerSideEncryption": (
                    SERVER_SIDE_ENCRYPTION
                ),
            },
        )

        remote_object = (
            s3_client.head_object(
                Bucket=bucket,
                Key=s3_key,
            )
        )

    except NoCredentialsError as error:
        raise S3ExportError(
            "AWS credentials were not found "
            "during S3 upload."
        ) from error

    except PartialCredentialsError as error:
        raise S3ExportError(
            "AWS credentials are incomplete."
        ) from error

    except (
        ClientError,
        BotoCoreError,
    ) as error:
        raise S3ExportError(
            "S3 upload failed for file: "
            f"{local_path}"
        ) from error

    remote_file_size = int(
        remote_object[
            "ContentLength"
        ]
    )

    if remote_file_size != local_file_size:
        raise S3ExportError(
            "Uploaded object size does not match "
            "the source file. "
            f"Local={local_file_size:,} bytes | "
            f"S3={remote_file_size:,} bytes | "
            f"File={local_path}"
        )

    encryption = remote_object.get(
        "ServerSideEncryption"
    )

    if encryption != SERVER_SIDE_ENCRYPTION:
        raise S3ExportError(
            "Uploaded object does not have the "
            "expected server-side encryption. "
            f"Expected={SERVER_SIDE_ENCRYPTION} | "
            f"Actual={encryption}"
        )

    etag = remote_object.get(
        "ETag"
    )

    if isinstance(
        etag,
        str,
    ):
        etag = etag.strip(
            '"'
        )

    logger.info(
        f"Upload verified | "
        f"s3://{bucket}/{s3_key} | "
        f"{local_file_size:,} bytes | "
        f"Encryption={encryption}"
    )

    return (
        etag,
        local_file_size,
    )


# ---------------------------------------------------------------------------
# Extraction Manifest
# ---------------------------------------------------------------------------

def get_extraction_manifest_path(
    batch_id: str,
) -> Path:
    """
    Return the local extraction manifest for a batch.
    """

    manifest_path = (
        MANIFEST_DIRECTORY
        / f"batch_id={batch_id}.json"
    )

    if not manifest_path.exists():
        raise S3ExportError(
            "Extraction manifest does not exist: "
            f"{manifest_path}"
        )

    return manifest_path


# ---------------------------------------------------------------------------
# Upload Manifest
# ---------------------------------------------------------------------------

def write_upload_manifest(
    batch_id: str,
    bucket: str,
    prefix: str,
    results: list[S3UploadResult],
    extraction_manifest_uri: str,
) -> Path:
    """
    Write a local upload manifest describing the S3 transfer.
    """

    UPLOAD_MANIFEST_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        UPLOAD_MANIFEST_DIRECTORY
        / f"batch_id={batch_id}.json"
    )

    uploaded_bytes = sum(
        result.file_size_bytes
        for result in results
    )

    payload = {
        "batch_id": batch_id,
        "source_system": SOURCE_SYSTEM,
        "destination": "amazon_s3",
        "bucket": bucket,
        "prefix": prefix,
        "uploaded_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "uploaded_files": len(
            results
        ),
        "uploaded_bytes": (
            uploaded_bytes
        ),
        "extraction_manifest_s3_uri": (
            extraction_manifest_uri
        ),
        "files": [
            asdict(
                result
            )
            for result in results
        ],
    }

    try:
        with output_path.open(
            mode="w",
            encoding="utf-8",
        ) as file:
            json.dump(
                payload,
                file,
                indent=2,
            )

    except OSError as error:
        raise S3ExportError(
            "Unable to write local upload manifest: "
            f"{output_path}"
        ) from error

    logger.info(
        f"Upload manifest created: "
        f"{output_path}"
    )

    return output_path


# ---------------------------------------------------------------------------
# Batch Upload
# ---------------------------------------------------------------------------

def upload_validated_batch(
    batch_id: str | None = None,
) -> BatchUploadResult:
    """
    Validate and upload one complete raw extraction batch.

    This is the main programmatic entry point.

    The batch is uploaded only when:

        1. AWS credentials resolve successfully.
        2. AWS identity is valid.
        3. Raw extraction validation passes.
        4. The configured S3 bucket is accessible.
    """

    resolved_batch_id = (
        resolve_batch_id(
            batch_id
        )
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "RAW BATCH S3 EXPORT"
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        f"Batch ID: "
        f"{resolved_batch_id}"
    )

    # ------------------------------------------------------------------
    # AWS credentials and identity
    # ------------------------------------------------------------------

    validate_aws_credentials_configured()

    validate_aws_identity()

    # ------------------------------------------------------------------
    # Raw quality gate
    # ------------------------------------------------------------------

    logger.info(
        "Running raw extraction quality gate..."
    )

    try:
        validation_result = (
            validate_batch(
                batch_id=resolved_batch_id
            )
        )

    except RawExtractionValidationError as error:
        raise S3ExportError(
            "Batch cannot be uploaded because "
            "raw extraction validation failed."
        ) from error

    if not validation_result.passed:
        raise S3ExportError(
            f"Batch '{resolved_batch_id}' "
            "did not pass raw extraction validation."
        )

    logger.info(
        "Quality gate passed | "
        f"{validation_result.tables_passed}/"
        f"{validation_result.tables_checked} "
        "tables"
    )

    # ------------------------------------------------------------------
    # Load extraction manifest
    # ------------------------------------------------------------------

    manifest = load_manifest(
        batch_id=resolved_batch_id
    )

    if (
        manifest["batch_id"]
        != resolved_batch_id
    ):
        raise S3ExportError(
            "Extraction manifest batch ID "
            "does not match requested batch."
        )

    if manifest.get(
        "failed_tables"
    ):
        raise S3ExportError(
            "Cannot upload an incomplete extraction batch. "
            f"Failed tables: "
            f"{manifest['failed_tables']}"
        )

    bucket = get_s3_bucket()

    prefix = get_raw_s3_prefix()

    logger.info(
        f"Destination: "
        f"s3://{bucket}/{prefix}/"
    )

    # ------------------------------------------------------------------
    # S3 client and bucket validation
    # ------------------------------------------------------------------

    s3_client = (
        create_s3_client()
    )

    validate_bucket_access(
        s3_client=s3_client,
        bucket=bucket,
    )

    # ------------------------------------------------------------------
    # Upload Parquet files
    # ------------------------------------------------------------------

    upload_results: list[
        S3UploadResult
    ] = []

    table_manifests = manifest.get(
        "tables",
        [],
    )

    if not table_manifests:
        raise S3ExportError(
            "Extraction manifest contains "
            "no table artifacts."
        )

    for table_manifest in table_manifests:

        table_name = (
            table_manifest[
                "table_name"
            ]
        )

        local_path = Path(
            table_manifest[
                "output_path"
            ]
        )

        s3_key = (
            build_data_s3_key(
                local_path=local_path,
                prefix=prefix,
            )
        )

        etag, file_size = (
            upload_file(
                s3_client=s3_client,
                local_path=local_path,
                bucket=bucket,
                s3_key=s3_key,
            )
        )

        uploaded_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        upload_results.append(
            S3UploadResult(
                batch_id=(
                    resolved_batch_id
                ),
                table_name=(
                    table_name
                ),
                local_path=str(
                    local_path.resolve()
                ),
                s3_bucket=(
                    bucket
                ),
                s3_key=(
                    s3_key
                ),
                s3_uri=(
                    f"s3://{bucket}/{s3_key}"
                ),
                file_size_bytes=(
                    file_size
                ),
                etag=(
                    etag
                ),
                uploaded_at_utc=(
                    uploaded_at
                ),
                upload_status=(
                    "SUCCESS"
                ),
            )
        )

    # ------------------------------------------------------------------
    # Upload extraction manifest
    # ------------------------------------------------------------------

    extraction_manifest_path = (
        get_extraction_manifest_path(
            resolved_batch_id
        )
    )

    extraction_manifest_key = (
        build_batch_manifest_s3_key(
            batch_id=(
                resolved_batch_id
            ),
            prefix=prefix,
        )
    )

    upload_file(
        s3_client=s3_client,
        local_path=(
            extraction_manifest_path
        ),
        bucket=bucket,
        s3_key=(
            extraction_manifest_key
        ),
    )

    extraction_manifest_uri = (
        f"s3://{bucket}/"
        f"{extraction_manifest_key}"
    )

    # ------------------------------------------------------------------
    # Create local upload manifest
    # ------------------------------------------------------------------

    local_upload_manifest = (
        write_upload_manifest(
            batch_id=(
                resolved_batch_id
            ),
            bucket=bucket,
            prefix=prefix,
            results=(
                upload_results
            ),
            extraction_manifest_uri=(
                extraction_manifest_uri
            ),
        )
    )

    # ------------------------------------------------------------------
    # Upload the upload manifest
    # ------------------------------------------------------------------

    upload_manifest_key = (
        build_upload_manifest_s3_key(
            batch_id=(
                resolved_batch_id
            ),
            prefix=prefix,
        )
    )

    upload_file(
        s3_client=s3_client,
        local_path=(
            local_upload_manifest
        ),
        bucket=bucket,
        s3_key=(
            upload_manifest_key
        ),
    )

    upload_manifest_uri = (
        f"s3://{bucket}/"
        f"{upload_manifest_key}"
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    uploaded_bytes = sum(
        result.file_size_bytes
        for result in upload_results
    )

    logger.info(
        "=" * 70
    )

    logger.info(
        "S3 EXPORT COMPLETE | "
        f"Batch={resolved_batch_id} | "
        f"Tables={len(upload_results)} | "
        f"Bytes={uploaded_bytes:,}"
    )

    logger.info(
        "Extraction manifest: "
        f"{extraction_manifest_uri}"
    )

    logger.info(
        "Upload manifest: "
        f"{upload_manifest_uri}"
    )

    logger.info(
        "=" * 70
    )

    return BatchUploadResult(
        batch_id=(
            resolved_batch_id
        ),
        bucket=(
            bucket
        ),
        prefix=(
            prefix
        ),
        uploaded_files=len(
            upload_results
        ),
        uploaded_bytes=(
            uploaded_bytes
        ),
        upload_results=(
            upload_results
        ),
        local_upload_manifest=(
            local_upload_manifest
        ),
        s3_upload_manifest_uri=(
            upload_manifest_uri
        ),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Upload a validated raw PostgreSQL "
            "extraction batch to Amazon S3."
        )
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help=(
            "Specific extraction batch ID. "
            "If omitted, the latest extraction "
            "batch is used."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """
    Command-line entry point.
    """

    args = parse_arguments()

    try:
        result = upload_validated_batch(
            batch_id=args.batch_id
        )

        logger.info(
            "Upload completed successfully | "
            f"Batch={result.batch_id} | "
            f"Files={result.uploaded_files}"
        )

    except S3ExportError as error:
        logger.error(
            f"S3 export failed: {error}"
        )

        raise


if __name__ == "__main__":
    main()
