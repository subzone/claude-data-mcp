"""
Cloud Data MCP — AWS S3 Provider
Discovery via boto3 using the default credential chain:
  1. Environment variables (AWS_ACCESS_KEY_ID etc.)
  2. ~/.aws/credentials
  3. IAM Instance Role / ECS Task Role
  4. AWS SSO / Identity Center
No config required when running in an environment with valid AWS credentials.
Blob file queries are handled by DuckDB engine directly.
"""
from __future__ import annotations

import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from src.config import settings

logger = logging.getLogger(__name__)

_s3_client = None


def _get_s3() -> boto3.client:
    global _s3_client
    if _s3_client is None:
        kwargs: dict = {"region_name": settings.aws_region}
        if settings.aws_access_key_id:
            kwargs["aws_access_key_id"] = settings.aws_access_key_id
            kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
            if settings.aws_session_token:
                kwargs["aws_session_token"] = settings.aws_session_token
        _s3_client = boto3.client("s3", **kwargs)
    return _s3_client


# ── Discovery ──────────────────────────────────────────────────────────────────

async def list_buckets() -> list[dict]:
    """List all S3 buckets accessible to the current identity."""
    try:
        s3 = _get_s3()
        response = s3.list_buckets()
        buckets = response.get("Buckets", [])

        allowed = settings.s3_allowed_buckets_list
        if allowed:
            buckets = [b for b in buckets if b["Name"] in allowed]

        return [
            {
                "name": b["Name"],
                "created": str(b.get("CreationDate", "")),
                "region": _get_bucket_region(b["Name"]),
            }
            for b in buckets
        ]
    except NoCredentialsError:
        raise RuntimeError(
            "No AWS credentials found. Run 'aws configure', set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, "
            "or ensure an IAM role is attached to this environment."
        )


def _get_bucket_region(bucket: str) -> str:
    try:
        resp = _get_s3().get_bucket_location(Bucket=bucket)
        return resp.get("LocationConstraint") or "us-east-1"
    except Exception:
        return "unknown"


async def list_objects(bucket: str, prefix: str | None = None, max_results: int = 200) -> list[dict]:
    """List objects in an S3 bucket with optional prefix filter."""
    s3 = _get_s3()
    kwargs: dict = {"Bucket": bucket, "MaxKeys": min(max_results, 1000)}
    if prefix:
        kwargs["Prefix"] = prefix

    try:
        response = s3.list_objects_v2(**kwargs)
        objects = response.get("Contents", [])
        return [
            {
                "key": obj["Key"],
                "size_bytes": obj["Size"],
                "last_modified": str(obj["LastModified"]),
                "storage_class": obj.get("StorageClass", "STANDARD"),
            }
            for obj in objects
        ]
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchBucket":
            raise RuntimeError(f"Bucket '{bucket}' not found.")
        if code in ("AccessDenied", "403"):
            raise RuntimeError(
                f"Access denied to bucket '{bucket}'. "
                f"Ensure your identity has s3:ListBucket permission."
            )
        raise RuntimeError(f"S3 error: {e}") from e
