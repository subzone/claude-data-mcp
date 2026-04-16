"""
Cloud Data MCP — DuckDB Query Engine
Provides authenticated DuckDB access to Azure Blob Storage and AWS S3.
Secrets are injected once at startup using DuckDB's secret manager.
"""

from __future__ import annotations

import logging

import duckdb

from src.config import settings

logger = logging.getLogger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Return a singleton DuckDB in-memory connection with cloud secrets configured."""
    global _conn
    if _conn is not None:
        return _conn

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # ── Azure ──────────────────────────────────────────────────────────────────
    if settings.azure_enabled:
        if settings.azure_client_id and settings.azure_client_secret:
            conn.execute(f"""
                CREATE OR REPLACE SECRET azure_sp (
                    TYPE azure,
                    PROVIDER service_principal,
                    TENANT_ID '{settings.azure_tenant_id}',
                    CLIENT_ID '{settings.azure_client_id}',
                    CLIENT_SECRET '{settings.azure_client_secret}',
                    ACCOUNT_NAME '{settings.azure_storage_account}'
                )
            """)
            logger.info("DuckDB: Azure secret (service principal) configured")
        else:
            # Fall back to credential chain (az login / MSI / workload identity)
            conn.execute(f"""
                CREATE OR REPLACE SECRET azure_chain (
                    TYPE azure,
                    PROVIDER credential_chain,
                    ACCOUNT_NAME '{settings.azure_storage_account}'
                )
            """)
            logger.info("DuckDB: Azure secret (credential chain) configured")

    # ── AWS S3 ─────────────────────────────────────────────────────────────────
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        conn.execute(f"""
            CREATE OR REPLACE SECRET s3_keys (
                TYPE s3,
                KEY_ID '{settings.aws_access_key_id}',
                SECRET '{settings.aws_secret_access_key}',
                REGION '{settings.aws_region}'
                {"" if not settings.aws_session_token else f", SESSION_TOKEN '{settings.aws_session_token}'"}
            )
        """)
        logger.info("DuckDB: S3 secret (access key) configured")
    else:
        # Attempt credential chain (~/.aws/credentials, instance metadata, etc.)
        # Gracefully skip if no credentials are available in this environment.
        try:
            conn.execute(f"""
                CREATE OR REPLACE SECRET s3_chain (
                    TYPE s3,
                    PROVIDER credential_chain,
                    REGION '{settings.aws_region}'
                )
            """)
            logger.info("DuckDB: S3 secret (credential chain) configured")
        except Exception as exc:
            logger.warning(
                "DuckDB: S3 credential chain unavailable — S3 queries will fail: %s", exc
            )

    _conn = conn
    return _conn


def run_query(sql: str, limit: int | None = None) -> list[dict]:
    """
    Execute a SQL query using DuckDB.
    Optionally wraps the query in a LIMIT to cap result size.
    Returns a list of row dicts.
    """
    conn = _get_conn()
    effective_sql = f"SELECT * FROM ({sql}) __q LIMIT {limit}" if limit else sql
    rel = conn.execute(effective_sql)
    columns = [desc[0] for desc in rel.description]
    return [dict(zip(columns, row, strict=False)) for row in rel.fetchall()]


def infer_schema(path: str) -> list[dict]:
    """
    Return column name + type pairs for any file DuckDB can read.
    Works with Parquet, CSV, JSON, and NDJSON.
    """
    conn = _get_conn()
    rel = conn.execute(f"DESCRIBE FROM '{path}' LIMIT 0")
    return [{"column_name": row[0], "column_type": row[1]} for row in rel.fetchall()]


def blob_to_duckdb_path(
    provider: str,
    account_or_bucket: str,
    container_or_prefix: str,
    blob_key: str,
) -> str:
    """
    Build a DuckDB-readable URL from provider-specific components.

    Azure: az://<account>.blob.core.windows.net/<container>/<blob>
    S3:    s3://<bucket>/<key>
    """
    if provider == "azure":
        return f"az://{account_or_bucket}.blob.core.windows.net/{container_or_prefix}/{blob_key}"
    if provider == "s3":
        return f"s3://{account_or_bucket}/{blob_key}"
    raise ValueError(f"Unknown provider for DuckDB path: {provider!r}")
