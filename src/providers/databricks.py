"""
Cloud Data MCP — Databricks Provider
Supports:
  - Unity Catalog browsing (catalogs, schemas, tables)
  - SQL Warehouse query execution
Auth:
  - Personal Access Token (DATABRICKS_TOKEN env var)
  - OAuth M2M (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
  - Azure Managed Identity / az login (when hosted on Azure)
"""
from __future__ import annotations
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from src.config import settings

logger = logging.getLogger(__name__)

_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        kwargs: dict = {"host": settings.databricks_host}

        if settings.databricks_token:
            # PAT auth
            kwargs["token"] = settings.databricks_token
            logger.info("Databricks: using Personal Access Token")
        elif settings.databricks_client_id and settings.databricks_client_secret:
            # OAuth M2M
            kwargs["client_id"] = settings.databricks_client_id
            kwargs["client_secret"] = settings.databricks_client_secret
            logger.info("Databricks: using OAuth M2M")
        else:
            # Let the SDK resolve credentials automatically
            # (Azure MSI, Databricks CLI config, env vars)
            logger.info("Databricks: using default credential chain")

        _client = WorkspaceClient(**kwargs)
    return _client


# ── Unity Catalog ──────────────────────────────────────────────────────────────

async def list_catalogs() -> list[dict]:
    """List all Unity Catalog catalogs accessible to the current identity."""
    client = _get_client()
    return [
        {
            "name": c.name,
            "comment": c.comment,
            "owner": c.owner,
            "type": str(c.catalog_type) if c.catalog_type else None,
        }
        for c in client.catalogs.list()
    ]


async def list_schemas(catalog: str) -> list[dict]:
    """List all schemas (databases) in a catalog."""
    client = _get_client()
    return [
        {
            "name": s.name,
            "full_name": s.full_name,
            "comment": s.comment,
            "owner": s.owner,
        }
        for s in client.schemas.list(catalog_name=catalog)
    ]


async def list_tables(catalog: str, schema: str) -> list[dict]:
    """List all tables in a catalog.schema."""
    client = _get_client()
    return [
        {
            "name": t.name,
            "full_name": t.full_name,
            "table_type": str(t.table_type) if t.table_type else None,
            "data_source_format": str(t.data_source_format) if t.data_source_format else None,
            "comment": t.comment,
            "owner": t.owner,
            "created_at": str(t.created_at) if t.created_at else None,
            "updated_at": str(t.updated_at) if t.updated_at else None,
        }
        for t in client.tables.list(catalog_name=catalog, schema_name=schema)
    ]


async def describe_table(full_table_name: str) -> dict:
    """
    Get full metadata for a table including column definitions.
    full_table_name format: 'catalog.schema.table'
    """
    client = _get_client()
    parts = full_table_name.split(".")
    if len(parts) != 3:
        raise ValueError("Table name must be in format 'catalog.schema.table'")

    table = client.tables.get(full_name=full_table_name)
    columns = [
        {
            "name": col.name,
            "type": str(col.type_text) if col.type_text else None,
            "nullable": col.nullable,
            "comment": col.comment,
        }
        for col in (table.columns or [])
    ]
    return {
        "full_name": table.full_name,
        "table_type": str(table.table_type) if table.table_type else None,
        "format": str(table.data_source_format) if table.data_source_format else None,
        "location": table.storage_location,
        "owner": table.owner,
        "comment": table.comment,
        "row_count": table.properties.get("numRows") if table.properties else None,
        "columns": columns,
    }


# ── SQL Warehouse ──────────────────────────────────────────────────────────────

async def run_query(sql: str, warehouse_id: str | None = None) -> list[dict]:
    """
    Execute SQL against a Databricks SQL Warehouse.
    Returns results as a list of dicts.
    """
    client = _get_client()
    wh_id = warehouse_id or settings.databricks_warehouse_id
    if not wh_id:
        raise RuntimeError(
            "No SQL Warehouse ID configured. Set DATABRICKS_WAREHOUSE_ID or pass warehouse_id."
        )

    statement = client.statement_execution.execute_statement(
        warehouse_id=wh_id,
        statement=sql,
        wait_timeout="60s",
        on_wait_timeout="CANCEL",
    )

    state = statement.status.state
    if state == StatementState.FAILED:
        raise RuntimeError(f"Databricks query failed: {statement.status.error.message}")
    if state == StatementState.CANCELED:
        raise RuntimeError("Databricks query timed out (60s limit). Try a more targeted query.")

    result = statement.result
    if not result or not result.data_array:
        return []

    schema = statement.manifest.schema.columns
    columns = [col.name for col in schema]
    return [dict(zip(columns, row)) for row in result.data_array]


async def sample_table(full_table_name: str, n: int = 25) -> list[dict]:
    """Return N rows from a Databricks table for exploration."""
    return await run_query(f"SELECT * FROM {full_table_name} LIMIT {n}")
