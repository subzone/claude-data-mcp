"""
Cloud Data MCP — Tool Definitions
All 7 MCP tools registered here and imported by server.py.
"""
from __future__ import annotations
import json
import logging
from fastmcp import FastMCP
from src.config import settings
from src.engine.duckdb import run_query as duckdb_query, infer_schema, blob_to_duckdb_path

logger = logging.getLogger(__name__)


def register_tools(mcp: FastMCP) -> None:

    # ── 1. discover ─────────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Discover all available data sources across Azure Storage, AWS S3, and Databricks.
Returns containers/buckets, Azure Table Storage tables, and Databricks catalogs.
Always call this first to understand what data is available before querying.

Returns:
  {
    "azure": { "containers": [...], "tables": [...] },   # if Azure configured
    "s3": { "buckets": [...] },                           # if AWS configured
    "databricks": { "catalogs": [...] }                   # if Databricks configured
  }"""
    )
    async def discover(include_blobs: bool = False, blob_prefix: str | None = None) -> str:
        result: dict = {}

        if settings.azure_enabled:
            from src.providers import azure
            try:
                containers = await azure.list_containers()
                tables = await azure.list_tables()
                entry: dict = {"containers": containers, "tables": tables}
                if include_blobs and containers:
                    entry["sample_blobs"] = {}
                    for c in containers[:3]:  # preview first 3 containers only
                        blobs = await azure.list_blobs(c["name"], prefix=blob_prefix, max_results=20)
                        entry["sample_blobs"][c["name"]] = blobs
                result["azure"] = entry
            except Exception as e:
                result["azure"] = {"error": str(e)}

        try:
            from src.providers import s3
            buckets = await s3.list_buckets()
            result["s3"] = {"buckets": buckets}
        except Exception as e:
            result["s3"] = {"error": str(e)}

        if settings.databricks_enabled:
            from src.providers import databricks
            try:
                catalogs = await databricks.list_catalogs()
                result["databricks"] = {"catalogs": catalogs}
            except Exception as e:
                result["databricks"] = {"error": str(e)}

        return json.dumps(result, default=str, indent=2)

    # ── 2. get_schema ────────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Infer the schema (column names and types) of a file or table without downloading data.

Supported sources:
  - Azure blob: provider="azure", path="container/folder/file.parquet"
  - S3 object:  provider="s3", path="bucket-name/prefix/file.csv"
  - Databricks: provider="databricks", path="catalog.schema.table"

Returns list of { column_name, column_type } objects.

Examples:
  get_schema(provider="azure", path="sales/2024/transactions.parquet")
  get_schema(provider="s3", path="my-bucket/data/customers.csv")
  get_schema(provider="databricks", path="main.sales.transactions")"""
    )
    async def get_schema(provider: str, path: str) -> str:
        provider = provider.lower()

        if provider == "databricks":
            from src.providers.databricks import describe_table
            info = await describe_table(path)
            return json.dumps(info["columns"], default=str, indent=2)

        if provider == "azure":
            parts = path.split("/", 1)
            if len(parts) != 2:
                return "Error: Azure path must be 'container/blob_path'"
            container, blob = parts
            duckdb_path = blob_to_duckdb_path("azure", settings.azure_storage_account, container, blob)
        elif provider == "s3":
            parts = path.split("/", 1)
            if len(parts) != 2:
                return "Error: S3 path must be 'bucket/key'"
            bucket, key = parts
            duckdb_path = blob_to_duckdb_path("s3", bucket, "", key).replace("s3:///", f"s3://{bucket}/")
        else:
            return f"Error: Unknown provider '{provider}'. Use 'azure', 's3', or 'databricks'."

        try:
            schema = infer_schema(duckdb_path)
            return json.dumps(schema, default=str, indent=2)
        except Exception as e:
            return f"Error inferring schema: {e}"

    # ── 3. query ─────────────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Execute a SQL query against Azure Blob Storage, AWS S3, or Databricks SQL Warehouse.

For Azure/S3: Write standard SQL using DuckDB syntax.
  Use full paths in FROM clause:
    Azure: FROM 'az://account.blob.core.windows.net/container/file.parquet'
    S3:    FROM 's3://bucket-name/prefix/file.parquet'
    Wildcard: FROM 's3://bucket/data/*.parquet'  (queries all matching files as one table)

For Databricks: Standard Spark SQL using Unity Catalog table names.
    FROM catalog.schema.table_name

Args:
  sql (str): SQL query to execute
  provider (str): 'azure', 's3', or 'databricks'
  max_rows (int): Maximum rows to return (default 1000)
  warehouse_id (str): Databricks SQL Warehouse ID (overrides config)

Returns rows as JSON array.

Examples:
  query(provider="azure", sql="SELECT region, SUM(revenue) FROM 'az://myaccount.blob.core.windows.net/sales/2024/*.parquet' GROUP BY region")
  query(provider="databricks", sql="SELECT * FROM main.sales.transactions WHERE year = 2024 LIMIT 100")"""
    )
    async def query(
        sql: str,
        provider: str,
        max_rows: int = 1000,
        warehouse_id: str | None = None,
    ) -> str:
        provider = provider.lower()

        try:
            if provider in ("azure", "s3"):
                rows = duckdb_query(sql, limit=max_rows)
            elif provider == "databricks":
                from src.providers.databricks import run_query as db_query
                rows = await db_query(sql, warehouse_id=warehouse_id)
                rows = rows[:max_rows]
            else:
                return f"Error: Unknown provider '{provider}'. Use 'azure', 's3', or 'databricks'."

            return json.dumps({
                "row_count": len(rows),
                "rows": rows,
                "truncated": len(rows) >= max_rows,
            }, default=str, indent=2)

        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    # ── 4. query_table_storage ───────────────────────────────────────────────────

    @mcp.tool(
        description="""Query Azure Table Storage using OData filter expressions.

Table Storage is a NoSQL key-value store — good for structured entity data.
Note: For tabular file analysis (CSV/Parquet), use the 'query' tool instead.

Args:
  table_name (str): Azure Table Storage table name
  filter_query (str, optional): OData filter expression
    Examples:
      "PartitionKey eq 'sales'"
      "Year eq '2024' and Region eq 'EMEA'"
      "Amount gt 5000"
      "Timestamp ge datetime'2024-01-01T00:00:00Z'"
  select (list[str], optional): Columns to return (omit for all)
  max_results (int): Maximum rows (default 200)

Returns rows as JSON array."""
    )
    async def query_table_storage(
        table_name: str,
        filter_query: str | None = None,
        select: list[str] | None = None,
        max_results: int = 200,
    ) -> str:
        if not settings.azure_enabled:
            return "Error: Azure not configured. Set AZURE_STORAGE_ACCOUNT."
        from src.providers.azure import query_table
        try:
            rows = await query_table(table_name, filter_query, select, max_results)
            return json.dumps({"row_count": len(rows), "rows": rows}, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    # ── 5. sample ────────────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Return a sample of rows from any data source to understand its content.

Supported sources:
  - Azure blob file:  provider="azure", path="container/file.parquet"
  - S3 object:        provider="s3", path="bucket/key.csv"
  - Azure Table:      provider="azure_table", path="TableName"
  - Databricks table: provider="databricks", path="catalog.schema.table"

Args:
  provider (str): 'azure', 's3', 'azure_table', or 'databricks'
  path (str): Resource path (see above)
  n (int): Number of rows to return (default 25)

Returns rows as JSON array."""
    )
    async def sample(provider: str, path: str, n: int = 25) -> str:
        provider = provider.lower()
        n = min(n, settings.max_query_rows)

        try:
            if provider == "azure":
                parts = path.split("/", 1)
                if len(parts) != 2:
                    return "Error: Azure path must be 'container/blob'"
                container, blob = parts
                duckdb_path = f"az://{settings.azure_storage_account}.blob.core.windows.net/{container}/{blob}"
                rows = duckdb_query(f"SELECT * FROM '{duckdb_path}'", limit=n)

            elif provider == "s3":
                rows = duckdb_query(f"SELECT * FROM 's3://{path}'", limit=n)

            elif provider == "azure_table":
                from src.providers.azure import sample_table
                rows = await sample_table(path, n)

            elif provider == "databricks":
                from src.providers.databricks import sample_table
                rows = await sample_table(path, n)

            else:
                return f"Error: Unknown provider '{provider}'."

            return json.dumps({"row_count": len(rows), "rows": rows}, default=str, indent=2)

        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    # ── 6. list_tables ───────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Browse Unity Catalog tables in Databricks.

Use this to navigate the catalog hierarchy before querying:
  1. list_tables(level="catalogs") → see all catalogs
  2. list_tables(level="schemas", catalog="main") → see schemas in 'main'
  3. list_tables(level="tables", catalog="main", schema="sales") → see all tables

Args:
  level (str): 'catalogs', 'schemas', or 'tables'
  catalog (str): Required for 'schemas' and 'tables' levels
  schema (str): Required for 'tables' level"""
    )
    async def list_tables(
        level: str = "catalogs",
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        if not settings.databricks_enabled:
            return "Error: Databricks not configured. Set DATABRICKS_HOST."
        from src.providers import databricks
        try:
            if level == "catalogs":
                result = await databricks.list_catalogs()
            elif level == "schemas":
                if not catalog:
                    return "Error: 'catalog' is required for level='schemas'"
                result = await databricks.list_schemas(catalog)
            elif level == "tables":
                if not catalog or not schema:
                    return "Error: 'catalog' and 'schema' are required for level='tables'"
                result = await databricks.list_tables(catalog, schema)
            else:
                return f"Error: Unknown level '{level}'. Use 'catalogs', 'schemas', or 'tables'."

            return json.dumps(result, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)

    # ── 7. describe_table ────────────────────────────────────────────────────────

    @mcp.tool(
        description="""Get full metadata for a Databricks Unity Catalog table including all column definitions.

Args:
  full_table_name (str): Table in format 'catalog.schema.table'
    Example: 'main.sales.transactions'

Returns:
  {
    "full_name": "main.sales.transactions",
    "table_type": "MANAGED",
    "format": "DELTA",
    "location": "abfss://...",
    "owner": "...",
    "comment": "...",
    "row_count": 1500000,
    "columns": [
      { "name": "transaction_id", "type": "STRING", "nullable": false, "comment": "..." },
      ...
    ]
  }"""
    )
    async def describe_table(full_table_name: str) -> str:
        if not settings.databricks_enabled:
            return "Error: Databricks not configured. Set DATABRICKS_HOST."
        from src.providers.databricks import describe_table as _describe
        try:
            result = await _describe(full_table_name)
            return json.dumps(result, default=str, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)
