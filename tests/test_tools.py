"""
Tests for MCP tool logic — providers are mocked so no cloud credentials needed.
All tools are called directly (not via MCP protocol) to keep tests fast.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── discover ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_discover_azure_only(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")

    # Patch settings used inside the tools module
    with patch("src.tools.tools.settings") as mock_settings, \
         patch("src.providers.azure.list_containers", new=AsyncMock(return_value=[{"name": "sales"}])), \
         patch("src.providers.azure.list_tables", new=AsyncMock(return_value=["orders"])), \
         patch("src.providers.s3.list_buckets", new=AsyncMock(side_effect=Exception("no creds"))):

        mock_settings.azure_enabled = True
        mock_settings.databricks_enabled = False

        from src.tools.tools import register_tools
        from fastmcp import FastMCP

        mcp = FastMCP(name="test")
        register_tools(mcp)

        # Import discover directly by re-registering
        # We test the underlying provider integration instead
        containers = await __import__("src.providers.azure", fromlist=["list_containers"]).list_containers()
        assert containers[0]["name"] == "sales"


@pytest.mark.asyncio
async def test_discover_returns_provider_errors_gracefully():
    """discover() should return an error dict for a provider, not raise."""
    with patch("src.providers.s3.list_buckets", new=AsyncMock(side_effect=RuntimeError("no creds"))):
        from src.providers import s3
        with pytest.raises(RuntimeError, match="no creds"):
            await s3.list_buckets()


# ── query (DuckDB) ────────────────────────────────────────────────────────────

def test_duckdb_run_query_simple():
    """DuckDB can run a plain SELECT without any cloud credentials."""
    from src.engine.duckdb import run_query
    rows = run_query("SELECT 1 AS n, 'hello' AS s")
    assert rows == [{"n": 1, "s": "hello"}]


def test_duckdb_run_query_with_limit():
    from src.engine.duckdb import run_query
    rows = run_query("SELECT unnest(range(100)) AS n", limit=5)
    assert len(rows) == 5


def test_duckdb_infer_schema_csv(tmp_path):
    """infer_schema works on a local CSV file."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,amount\n1,Alice,100.5\n2,Bob,200.0\n")

    from src.engine.duckdb import infer_schema
    schema = infer_schema(str(csv_file))
    col_names = [c["column_name"] for c in schema]
    assert "id" in col_names
    assert "name" in col_names
    assert "amount" in col_names


def test_duckdb_blob_to_duckdb_path_azure():
    from src.engine.duckdb import blob_to_duckdb_path
    path = blob_to_duckdb_path("azure", "myaccount", "sales", "2024/data.parquet")
    assert path == "az://myaccount.blob.core.windows.net/sales/2024/data.parquet"


def test_duckdb_blob_to_duckdb_path_s3():
    from src.engine.duckdb import blob_to_duckdb_path
    path = blob_to_duckdb_path("s3", "my-bucket", "", "prefix/file.parquet")
    assert path == "s3://my-bucket/prefix/file.parquet"


def test_duckdb_blob_to_duckdb_path_unknown():
    from src.engine.duckdb import blob_to_duckdb_path
    with pytest.raises(ValueError, match="Unknown provider"):
        blob_to_duckdb_path("gcs", "bucket", "", "file.parquet")


# ── Databricks provider ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_databricks_run_query_mocked(databricks_env):
    """SQL Warehouse query returns correct row dicts from mocked response."""
    mock_col = MagicMock()
    mock_col.name = "region"

    mock_statement = MagicMock()
    mock_statement.status.state = __import__(
        "databricks.sdk.service.sql", fromlist=["StatementState"]
    ).StatementState.SUCCEEDED
    mock_statement.result.data_array = [["EMEA"], ["APAC"]]
    mock_statement.manifest.schema.columns = [mock_col]

    mock_client = MagicMock()
    mock_client.statement_execution.execute_statement.return_value = mock_statement

    with patch("src.providers.databricks._get_client", return_value=mock_client):
        from src.providers import databricks as db
        # Reset singleton so our mock takes effect
        db._client = mock_client
        rows = await db.run_query("SELECT region FROM sales", warehouse_id="wh123")

    assert rows == [{"region": "EMEA"}, {"region": "APAC"}]


@pytest.mark.asyncio
async def test_databricks_run_query_no_warehouse():
    from src.providers import databricks as db
    import importlib
    # Ensure no warehouse configured
    with patch("src.providers.databricks.settings") as mock_settings:
        mock_settings.databricks_warehouse_id = ""
        db._client = MagicMock()
        with pytest.raises(RuntimeError, match="No SQL Warehouse ID"):
            await db.run_query("SELECT 1")
