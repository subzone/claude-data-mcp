"""Shared fixtures for cloud-data-mcp tests."""
import pytest


@pytest.fixture(autouse=True)
def reset_duckdb_singleton():
    """Reset the DuckDB connection singleton so each test initialises fresh."""
    import src.engine.duckdb as duckdb_module
    duckdb_module._conn = None
    yield
    duckdb_module._conn = None


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    """Ensure no real cloud credentials leak into unit tests."""
    for key in [
        "AZURE_STORAGE_ACCOUNT", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
        "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_WAREHOUSE_ID",
    ]:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def azure_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-id")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-id")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "client-secret")


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testsecret")


@pytest.fixture
def databricks_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "abc123")
