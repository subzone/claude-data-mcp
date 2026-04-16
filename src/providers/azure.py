"""
Cloud Data MCP — Azure Provider
Discovery: containers and blobs via Azure SDK (DefaultAzureCredential).
Table Storage: OData queries via azure-data-tables.
Blob file queries are handled by DuckDB engine directly.
"""
from __future__ import annotations

import logging

from azure.data.tables import TableServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.config import settings

logger = logging.getLogger(__name__)

_credential: DefaultAzureCredential | None = None
_blob_client: BlobServiceClient | None = None
_table_client: TableServiceClient | None = None


def _get_credential() -> DefaultAzureCredential:
    global _credential
    if _credential is None:
        # Tries in order: env vars (SP), Workload Identity, MSI, az login, browser
        _credential = DefaultAzureCredential()
    return _credential


def _get_blob_client() -> BlobServiceClient:
    global _blob_client
    if _blob_client is None:
        url = f"https://{settings.azure_storage_account}.blob.core.windows.net"
        _blob_client = BlobServiceClient(url, credential=_get_credential())
    return _blob_client


def _get_table_client() -> TableServiceClient:
    global _table_client
    if _table_client is None:
        url = f"https://{settings.azure_storage_account}.table.core.windows.net"
        _table_client = TableServiceClient(url, credential=_get_credential())
    return _table_client


# ── Discovery ──────────────────────────────────────────────────────────────────

async def list_containers() -> list[dict]:
    """List all blob containers in the storage account."""
    client = _get_blob_client()
    containers = []
    for c in client.list_containers(include_metadata=True):
        containers.append({
            "name": c["name"],
            "last_modified": str(c["last_modified"]) if c.get("last_modified") else None,
            "public_access": c.get("public_access"),
            "metadata": c.get("metadata") or {},
        })
    return containers


async def list_blobs(container: str, prefix: str | None = None, max_results: int = 200) -> list[dict]:
    """List blobs in a container with optional prefix filter."""
    client = _get_blob_client().get_container_client(container)
    blobs = []
    count = 0
    for b in client.list_blobs(name_starts_with=prefix, include=["metadata"]):
        if count >= max_results:
            break
        blobs.append({
            "name": b["name"],
            "size_bytes": b["size"],
            "content_type": b.get("content_settings", {}).get("content_type"),
            "last_modified": str(b["last_modified"]) if b.get("last_modified") else None,
            "tier": b.get("blob_tier"),
        })
        count += 1
    return blobs


# ── Table Storage ──────────────────────────────────────────────────────────────

async def list_tables() -> list[str]:
    """List all tables in Azure Table Storage."""
    client = _get_table_client()
    return [t["name"] for t in client.list_tables()]


async def query_table(
    table_name: str,
    filter_query: str | None = None,
    select: list[str] | None = None,
    max_results: int = 200,
) -> list[dict]:
    """
    Query Azure Table Storage with an OData filter expression.
    Examples:
      filter_query="PartitionKey eq 'sales' and Year eq '2024'"
      filter_query="Amount gt 1000"
    """
    client = _get_table_client().get_table_client(table_name)
    entities = []
    count = 0

    query_params: dict = {}
    if filter_query:
        query_params["query_filter"] = filter_query
    if select:
        query_params["select"] = select

    for entity in client.list_entities(**query_params):
        if count >= max_results:
            break
        # Convert entity to plain dict, skip internal Azure metadata keys
        row = {k: v for k, v in entity.items() if not k.startswith("odata.")}
        entities.append(row)
        count += 1

    return entities


async def sample_table(table_name: str, n: int = 25) -> list[dict]:
    """Return the first N entities from a table to understand its structure."""
    return await query_table(table_name, max_results=n)
