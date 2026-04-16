"""
Cloud Data MCP — application entry point (lives inside the src package).
server.py at the repo root is a thin shim that calls this.
"""

from __future__ import annotations

import logging
import os
import sys

from fastmcp import FastMCP

from src.config import settings
from src.tools.tools import register_tools

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="cloud-data-mcp",
    instructions="""You are connected to a cloud data analytics connector that can query:
- Azure Blob Storage (CSV, Parquet, JSON files) and Azure Table Storage
- AWS S3 (CSV, Parquet, JSON files)
- Databricks Unity Catalog and SQL Warehouses

Workflow for answering data questions:
1. Call discover() to see what data sources are available
2. Call get_schema() or sample() to understand the structure of relevant datasets
3. Call query() with SQL to answer the user's specific question
4. Summarise the results in plain language

Always prefer targeted SQL queries over broad scans to minimise cost and latency.
When querying Parquet files, DuckDB will only read the columns your SQL references.""",
)

register_tools(mcp)


def main() -> None:
    transport = os.environ.get("TRANSPORT", "stdio").lower()
    active = []
    if settings.azure_enabled:
        active.append(f"Azure ({settings.azure_storage_account})")
    active.append("S3")
    if settings.databricks_enabled:
        active.append(f"Databricks ({settings.databricks_host})")
    logger.info(f"Active providers: {', '.join(active)}")

    if transport == "http":
        port = int(os.environ.get("PORT", "8000"))
        logger.info(f"Starting HTTP server on port {port}")
        mcp.run(transport="streamable-http", host="0.0.0.0", port=port, path="/mcp")
    else:
        logger.info("Starting stdio server")
        mcp.run(transport="stdio")
