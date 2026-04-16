# cloud-data-mcp

> Query Azure Storage, AWS S3, and Databricks from Claude using natural language.
> Powered by DuckDB — no data warehouse required.

[![CI](https://github.com/subzone/cloud-data-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/subzone/cloud-data-mcp/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/cloud-data-mcp)](https://pypi.org/project/cloud-data-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Author:** Milenko Mitrovic (https://github.com/subzone)
**License:** MIT

---

## What It Does

Business users ask questions in plain language. Claude queries your data and answers them directly.

```
User: "What was total revenue by region in Q3 2024?"

Claude:
  1. Discovers available data sources
  2. Infers schema from sales/*.parquet
  3. Runs: SELECT region, SUM(revenue) FROM 'az://...sales/2024/*.parquet' WHERE quarter=3 GROUP BY region
  4. Returns answer in plain English
```

DuckDB queries Parquet and CSV files **directly in blob storage** — no download, no ETL, no extra infrastructure.

---

## Supported Sources

| Source | Formats | Query Engine |
|---|---|---|
| Azure Blob Storage | CSV, Parquet, JSON, NDJSON | DuckDB |
| Azure Table Storage | NoSQL entities | Azure SDK (OData) |
| AWS S3 | CSV, Parquet, JSON | DuckDB |
| Databricks Unity Catalog | Delta, Parquet, CSV | Databricks SQL Warehouse |

---

## Tools

| Tool | Description |
|---|---|
| `discover` | List all containers, buckets, and Databricks catalogs |
| `get_schema` | Infer column names and types from any file or table |
| `query` | SQL against Azure/S3 (DuckDB) or Databricks (SQL Warehouse) |
| `query_table_storage` | OData queries against Azure Table Storage |
| `sample` | Return N rows from any source |
| `list_tables` | Browse Databricks Unity Catalog hierarchy |
| `describe_table` | Full column metadata for a Databricks table |

---

## Setup

### 1. Install

```bash
git clone https://github.com/YOUR_GITHUB/cloud-data-mcp
cd cloud-data-mcp
pip install -e .
```

### 2. Configure

Copy `.env.example` to `.env` and fill in what you need:

```bash
cp .env.example .env
```

```env
# Azure — only AZURE_STORAGE_ACCOUNT is required if you've run `az login`
AZURE_STORAGE_ACCOUNT=mystorageaccount

# Azure Service Principal (optional — for CI/CD or production)
AZURE_TENANT_ID=
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=

# AWS — leave empty if using ~/.aws/credentials or IAM role
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

# Databricks
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=                  # PAT — leave empty for OAuth
DATABRICKS_CLIENT_ID=              # OAuth M2M
DATABRICKS_CLIENT_SECRET=          # OAuth M2M
DATABRICKS_WAREHOUSE_ID=
```

### 3. Authentication

| Provider | How it works |
|---|---|
| **Azure** | `az login` is enough for local use. For production, attach a Managed Identity or set Service Principal env vars. Required RBAC: `Storage Blob Data Reader` + `Storage Table Data Reader` |
| **AWS S3** | `aws configure` or IAM role. Required policy: `s3:ListAllMyBuckets`, `s3:GetObject`, `s3:ListBucket` |
| **Databricks** | Set `DATABRICKS_TOKEN` (PAT) or `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET` (OAuth M2M) |

---

## Claude Desktop (stdio)

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cloud-data-mcp": {
      "command": "python",
      "args": ["/path/to/cloud-data-mcp/server.py"],
      "env": {
        "AZURE_STORAGE_ACCOUNT": "mystorageaccount",
        "DATABRICKS_HOST": "https://adb-xxxx.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

## Claude.ai / Cowork (Remote Connector)

Deploy as an HTTPS server and add the URL to claude.ai → Settings → Connectors:

```bash
TRANSPORT=http PORT=8000 python server.py
```

The server must be publicly reachable from Anthropic's infrastructure.

## Claude Code (local MCP server)

```bash
# Option A — from PyPI (recommended, no clone needed)
claude mcp add cloud-data-mcp \
  -e AZURE_STORAGE_ACCOUNT=mystorageaccount \
  -e DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net \
  -e DATABRICKS_TOKEN=dapi... \
  -- uvx cloud-data-mcp

# Option B — from source (for development)
git clone https://github.com/subzone/cloud-data-mcp
cd cloud-data-mcp && pip install -e .
claude mcp add cloud-data-mcp \
  -e AZURE_STORAGE_ACCOUNT=mystorageaccount \
  -- python /path/to/cloud-data-mcp/server.py
```

Or add manually to `~/.claude.json` (or your project's `.claude/settings.json`):

```json
{
  "mcpServers": {
    "cloud-data-mcp": {
      "command": "uvx",
      "args": ["cloud-data-mcp"],
      "env": {
        "AZURE_STORAGE_ACCOUNT": "mystorageaccount",
        "DATABRICKS_HOST": "https://adb-xxxx.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

Restart Claude Code after making changes. Run `claude mcp list` to confirm it appears.

---

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Lint
ruff check . && ruff format --check .

# Test (no cloud credentials required)
pytest -v

# Run the server locally for manual testing
python server.py
```

**Releasing a new version:**

```bash
# Bump version in pyproject.toml, commit, then:
git tag v1.0.1
git push origin v1.0.1
```

The Release workflow will:
1. Run the full test suite (gates the release)
2. Build the wheel + sdist
3. Create a GitHub Release with auto-generated changelog
4. Publish to PyPI via OIDC trusted publishing

**One-time PyPI trusted publishing setup** (no API token needed):
1. Create the project on [pypi.org](https://pypi.org)
2. Go to **Manage → Publishing → Add publisher**
   - Owner: `subzone`, Repo: `cloud-data-mcp`, Workflow: `release.yml`
3. Add a `pypi` environment in GitHub repo **Settings → Environments**

---

## Project Structure

```
cloud-data-mcp/
├── src/
│   ├── app.py               # FastMCP server setup + main()
│   ├── config.py            # Settings (pydantic-settings, reads .env)
│   ├── engine/
│   │   └── duckdb.py        # DuckDB engine with Azure + S3 secret injection
│   ├── providers/
│   │   ├── azure.py         # Azure Blob Storage + Table Storage
│   │   ├── s3.py            # AWS S3
│   │   └── databricks.py    # Databricks Unity Catalog + SQL Warehouse
│   └── tools/
│       └── tools.py         # All 7 MCP tool definitions
├── tests/
│   ├── conftest.py          # Fixtures (isolated from real cloud creds)
│   ├── test_config.py       # Settings unit tests
│   └── test_tools.py        # Engine + provider tests (mocked)
├── .github/workflows/
│   ├── ci.yml               # Lint + test on every PR and push to main
│   └── release.yml          # Build + publish on version tag push
├── server.py                # Shim: `python server.py` for local dev
├── pyproject.toml
└── .env.example
```

---

## Publish to Anthropic Connectors Directory

To list this connector at [claude.com/connectors](https://claude.com/connectors):

1. Deploy your server to a public HTTPS endpoint
2. Go to [claude.com/connectors](https://claude.com/connectors)
3. Click **"Share yours"**
4. Submit your MCP server URL and description

Your name and GitHub will be shown as the author in the directory.

---

## WAF Alignment

| Pillar | Implementation |
|---|---|
| **Security** | DefaultAzureCredential, AWS credential chain — no hardcoded secrets |
| **Reliability** | Per-tool error handling, graceful provider degradation |
| **Performance** | DuckDB column pruning, predicate pushdown, range reads |
| **Cost** | Queries read only referenced columns from Parquet — minimal egress |
| **Operations** | Structured logging, provider health in `discover` output |
