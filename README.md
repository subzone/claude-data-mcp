# cloud-data-mcp

> Query Azure Storage, AWS S3, and Databricks from Claude using natural language.
> Powered by DuckDB — no data warehouse required.

**Author:** [YOUR_NAME](https://github.com/YOUR_GITHUB)
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

## Claude Code (Plugin)

```bash
/plugin install https://github.com/YOUR_GITHUB/cloud-data-mcp
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
# claude-data-mcp
