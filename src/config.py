"""
Cloud Data MCP — Settings
Loaded once at startup from environment variables (or a .env file).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Azure ──────────────────────────────────────────────────────────────────
    azure_storage_account: str = ""
    azure_tenant_id: str = ""
    azure_client_id: str = ""
    azure_client_secret: str = ""

    # ── AWS ───────────────────────────────────────────────────────────────────
    aws_region: str = "us-east-1"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_session_token: str = ""
    # Comma-separated allowlist, e.g. "bucket-a,bucket-b". Empty = all buckets.
    s3_allowed_buckets: str = ""

    # ── Databricks ────────────────────────────────────────────────────────────
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_client_id: str = ""
    databricks_client_secret: str = ""
    databricks_warehouse_id: str = ""

    # ── General ───────────────────────────────────────────────────────────────
    max_query_rows: int = 5000

    # ── Derived ───────────────────────────────────────────────────────────────
    @property
    def azure_enabled(self) -> bool:
        return bool(self.azure_storage_account)

    @property
    def databricks_enabled(self) -> bool:
        return bool(self.databricks_host)

    @property
    def s3_allowed_buckets_list(self) -> list[str]:
        if not self.s3_allowed_buckets:
            return []
        return [b.strip() for b in self.s3_allowed_buckets.split(",") if b.strip()]


settings = Settings()
