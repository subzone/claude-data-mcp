"""Tests for src/config.py settings loading."""
import pytest
from src.config import Settings


def test_defaults_all_empty():
    s = Settings()
    assert s.azure_storage_account == ""
    assert s.databricks_host == ""
    assert s.aws_region == "us-east-1"
    assert s.max_query_rows == 5000


def test_azure_enabled_flag():
    s = Settings(azure_storage_account="myaccount")
    assert s.azure_enabled is True

    s2 = Settings()
    assert s2.azure_enabled is False


def test_databricks_enabled_flag():
    s = Settings(databricks_host="https://adb-test.net")
    assert s.databricks_enabled is True


def test_s3_allowed_buckets_list_parsing():
    s = Settings(s3_allowed_buckets="bucket-a,bucket-b, bucket-c ")
    assert s.s3_allowed_buckets_list == ["bucket-a", "bucket-b", "bucket-c"]


def test_s3_allowed_buckets_empty():
    s = Settings()
    assert s.s3_allowed_buckets_list == []


def test_env_override(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "envaccount")
    monkeypatch.setenv("MAX_QUERY_ROWS", "999")
    s = Settings()
    assert s.azure_storage_account == "envaccount"
    assert s.max_query_rows == 999
