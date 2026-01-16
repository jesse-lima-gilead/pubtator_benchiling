"""
Benchling Ingestion Module

This module provides ingestion capabilities for documents from Benchling S3 storage.
Designed to run in Databricks without AWS secrets (uses instance profile).
Saves chunks and metadata to Delta tables instead of PostgreSQL.
"""

from src.data_ingestion.ingest_benchling.articles_ingestor import BenchlingIngestor
from src.data_ingestion.ingest_benchling.benchling_articles_extractor import (
    extract_benchling_articles,
)
from src.data_ingestion.ingest_benchling.databricks_delta_handler import (
    DatabricksDeltaHandler,
)

__all__ = [
    "BenchlingIngestor",
    "extract_benchling_articles",
    "DatabricksDeltaHandler",
]
