"""
Benchling Articles Extractor

Extracts documents from Benchling S3 bucket and prepares them for processing.
Similar to Apollo extractor but optimized for Databricks environment.
"""

import hashlib
import os
import re
import unicodedata
from pathlib import PurePosixPath
from typing import Dict, List, Optional

from src.data_ingestion.ingest_benchling.benchling_s3_client import BenchlingS3Client
from src.data_ingestion.ingest_benchling.databricks_delta_handler import DatabricksDeltaHandler
from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig

import logging

logger = logging.getLogger(__name__)


# Temp file patterns to skip
TEMP_PREFIXES = ("~$", ".DS_Store", "Thumbs.db")
TEMP_EXTS = {".tmp", ".db", ".lnk"}


def stable_hash(path: str) -> str:
    """Generate stable document ID from path using SHA256."""
    return hashlib.sha256(path.encode("utf-8")).hexdigest()


def clean_path_str(s: str) -> str:
    """
    Normalize unicode and remove invisible/nuisance characters.
    """
    if not isinstance(s, str):
        return s

    s = unicodedata.normalize("NFKC", s)

    # Remove invisible nuisance characters
    for ch in ("\u00AD", "\u200B", "\u200C", "\uFEFF", "\u00A0"):
        s = s.replace(ch, "")

    # Various dashes -> hyphen
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", s)

    # Normalize curly quotes and apostrophes
    s = re.sub(r"[''`']", "-", s)

    # Collapse whitespace around slashes and hyphens
    s = re.sub(r"\s*[/\\]\s*", "/", s)
    s = re.sub(r"\s*-\s*", "-", s)

    return s


def is_temp_file(file_path: str) -> bool:
    """Check if file is a temporary/hidden file."""
    s_clean = clean_path_str(file_path)
    p = PurePosixPath(s_clean)
    filename = p.name
    extension = p.suffix.lower()
    
    return (
        filename.startswith("~$")
        or any(pref in s_clean for pref in TEMP_PREFIXES)
        or extension in TEMP_EXTS
    )


def make_safe_filename(filename: str, max_len: Optional[int] = None) -> str:
    """
    Produce a safe filename by replacing non-alphanumeric characters with '_'.
    """
    p = PurePosixPath(filename)
    stem = p.stem
    ext = p.suffix

    safe_stem = re.sub(r"[^A-Za-z0-9]", "_", stem)
    safe_stem = re.sub(r"_+", "_", safe_stem).strip("_")

    if max_len and len(safe_stem) > max_len:
        safe_stem = safe_stem[:max_len]

    if not safe_stem:
        safe_stem = "file"

    return f"{safe_stem}{ext}"


def extract_benchling_articles(
    config: BenchlingConfig,
    local_staging_path: str,
    workflow_id: Optional[str] = None,
    file_types: Optional[List[str]] = None,
    source: str = "benchling",
    write_to_delta: bool = True,
) -> Dict[str, str]:
    """
    Extract documents from Benchling S3 bucket to local staging directory.
    
    Args:
        config: Benchling configuration
        local_staging_path: Local directory to download files to
        workflow_id: Processing workflow ID
        file_types: List of file extensions to include (None = use config)
        source: Source name for metadata
        write_to_delta: Whether to write document records to Delta table
        
    Returns:
        Dictionary mapping S3 path -> document_grsar_id
    """
    # Initialize S3 client
    s3_client = BenchlingS3Client(
        bucket_name=config.s3.bucket_name,
        bucket_region=config.s3.bucket_region,
    )
    
    # Initialize Delta handler if needed
    delta_handler = None
    if write_to_delta:
        delta_handler = DatabricksDeltaHandler(
            catalog=config.delta.catalog,
            schema=config.delta.schema,
            documents_table=config.delta.documents_table,
            chunks_table=config.delta.chunks_table,
        )
    
    # Determine file types to extract
    if file_types is None:
        file_types = config.allowed_file_types
    
    # List files from S3
    all_files = s3_client.list_files(
        prefix=config.s3.source_prefix,
        file_types=file_types,
    )
    
    # Filter out temp files
    filtered_files = [f for f in all_files if not is_temp_file(f)]
    
    logger.info(f"Found {len(filtered_files)} files to extract (filtered from {len(all_files)})")
    
    # Ensure staging directory exists
    os.makedirs(local_staging_path, exist_ok=True)
    
    files_to_grsar_id = {}
    
    for s3_path in filtered_files:
        # Generate stable document ID
        document_grsar_id = stable_hash(s3_path)
        
        # Extract file info
        original_filename = s3_path.split("/")[-1]
        extension = original_filename.split(".")[-1].lower() if "." in original_filename else ""
        safe_filename = f"{document_grsar_id}.{extension}"
        
        # Download to local staging
        local_path = os.path.join(local_staging_path, safe_filename)
        success = s3_client.download_file(s3_path, local_path)
        
        if not success:
            logger.warning(f"Failed to download: {s3_path}")
            continue
        
        # Get file size
        size_bytes = os.path.getsize(local_path) if os.path.exists(local_path) else 0
        
        # Insert document record to Delta table
        if delta_handler:
            delta_handler.insert_document(
                document_grsar_id=document_grsar_id,
                source=source,
                file_name=original_filename,
                file_path=s3_path,
                safe_file_name=safe_filename,
                workflow_id=workflow_id,
                size_bytes=size_bytes,
            )
        
        files_to_grsar_id[s3_path] = document_grsar_id
        logger.info(f"Extracted: {s3_path} -> {document_grsar_id}")
    
    logger.info(f"Extracted {len(files_to_grsar_id)} files from Benchling S3")
    
    return files_to_grsar_id


def extract_single_file(
    config: BenchlingConfig,
    s3_path: str,
    local_staging_path: str,
    workflow_id: Optional[str] = None,
    source: str = "benchling",
    write_to_delta: bool = True,
) -> Optional[str]:
    """
    Extract a single file from S3.
    
    Args:
        config: Benchling configuration
        s3_path: Full S3 key path
        local_staging_path: Local directory to download to
        workflow_id: Processing workflow ID
        source: Source name
        write_to_delta: Whether to write to Delta table
        
    Returns:
        document_grsar_id if successful, None otherwise
    """
    s3_client = BenchlingS3Client(
        bucket_name=config.s3.bucket_name,
        bucket_region=config.s3.bucket_region,
    )
    
    # Generate document ID
    document_grsar_id = stable_hash(s3_path)
    original_filename = s3_path.split("/")[-1]
    extension = original_filename.split(".")[-1].lower() if "." in original_filename else ""
    safe_filename = f"{document_grsar_id}.{extension}"
    
    # Download file
    local_path = os.path.join(local_staging_path, safe_filename)
    os.makedirs(local_staging_path, exist_ok=True)
    
    if not s3_client.download_file(s3_path, local_path):
        return None
    
    # Write to Delta if requested
    if write_to_delta:
        delta_handler = DatabricksDeltaHandler(
            catalog=config.delta.catalog,
            schema=config.delta.schema,
            documents_table=config.delta.documents_table,
            chunks_table=config.delta.chunks_table,
        )
        size_bytes = os.path.getsize(local_path) if os.path.exists(local_path) else 0
        delta_handler.insert_document(
            document_grsar_id=document_grsar_id,
            source=source,
            file_name=original_filename,
            file_path=s3_path,
            safe_file_name=safe_filename,
            workflow_id=workflow_id,
            size_bytes=size_bytes,
        )
    
    return document_grsar_id
