"""
Benchling Metadata Loader

Utility functions to load Benchling JSON metadata from S3 and enrich documents.
"""

import json
import boto3
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def load_benchling_metadata_from_s3(
    bucket_name: str,
    base_prefix: str = "benchling_unstructured/",
) -> Dict[str, dict]:
    """
    Scans S3 for metadata JSON files and returns:
      { entry_id -> metadata_dict }
    
    Args:
        bucket_name: S3 bucket name
        base_prefix: Base prefix to search for metadata files
        
    Returns:
        Dictionary mapping entry_id to metadata
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    metadata_by_entry = {}

    for page in paginator.paginate(Bucket=bucket_name, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.lower().endswith(".json"):
                continue

            # Skip non-metadata JSON if needed
            if not key.lower().endswith(("metadata.json", "_metadata.json")):
                continue

            logger.debug(f"[metadata] Reading {key}")

            try:
                response = s3.get_object(Bucket=bucket_name, Key=key)
                payload = json.loads(response["Body"].read())

                entry_id = payload.get("id")
                if not entry_id:
                    continue

                metadata_by_entry[entry_id] = {
                    "name": payload.get("name"),
                    "display_id": payload.get("displayId"),
                    "folder_id": payload.get("folderId"),
                    "created_at": payload.get("createdAt"),
                    "modified_at": payload.get("modifiedAt"),
                    "authors": payload.get("authors"),
                    "creator": payload.get("creator"),
                }

            except Exception as e:
                logger.warning(f"Failed to load metadata {key}: {e}")

    logger.info(f"Loaded metadata for {len(metadata_by_entry)} entries")
    return metadata_by_entry


def parse_benchling_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse Benchling datetime string to datetime object.
    
    Args:
        dt_str: ISO datetime string from Benchling
        
    Returns:
        datetime object or None
    """
    if not dt_str:
        return None
    
    try:
        # Benchling uses ISO 8601 format
        if "T" in dt_str:
            # Remove timezone info if present (keep naive datetime)
            dt_str = dt_str.split("+")[0].split("Z")[0]
            return datetime.fromisoformat(dt_str)
        else:
            return datetime.fromisoformat(dt_str)
    except Exception as e:
        logger.warning(f"Failed to parse datetime {dt_str}: {e}")
        return None


def get_metadata_for_entry(
    entry_id: str,
    metadata_by_entry: Dict[str, dict],
    s3_path: Optional[str] = None,
) -> Dict:
    """
    Get metadata dictionary for a specific entry, formatted for Delta table insertion.
    
    Args:
        entry_id: Benchling entry ID
        metadata_by_entry: Dictionary from load_benchling_metadata_from_s3
        s3_path: Optional S3 path to extract project/date info
        
    Returns:
        Dictionary with formatted metadata fields
    """
    benchling_meta = metadata_by_entry.get(entry_id)
    if not benchling_meta:
        return {}
    
    # Extract project and date from S3 path if provided
    project = None
    date = None
    if s3_path:
        parts = s3_path.split("/")
        # Look for date pattern YYYY-MM-DD
        import re
        date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
        for part in parts:
            if date_pattern.search(part):
                date = date_pattern.search(part).group()
                break
        # Project is usually the directory before entry_id
        if "benchling_unstructured" in s3_path:
            benchling_idx = s3_path.find("benchling_unstructured")
            after_benchling = s3_path[benchling_idx + len("benchling_unstructured/"):]
            project_parts = after_benchling.split("/")
            if len(project_parts) > 0:
                project = project_parts[0]
    
    return {
        "entry_id": entry_id,
        "name": benchling_meta.get("name"),
        "display_id": benchling_meta.get("display_id"),
        "folder_id": benchling_meta.get("folder_id"),
        "benchling_created_at": parse_benchling_datetime(benchling_meta.get("created_at")),
        "benchling_modified_at": parse_benchling_datetime(benchling_meta.get("modified_at")),
        "authors": benchling_meta.get("authors"),
        "creator": benchling_meta.get("creator"),
        "project": project,
        "date": date,
    }
