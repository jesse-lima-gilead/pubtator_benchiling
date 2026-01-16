"""
Benchling S3 Client for Databricks

S3 client that uses Databricks instance profile for authentication.
No AWS secrets required - relies on IAM role attached to Databricks cluster.
"""

import os
import boto3
from botocore.exceptions import ClientError
from typing import List, Optional, BinaryIO
import logging

logger = logging.getLogger(__name__)


class BenchlingS3Client:
    """
    S3 client for accessing Benchling data from Databricks.
    Uses instance profile authentication (no explicit credentials needed).
    """
    
    def __init__(
        self,
        bucket_name: str,
        bucket_region: str = "us-west-2",
    ):
        """
        Initialize S3 client.
        
        Args:
            bucket_name: S3 bucket name
            bucket_region: AWS region for the bucket
        """
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        
        # In Databricks, boto3 automatically uses instance profile
        # No explicit credentials needed
        self.s3 = boto3.resource("s3", region_name=bucket_region)
        self.bucket = self.s3.Bucket(bucket_name)
        self.client = boto3.client("s3", region_name=bucket_region)
        
        logger.info(f"Initialized S3 client for bucket: {bucket_name}")
    
    def list_files(self, prefix: str = "", file_types: Optional[List[str]] = None) -> List[str]:
        """
        List files in S3 bucket with optional prefix and file type filter.
        
        Args:
            prefix: S3 prefix to filter files
            file_types: List of file extensions to include (e.g., ["pdf", "docx"])
            
        Returns:
            List of S3 keys
        """
        try:
            files = []
            for obj in self.bucket.objects.filter(Prefix=prefix):
                key = obj.key
                # Skip directory markers
                if key.endswith("/"):
                    continue
                
                # Filter by file type if specified
                if file_types:
                    ext = key.split(".")[-1].lower() if "." in key else ""
                    if ext not in [ft.lower() for ft in file_types]:
                        continue
                
                # Skip temp/hidden files
                filename = key.split("/")[-1]
                if filename.startswith("~$") or filename.startswith("."):
                    continue
                    
                files.append(key)
            
            logger.info(f"Found {len(files)} files in {prefix}")
            return files
            
        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download file from S3 to local path.
        
        Args:
            s3_key: S3 object key
            local_path: Local file path to save to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists (handle case where dirname returns empty string)
            dir_name = os.path.dirname(local_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            
            self.bucket.download_file(s3_key, local_path)
            logger.info(f"Downloaded {s3_key} to {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            return False
    
    def download_file_content(self, s3_key: str, as_binary: bool = True) -> Optional[bytes]:
        """
        Download file content directly to memory.
        
        Args:
            s3_key: S3 object key
            as_binary: Return as bytes (True) or string (False)
            
        Returns:
            File content or None if failed
        """
        try:
            obj = self.bucket.Object(s3_key)
            response = obj.get()
            content = response["Body"].read()
            
            if not as_binary:
                content = content.decode("utf-8")
            
            return content
            
        except ClientError as e:
            logger.error(f"Failed to download content for {s3_key}: {e}")
            return None
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """
        Upload local file to S3.
        
        Args:
            local_path: Local file path
            s3_key: S3 object key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.bucket.upload_file(local_path, s3_key)
            logger.info(f"Uploaded {local_path} to {s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
    
    def file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def get_file_metadata(self, s3_key: str) -> Optional[dict]:
        """Get file metadata from S3."""
        try:
            obj = self.bucket.Object(s3_key)
            return {
                "size_bytes": obj.content_length,
                "last_modified": obj.last_modified.isoformat(),
                "content_type": obj.content_type,
            }
        except ClientError as e:
            logger.error(f"Failed to get metadata for {s3_key}: {e}")
            return None
