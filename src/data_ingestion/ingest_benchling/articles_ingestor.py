"""
Benchling Articles Ingestor

Main orchestrator class for Benchling document ingestion.
Follows Apollo pattern but simplified for Databricks environment.
Skips NER annotation and merge annotation steps.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig, get_default_config
from src.data_ingestion.ingest_benchling.benchling_articles_extractor import (
    extract_benchling_articles,
    stable_hash,
)
from src.data_ingestion.ingest_benchling.databricks_delta_handler import DatabricksDeltaHandler

# File-type specific ingestors
from src.data_ingestion.ingest_benchling.ingest_pdf.pdf_articles_ingestor import BenchlingPDFIngestor
from src.data_ingestion.ingest_benchling.ingest_docx.docx_articles_ingestor import BenchlingDOCXIngestor
from src.data_ingestion.ingest_benchling.ingest_xlsx.xlsx_articles_ingestor import BenchlingXLSXIngestor
from src.data_ingestion.ingest_benchling.ingest_pptx.pptx_articles_ingestor import BenchlingPPTXIngestor
from src.data_ingestion.ingest_benchling.ingest_txt.txt_articles_ingestor import BenchlingTXTIngestor

from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

logger = logging.getLogger(__name__)


class BenchlingIngestor:
    """
    Main ingestor class for Benchling documents.
    Coordinates extraction and processing of different file types.
    
    Similar to APOLLOIngestor but:
    - Uses inline config (no YAML)
    - Uses Databricks Delta tables (no PostgreSQL)
    - Uses instance profile S3 access (no AWS secrets)
    - Skips NER annotation steps
    """
    
    def __init__(
        self,
        workflow_id: str,
        config: Optional[BenchlingConfig] = None,
        file_type: str = "all",
        source: str = "benchling",
        write_to_delta: bool = True,
    ):
        """
        Initialize Benchling ingestor.
        
        Args:
            workflow_id: Unique workflow identifier
            config: Benchling configuration (uses default if None)
            file_type: File type to process ("all" or specific extension)
            source: Source name for metadata
            write_to_delta: Whether to write to Delta tables
        """
        self.workflow_id = workflow_id
        self.config = config or get_default_config()
        self.file_type = file_type
        self.source = source
        self.write_to_delta = write_to_delta
        
        # Get paths for this workflow
        self.paths = self.config.paths.get_paths(workflow_id, source)
        
        # Initialize file handler
        self.file_handler = LocalFileHandler()
        
        # Initialize Delta handler
        self.delta_handler = None
        if write_to_delta:
            self.delta_handler = DatabricksDeltaHandler(
                catalog=self.config.delta.catalog,
                schema=self.config.delta.schema,
                documents_table=self.config.delta.documents_table,
                chunks_table=self.config.delta.chunks_table,
            )
        
        # Create all directories
        self._create_directories()
        
        # Initialize file-type specific ingestors
        self._init_ingestors()
        
        logger.info(f"Initialized BenchlingIngestor for workflow: {workflow_id}")
    
    def _create_directories(self):
        """Create all required directories."""
        for path_name, path_value in self.paths.items():
            Path(path_value).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {path_value}")
    
    def _init_ingestors(self):
        """Initialize file-type specific ingestors."""
        common_kwargs = {
            "workflow_id": self.workflow_id,
            "config": self.config,
            "paths": self.paths,
            "file_handler": self.file_handler,
            "source": self.source,
            "write_to_delta": self.write_to_delta,
        }
        
        self.pdf_ingestor = BenchlingPDFIngestor(**common_kwargs)
        self.docx_ingestor = BenchlingDOCXIngestor(**common_kwargs)
        self.xlsx_ingestor = BenchlingXLSXIngestor(**common_kwargs)
        self.pptx_ingestor = BenchlingPPTXIngestor(**common_kwargs)
        self.txt_ingestor = BenchlingTXTIngestor(**common_kwargs)
        
        # Map extensions to ingestors
        self.ingestor_map = {
            "pdf": self.pdf_ingestor,
            "docx": self.docx_ingestor,
            "doc": self.docx_ingestor,
            "xlsx": self.xlsx_ingestor,
            "xls": self.xlsx_ingestor,
            "pptx": self.pptx_ingestor,
            "ppt": self.pptx_ingestor,
            "txt": self.txt_ingestor,
            "csv": self.txt_ingestor,
        }
    
    def extract(
        self,
        file_types: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Extract documents from Benchling S3 bucket.
        
        Args:
            file_types: List of file types to extract (None = all allowed)
            
        Returns:
            Dictionary mapping S3 path -> document_grsar_id
        """
        logger.info(f"Starting extraction for workflow: {self.workflow_id}")
        
        # Determine file types
        if file_types is None:
            if self.file_type == "all":
                file_types = self.config.allowed_file_types
            else:
                file_types = [self.file_type]
        
        # Extract from S3
        files_map = extract_benchling_articles(
            config=self.config,
            local_staging_path=self.paths["ingestion_path"],
            workflow_id=self.workflow_id,
            file_types=file_types,
            source=self.source,
            write_to_delta=self.write_to_delta,
        )
        
        logger.info(f"Extracted {len(files_map)} files")
        return files_map
    
    def process_file(self, file_name: str) -> bool:
        """
        Process a single file based on its extension.
        
        Args:
            file_name: Name of file in ingestion directory
            
        Returns:
            True if processed successfully
        """
        # Get file extension
        ext = file_name.split(".")[-1].lower() if "." in file_name else ""
        
        # Skip if not in allowed types
        if ext not in self.config.allowed_file_types:
            logger.info(f"Skipping {file_name}: extension {ext} not in allowed types")
            return False
        
        # Get appropriate ingestor
        ingestor = self.ingestor_map.get(ext)
        if ingestor is None:
            logger.warning(f"No ingestor for extension: {ext}")
            return False
        
        # Process file
        try:
            logger.info(f"Processing {file_name} with {type(ingestor).__name__}")
            ingestor.run(file_name=file_name)
            
            # Update document status
            if self.delta_handler:
                document_grsar_id = file_name.split(".")[0]
                self.delta_handler.update_document_status(
                    document_grsar_id=document_grsar_id,
                    status="processed",
                    workflow_id=self.workflow_id,
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {file_name}: {e}")
            
            # Update document status with error
            if self.delta_handler:
                document_grsar_id = file_name.split(".")[0]
                self.delta_handler.update_document_status(
                    document_grsar_id=document_grsar_id,
                    status="failed",
                    workflow_id=self.workflow_id,
                    error_message=str(e),
                )
            
            # Move to failed directory
            src_path = os.path.join(self.paths["ingestion_path"], file_name)
            dst_path = os.path.join(self.paths["failed_ingestion_path"], file_name)
            if os.path.exists(src_path):
                self.file_handler.move_file(src_path, dst_path)
            
            return False
    
    def run(self, extract_first: bool = True) -> Dict[str, Any]:
        """
        Run the full ingestion pipeline.
        
        Args:
            extract_first: Whether to extract from S3 first
            
        Returns:
            Summary of processing results
        """
        logger.info(f"Starting Benchling ingestion pipeline for workflow: {self.workflow_id}")
        
        results = {
            "workflow_id": self.workflow_id,
            "extracted": 0,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
        }
        
        # Step 1: Extract from S3
        if extract_first:
            files_map = self.extract()
            results["extracted"] = len(files_map)
        
        # Step 2: Process each file
        ingestion_path = self.paths["ingestion_path"]
        files_to_process = self.file_handler.list_files(ingestion_path)
        
        for file_name in files_to_process:
            logger.info(f"Processing file: {file_name}")
            
            # Get file extension
            ext = file_name.split(".")[-1].lower() if "." in file_name else ""
            
            # Filter by file type if specified
            if self.file_type != "all" and ext != self.file_type:
                logger.info(f"Skipping {file_name}: not matching file_type {self.file_type}")
                results["skipped"] += 1
                continue
            
            # Skip if not in allowed types
            if ext not in self.config.allowed_file_types:
                logger.info(f"Skipping {file_name}: extension {ext} not allowed")
                results["skipped"] += 1
                continue
            
            # Process
            if self.process_file(file_name):
                results["processed"] += 1
            else:
                results["failed"] += 1
        
        logger.info(f"Ingestion complete: {results}")
        return results
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of current processing state."""
        ingestion_files = self.file_handler.list_files(self.paths["ingestion_path"])
        bioc_files = self.file_handler.list_files(self.paths["bioc_path"])
        failed_files = self.file_handler.list_files(self.paths["failed_ingestion_path"])
        chunks_files = self.file_handler.list_files(self.paths["chunks_path"])
        embeddings_files = self.file_handler.list_files(self.paths["embeddings_path"])
        
        return {
            "workflow_id": self.workflow_id,
            "ingestion_files": len(ingestion_files),
            "bioc_files": len(bioc_files),
            "failed_files": len(failed_files),
            "chunks_files": len(chunks_files),
            "embeddings_files": len(embeddings_files),
            "paths": self.paths,
        }
