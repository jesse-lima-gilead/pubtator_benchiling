"""
Benchling DOCX Articles Ingestor

Processes DOCX files from Benchling S3 bucket.
Converts DOCX -> HTML -> BioC XML, extracts tables.
Skips NER annotation steps.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import logging

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor

# Reuse existing DOCX processing functions
from src.data_ingestion.ingest_apollo.ingest_docx.apollo_docx_to_bioc_converter import (
    convert_apollo_html_to_bioc,
)
from src.data_ingestion.ingest_apollo.ingest_docx.apollo_tables_processor import (
    process_tables,
)

from datetime import datetime
import json

logger = logging.getLogger(__name__)


class BenchlingDOCXIngestor:
    """
    DOCX ingestor for Benchling documents.
    
    Pipeline:
    1. Convert DOCX to HTML using Pandoc
    2. Extract tables from HTML
    3. Convert HTML to BioC XML
    4. Save metadata
    
    Note: Skips NER annotation as not applicable for Benchling.
    """
    
    def __init__(
        self,
        workflow_id: str,
        config: BenchlingConfig,
        paths: Dict[str, str],
        file_handler: LocalFileHandler,
        source: str = "benchling",
        write_to_delta: bool = True,
        **kwargs,
    ):
        self.workflow_id = workflow_id
        self.config = config
        self.paths = paths
        self.file_handler = file_handler
        self.source = source
        self.write_to_delta = write_to_delta
        
        self.ingestion_path = paths["ingestion_path"]
        self.interim_path = paths["ingestion_interim_path"]
        self.bioc_path = paths["bioc_path"]
        self.metadata_path = paths["metadata_path"]
        self.chunks_path = paths["chunks_path"]
        self.embeddings_path = paths["embeddings_path"]
        self.failed_path = paths["failed_ingestion_path"]
        
        # Initialize Pandoc processor
        self.pandoc_processor = PandocProcessor(pandoc_executable="pandoc")
    
    def convert_docx_to_html(self, file_name: str) -> Optional[str]:
        """Convert DOCX to HTML using Pandoc."""
        document_id = file_name.replace(".docx", "").replace(".doc", "")
        
        # Create document-specific interim directory
        doc_interim_dir = os.path.join(self.interim_path, document_id)
        Path(doc_interim_dir).mkdir(parents=True, exist_ok=True)
        
        input_path = os.path.join(self.ingestion_path, file_name)
        output_path = os.path.join(doc_interim_dir, f"{document_id}.html")
        
        try:
            self.pandoc_processor.convert(
                input_path=input_path,
                output_path=output_path,
                input_format="docx",
                output_format="html",
                failed_ingestion_path=self.failed_path,
                extract_media_dir=doc_interim_dir,
            )
            
            if os.path.exists(output_path):
                logger.info(f"Converted {file_name} to HTML: {output_path}")
                return output_path
            else:
                logger.error(f"HTML output not created for {file_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to convert {file_name} to HTML: {e}")
            return None
    
    def extract_tables_from_html(
        self,
        html_path: str,
        document_id: str,
        metadata: Dict[str, Any],
    ) -> list:
        """Extract tables from HTML file."""
        try:
            html_content = self.file_handler.read_file(html_path)
            
            doc_interim_dir = os.path.join(self.interim_path, document_id)
            
            # The Apollo process_tables function has different signature:
            # process_tables(html_str, source_filename, output_tables_path, article_metadata_path, table_state)
            # We need to adapt it for Benchling use
            html_with_tables, table_details = process_tables(
                html_str=html_content,
                source_filename=f"{document_id}.html",
                output_tables_path=doc_interim_dir,
                article_metadata_path=self.metadata_path,
                table_state="remove",
            )
            
            # Add additional metadata to each table detail
            for table in table_details:
                if 'payload' in table:
                    table['payload']['document_grsar_id'] = document_id
                    table['payload'].update(metadata)
            
            # Save modified HTML
            self.file_handler.write_file(html_path, html_with_tables)
            
            logger.info(f"Extracted {len(table_details)} tables from {document_id}")
            return table_details
            
        except Exception as e:
            logger.error(f"Failed to extract tables from {html_path}: {e}")
            return []
    
    def convert_html_to_bioc(self, document_id: str):
        """Convert HTML to BioC XML."""
        try:
            convert_apollo_html_to_bioc(
                apollo_file_name=f"{document_id}.docx",
                apollo_interim_path=self.interim_path,
                bioc_path=self.bioc_path,
                metadata_path=self.metadata_path,
            )
            logger.info(f"Converted {document_id} HTML to BioC XML")
            
        except Exception as e:
            logger.error(f"Failed to convert {document_id} to BioC: {e}")
            raise
    
    def extract_metadata(self, file_name: str, file_path: str) -> Dict[str, Any]:
        """Extract metadata from DOCX file."""
        document_grsar_id = file_name.split(".")[0]
        
        metadata = {
            "document_grsar_id": document_grsar_id,
            "source": self.source,
            "file_name": file_name,
            "file_path": file_path,
            "workflow_id": self.workflow_id,
            "processing_date": datetime.now().isoformat(),
            "file_type": "docx",
        }
        
        # Try to get file size
        full_path = os.path.join(self.ingestion_path, file_name)
        if os.path.exists(full_path):
            metadata["size_bytes"] = os.path.getsize(full_path)
        
        return metadata
    
    def save_metadata(self, metadata: Dict[str, Any], document_id: str):
        """Save metadata to JSON file."""
        Path(self.metadata_path).mkdir(parents=True, exist_ok=True)
        metadata_file = os.path.join(self.metadata_path, f"{document_id}_metadata.json")
        
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved metadata to {metadata_file}")
    
    def run(self, file_name: str):
        """
        Run the DOCX ingestion pipeline.
        
        Args:
            file_name: Name of DOCX file in ingestion directory
        """
        if not file_name.lower().endswith((".docx", ".doc")):
            logger.warning(f"{file_name} is not a DOCX file")
            return
        
        logger.info(f"Processing DOCX: {file_name}")
        
        document_id = file_name.split(".")[0]
        docx_path = os.path.join(self.ingestion_path, file_name)
        
        try:
            # Step 1: Extract metadata
            metadata = self.extract_metadata(file_name, docx_path)
            self.save_metadata(metadata, document_id)
            
            # Step 2: Convert DOCX to HTML
            logger.info(f"Converting {file_name} to HTML")
            html_path = self.convert_docx_to_html(file_name)
            
            if not html_path:
                raise Exception(f"Failed to convert {file_name} to HTML")
            
            # Step 3: Extract tables from HTML
            logger.info(f"Extracting tables from HTML")
            table_details = self.extract_tables_from_html(html_path, document_id, metadata)
            
            # Save table details
            if table_details:
                Path(self.embeddings_path).mkdir(parents=True, exist_ok=True)
                tables_file = os.path.join(self.embeddings_path, f"{document_id}_tables.json")
                with open(tables_file, "w", encoding="utf-8") as f:
                    json.dump(table_details, f, indent=2, ensure_ascii=False)
            
            # Step 4: Convert HTML to BioC XML
            logger.info(f"Converting to BioC XML")
            self.convert_html_to_bioc(document_id)
            
            logger.info(f"Successfully processed DOCX: {file_name}")
            
        except Exception as e:
            logger.error(f"Failed to process DOCX {file_name}: {e}")
            
            # Move to failed directory
            failed_path = os.path.join(self.failed_path, file_name)
            if os.path.exists(docx_path):
                Path(self.failed_path).mkdir(parents=True, exist_ok=True)
                self.file_handler.move_file(docx_path, failed_path)
            
            raise
