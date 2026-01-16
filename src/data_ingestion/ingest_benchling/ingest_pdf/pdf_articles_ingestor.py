"""
Benchling PDF Articles Ingestor

Processes PDF files from Benchling S3 bucket.
Converts PDF -> HTML -> BioC XML, extracts tables.
Skips NER annotation steps.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import logging

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

# Reuse existing PDF processing functions
from src.data_ingestion.ingest_preprints_rxivs.preprint_pdf_to_bioc_converter import (
    extract_pages_block_level_simple,
    make_document_from_blocks,
    build_bioc_collection_lib,
)

import bioc
from datetime import datetime
import json
import uuid

logger = logging.getLogger(__name__)


class BenchlingPDFIngestor:
    """
    PDF ingestor for Benchling documents.
    
    Pipeline:
    1. Read PDF using PyMuPDF
    2. Extract text blocks
    3. Convert to BioC XML
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
    
    def extract_text_from_pdf(self, pdf_path: str) -> list:
        """Extract text blocks from PDF using PyMuPDF."""
        try:
            kept_blocks_per_page = extract_pages_block_level_simple(
                pdf_path=pdf_path,
                table_thresh=0.2,
            )
            return kept_blocks_per_page
        except Exception as e:
            logger.error(f"Failed to extract text from PDF {pdf_path}: {e}")
            raise
    
    def convert_to_bioc(
        self,
        document_id: str,
        kept_blocks_per_page: list,
        metadata: Dict[str, Any],
    ) -> bioc.BioCCollection:
        """Convert extracted blocks to BioC collection."""
        
        doc_dict = make_document_from_blocks(
            doc_id=document_id,
            kept_blocks_per_page=kept_blocks_per_page,
            infons=metadata,
            min_words=50,
        )
        
        bioc_collection = build_bioc_collection_lib(
            source=self.source,
            date_str=datetime.now().strftime("%Y-%m-%d"),
            documents=[doc_dict],
        )
        
        return bioc_collection
    
    def save_bioc_xml(self, bioc_collection: bioc.BioCCollection, output_path: str):
        """Save BioC collection to XML file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            bioc.dump(bioc_collection, f)
        logger.info(f"Saved BioC XML to {output_path}")
    
    def extract_metadata(self, file_name: str, file_path: str) -> Dict[str, Any]:
        """Extract metadata from PDF file."""
        document_grsar_id = file_name.split(".")[0]
        
        metadata = {
            "document_grsar_id": document_grsar_id,
            "source": self.source,
            "file_name": file_name,
            "file_path": file_path,
            "workflow_id": self.workflow_id,
            "processing_date": datetime.now().isoformat(),
            "file_type": "pdf",
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
        Run the PDF ingestion pipeline.
        
        Args:
            file_name: Name of PDF file in ingestion directory
        """
        if not file_name.lower().endswith(".pdf"):
            logger.warning(f"{file_name} is not a PDF file")
            return
        
        logger.info(f"Processing PDF: {file_name}")
        
        document_id = file_name.split(".")[0]
        pdf_path = os.path.join(self.ingestion_path, file_name)
        
        try:
            # Step 1: Extract metadata
            metadata = self.extract_metadata(file_name, pdf_path)
            self.save_metadata(metadata, document_id)
            
            # Step 2: Extract text from PDF
            logger.info(f"Extracting text from {file_name}")
            kept_blocks_per_page = self.extract_text_from_pdf(pdf_path)
            
            total_blocks = sum(len(page) for page in kept_blocks_per_page)
            logger.info(f"Extracted {total_blocks} blocks from {len(kept_blocks_per_page)} pages")
            
            # Step 3: Convert to BioC XML
            logger.info(f"Converting to BioC XML")
            bioc_collection = self.convert_to_bioc(
                document_id=document_id,
                kept_blocks_per_page=kept_blocks_per_page,
                metadata=metadata,
            )
            
            # Step 4: Save BioC XML
            bioc_output_path = os.path.join(self.bioc_path, f"{document_id}.xml")
            self.save_bioc_xml(bioc_collection, bioc_output_path)
            
            logger.info(f"Successfully processed PDF: {file_name}")
            
        except Exception as e:
            logger.error(f"Failed to process PDF {file_name}: {e}")
            
            # Move to failed directory
            failed_path = os.path.join(self.failed_path, file_name)
            if os.path.exists(pdf_path):
                Path(self.failed_path).mkdir(parents=True, exist_ok=True)
                self.file_handler.move_file(pdf_path, failed_path)
            
            raise
