"""
Benchling TXT/CSV Articles Ingestor

Processes TXT and CSV files from Benchling S3 bucket.
Converts to BioC XML format.
Skips NER annotation steps.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import logging
import csv

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

import bioc
from datetime import datetime
import json
import uuid

logger = logging.getLogger(__name__)


class BenchlingTXTIngestor:
    """
    TXT/CSV ingestor for Benchling documents.
    
    Pipeline:
    1. Read TXT/CSV content
    2. Convert to BioC XML
    3. Save metadata
    
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
    
    def read_txt_content(self, file_path: str) -> str:
        """Read TXT file content."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except UnicodeDecodeError:
            # Try with latin-1 encoding
            with open(file_path, "r", encoding="latin-1") as f:
                return f.read()
    
    def read_csv_content(self, file_path: str) -> str:
        """Read CSV file and convert to text."""
        try:
            rows = []
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    rows.append(" | ".join(row))
            return "\n".join(rows)
        except Exception as e:
            logger.error(f"Failed to read CSV {file_path}: {e}")
            return ""
    
    def convert_to_bioc(
        self,
        document_id: str,
        content: str,
        metadata: Dict[str, Any],
    ) -> bioc.BioCCollection:
        """Convert text content to BioC collection."""
        
        # Create BioC collection
        collection = bioc.BioCCollection()
        collection.source = self.source
        collection.date = datetime.now().strftime("%Y-%m-%d")
        collection.key = "benchling_txt"
        
        # Create document
        document = bioc.BioCDocument()
        document.id = document_id
        
        # Add metadata as infons
        for key, value in metadata.items():
            if isinstance(value, (str, int, float, bool)):
                document.infons[key] = str(value)
        
        # Split content into paragraphs
        paragraphs = content.split("\n\n")
        offset = 0
        
        for i, para_text in enumerate(paragraphs):
            if not para_text.strip():
                continue
            
            passage = bioc.BioCPassage()
            passage.offset = offset
            passage.text = para_text.strip()
            passage.infons["type"] = "paragraph"
            passage.infons["section"] = "body"
            
            document.passages.append(passage)
            offset += len(para_text) + 2  # +2 for \n\n
        
        collection.documents.append(document)
        return collection
    
    def save_bioc_xml(self, bioc_collection: bioc.BioCCollection, output_path: str):
        """Save BioC collection to XML file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            bioc.dump(bioc_collection, f)
        logger.info(f"Saved BioC XML to {output_path}")
    
    def extract_metadata(self, file_name: str, file_path: str) -> Dict[str, Any]:
        """Extract metadata from TXT/CSV file."""
        document_grsar_id = file_name.split(".")[0]
        extension = file_name.split(".")[-1].lower()
        
        metadata = {
            "document_grsar_id": document_grsar_id,
            "source": self.source,
            "file_name": file_name,
            "file_path": file_path,
            "workflow_id": self.workflow_id,
            "processing_date": datetime.now().isoformat(),
            "file_type": extension,
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
        Run the TXT/CSV ingestion pipeline.
        
        Args:
            file_name: Name of TXT/CSV file in ingestion directory
        """
        extension = file_name.split(".")[-1].lower()
        if extension not in ("txt", "csv"):
            logger.warning(f"{file_name} is not a TXT/CSV file")
            return
        
        logger.info(f"Processing TXT/CSV: {file_name}")
        
        document_id = file_name.split(".")[0]
        file_path = os.path.join(self.ingestion_path, file_name)
        
        try:
            # Step 1: Extract metadata
            metadata = self.extract_metadata(file_name, file_path)
            self.save_metadata(metadata, document_id)
            
            # Step 2: Read content
            logger.info(f"Reading content from {file_name}")
            if extension == "csv":
                content = self.read_csv_content(file_path)
            else:
                content = self.read_txt_content(file_path)
            
            if not content:
                raise Exception(f"Empty content in {file_name}")
            
            metadata["char_count"] = len(content)
            metadata["word_count"] = len(content.split())
            
            # Step 3: Convert to BioC XML
            logger.info(f"Converting to BioC XML")
            bioc_collection = self.convert_to_bioc(
                document_id=document_id,
                content=content,
                metadata=metadata,
            )
            
            # Step 4: Save BioC XML
            bioc_output_path = os.path.join(self.bioc_path, f"{document_id}.xml")
            self.save_bioc_xml(bioc_collection, bioc_output_path)
            
            logger.info(f"Successfully processed TXT/CSV: {file_name}")
            
        except Exception as e:
            logger.error(f"Failed to process TXT/CSV {file_name}: {e}")
            
            # Move to failed directory
            failed_path = os.path.join(self.failed_path, file_name)
            if os.path.exists(file_path):
                Path(self.failed_path).mkdir(parents=True, exist_ok=True)
                self.file_handler.move_file(file_path, failed_path)
            
            raise
