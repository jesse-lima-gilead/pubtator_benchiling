"""
Benchling XLSX Articles Ingestor

Processes XLSX files from Benchling S3 bucket.
Converts each sheet to CSV -> HTML, extracts tables.
Skips NER annotation steps.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
import pandas as pd

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

# Reuse existing XLSX processing functions
from src.data_ingestion.ingest_apollo.ingest_xlsx.xlsx_table_processor import (
    process_tables,
)

from datetime import datetime
import json

logger = logging.getLogger(__name__)


class BenchlingXLSXIngestor:
    """
    XLSX ingestor for Benchling documents.
    
    Pipeline:
    1. Read XLSX and extract all sheets
    2. Convert each sheet to CSV -> HTML
    3. Extract tables from HTML
    4. Save metadata and table details
    
    Note: Skips BioC conversion as XLSX is tabular data.
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
    
    def make_safe_filename(self, filename: str, max_len: Optional[int] = None) -> str:
        """Produce a safe filename."""
        safe_stem = re.sub(r"[^A-Za-z0-9]", "_", filename)
        safe_stem = re.sub(r"_+", "_", safe_stem).strip("_")
        
        if max_len and len(safe_stem) > max_len:
            safe_stem = safe_stem[:max_len]
        
        if not safe_stem:
            safe_stem = "file"
        
        return safe_stem
    
    def validate_xlsx(self, xlsx_path: str) -> bool:
        """Validate XLSX file can be opened."""
        try:
            pd.read_excel(xlsx_path, sheet_name=None, nrows=1)
            return True
        except Exception as e:
            logger.error(f"Invalid XLSX file {xlsx_path}: {e}")
            return False
    
    def convert_sheet_to_html(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        document_id: str,
    ) -> Optional[str]:
        """Convert a single sheet to HTML directly using pandas.
        
        Note: Using pandas to_html instead of Pandoc because Pandoc doesn't
        support CSV as an input format directly.
        """
        safe_sheet_name = self.make_safe_filename(sheet_name)
        
        # Create sheet directory
        sheet_dir = os.path.join(self.interim_path, document_id, safe_sheet_name)
        Path(sheet_dir).mkdir(parents=True, exist_ok=True)
        
        # Save as CSV for reference
        csv_path = os.path.join(sheet_dir, f"{safe_sheet_name}.csv")
        df.to_csv(csv_path, index=False)
        
        # Convert DataFrame to HTML directly using pandas
        html_path = os.path.join(sheet_dir, f"{safe_sheet_name}.html")
        
        try:
            # Generate HTML table from DataFrame
            html_content = df.to_html(
                index=False,
                na_rep="",
                classes=["dataframe", "table"],
                border=1,
            )
            
            # Wrap in basic HTML structure
            full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{sheet_name}</title>
</head>
<body>
{html_content}
</body>
</html>"""
            
            # Write HTML file
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(full_html)
            
            if os.path.exists(html_path):
                logger.info(f"Converted sheet '{sheet_name}' to HTML")
                return html_path
                
        except Exception as e:
            logger.error(f"Failed to convert sheet '{sheet_name}' to HTML: {e}")
        
        return None
    
    def extract_tables_from_html(
        self,
        html_path: str,
        sheet_name: str,
        sheet_idx: int,
        document_id: str,
        metadata: Dict[str, Any],
    ) -> list:
        """Extract tables from HTML file."""
        try:
            html_content = self.file_handler.read_file(html_path)
            safe_sheet_name = self.make_safe_filename(sheet_name)
            sheet_dir = os.path.join(self.interim_path, document_id, safe_sheet_name)
            
            html_with_tables, table_details = process_tables(
                html_str=html_content,
                source_filename=f"{safe_sheet_name}.html",
                sheet_idx=sheet_idx,
                output_tables_path=sheet_dir,
                article_metadata=metadata,
                xlsx_filename=document_id,
            )
            
            # Add sheet name to each table
            for table in table_details:
                table['payload']['sheet_name'] = sheet_name
                table['payload']['safe_sheet_name'] = safe_sheet_name
            
            return table_details
            
        except Exception as e:
            logger.error(f"Failed to extract tables from {html_path}: {e}")
            return []
    
    def extract_metadata(self, file_name: str, file_path: str) -> Dict[str, Any]:
        """Extract metadata from XLSX file."""
        document_grsar_id = file_name.split(".")[0]
        
        metadata = {
            "document_grsar_id": document_grsar_id,
            "source": self.source,
            "file_name": file_name,
            "file_path": file_path,
            "workflow_id": self.workflow_id,
            "processing_date": datetime.now().isoformat(),
            "file_type": "xlsx",
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
        Run the XLSX ingestion pipeline.
        
        Args:
            file_name: Name of XLSX file in ingestion directory
        """
        if not file_name.lower().endswith((".xlsx", ".xls")):
            logger.warning(f"{file_name} is not an XLSX file")
            return
        
        logger.info(f"Processing XLSX: {file_name}")
        
        document_id = file_name.split(".")[0]
        xlsx_path = os.path.join(self.ingestion_path, file_name)
        
        try:
            # Validate file
            if not self.validate_xlsx(xlsx_path):
                raise Exception(f"Invalid XLSX file: {file_name}")
            
            # Step 1: Extract metadata
            metadata = self.extract_metadata(file_name, xlsx_path)
            self.save_metadata(metadata, document_id)
            
            # Step 2: Read all sheets
            logger.info(f"Reading sheets from {file_name}")
            all_sheets = pd.read_excel(xlsx_path, sheet_name=None, engine='openpyxl')
            
            logger.info(f"Found {len(all_sheets)} sheets")
            metadata["sheet_count"] = len(all_sheets)
            metadata["sheet_names"] = list(all_sheets.keys())
            
            # Step 3: Process each sheet
            all_table_details = []
            sheet_idx = 0
            
            for sheet_name, df in all_sheets.items():
                sheet_idx += 1
                logger.info(f"Processing sheet: {sheet_name} ({df.shape[0]} rows x {df.shape[1]} cols)")
                
                # Convert to HTML
                html_path = self.convert_sheet_to_html(df, sheet_name, document_id)
                
                if html_path:
                    # Extract tables
                    table_details = self.extract_tables_from_html(
                        html_path=html_path,
                        sheet_name=sheet_name,
                        sheet_idx=sheet_idx,
                        document_id=document_id,
                        metadata=metadata,
                    )
                    all_table_details.extend(table_details)
            
            # Step 4: Save all table details
            Path(self.embeddings_path).mkdir(parents=True, exist_ok=True)
            tables_file = os.path.join(self.embeddings_path, f"{document_id}_tables.json")
            
            with open(tables_file, "w", encoding="utf-8") as f:
                json.dump(all_table_details, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(all_table_details)} table details to {tables_file}")
            logger.info(f"Successfully processed XLSX: {file_name}")
            
        except Exception as e:
            logger.error(f"Failed to process XLSX {file_name}: {e}")
            
            # Move to failed directory
            failed_path = os.path.join(self.failed_path, file_name)
            if os.path.exists(xlsx_path):
                Path(self.failed_path).mkdir(parents=True, exist_ok=True)
                self.file_handler.move_file(xlsx_path, failed_path)
            
            raise
