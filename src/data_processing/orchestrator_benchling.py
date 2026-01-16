"""
Benchling Processing Orchestrator

Creates chunks and embeddings for Benchling documents.
Saves to Databricks Delta tables instead of PostgreSQL.
Skips NER annotation and merge annotation steps.
"""

import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import logging

from src.data_ingestion.ingest_benchling.benchling_config import BenchlingConfig, get_default_config
from src.data_ingestion.ingest_benchling.databricks_delta_handler import DatabricksDeltaHandler
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

import bioc

logger = logging.getLogger(__name__)


class BenchlingArticleProcessor:
    """
    Databricks-native orchestrator for Benchling document processing.
    
    Creates chunks and embeddings, saves to Delta tables.
    Simplified pipeline that skips NER annotation steps.
    """
    
    def __init__(
        self,
        workflow_id: str,
        config: Optional[BenchlingConfig] = None,
        source: str = "benchling",
        write_to_delta: bool = True,
        embeddings_model: str = "pubmedbert",
        window_size: int = 512,
        stride: int = 256,
    ):
        """
        Initialize Benchling article processor.
        
        Args:
            workflow_id: Unique workflow identifier
            config: Benchling configuration
            source: Source name
            write_to_delta: Whether to write to Delta tables
            embeddings_model: Model to use for embeddings
            window_size: Sliding window size for chunking (in words)
            stride: Stride for sliding window (overlap = window_size - stride)
        """
        self.workflow_id = workflow_id
        self.config = config or get_default_config()
        self.source = source
        self.write_to_delta = write_to_delta
        self.embeddings_model = embeddings_model
        self.window_size = window_size
        self.stride = stride
        
        # Get paths
        self.paths = self.config.paths.get_paths(workflow_id, source)
        
        # Initialize handlers
        self.file_handler = LocalFileHandler()
        
        self.delta_handler = None
        if write_to_delta:
            self.delta_handler = DatabricksDeltaHandler(
                catalog=self.config.delta.catalog,
                schema=self.config.delta.schema,
                documents_table=self.config.delta.documents_table,
                chunks_table=self.config.delta.chunks_table,
            )
        
        # Embeddings model (loaded lazily)
        self._model = None
        self._tokenizer = None
    
    def _load_embeddings_model(self):
        """Load embeddings model lazily."""
        if self._model is None:
            try:
                from src.pubtator_utils.embeddings_handler.embeddings_generator import (
                    load_embeddings_model,
                    load_embeddings_model_from_path,
                )
                
                # Try to use model path from config first (inline config approach)
                model_path = getattr(self.config, 'embeddings_model_path', None)
                
                # Check if model_path is an S3 path - if so, try local fallback paths
                if model_path and model_path.startswith('s3://'):
                    # For local execution, try common local paths
                    import os
                    local_paths = [
                        f"./src/models/{self.embeddings_model}-base-embeddings",
                        f"./models/{self.embeddings_model}-base-embeddings",
                        f"/tmp/models/{self.embeddings_model}-base-embeddings",
                    ]
                    model_path = None
                    for local_path in local_paths:
                        if os.path.exists(local_path):
                            model_path = local_path
                            break
                
                if model_path and os.path.exists(model_path):
                    # Use direct path loading (no YAML config needed)
                    self._model, self._tokenizer = load_embeddings_model_from_path(model_path)
                    logger.info(f"Loaded embeddings model from path: {model_path}")
                else:
                    # Fallback to YAML config-based loading
                    self._model, self._tokenizer = load_embeddings_model(
                        model_name=self.embeddings_model
                    )
                    logger.info(f"Loaded embeddings model: {self.embeddings_model}")
                    
            except Exception as e:
                logger.warning(f"Failed to load embeddings model: {e}")
                self._model = None
                self._tokenizer = None
        
        return self._model, self._tokenizer
    
    def sliding_window_chunk(self, text: str) -> List[Dict[str, Any]]:
        """
        Split text into overlapping chunks using sliding window.
        
        Args:
            text: Input text to chunk
            
        Returns:
            List of chunk dictionaries with text, start/end positions
        """
        # Split into words while preserving whitespace
        words = re.findall(r'\S+|\s+', text)
        chunks = []
        i = 0
        
        while i < len(words):
            chunk_words = words[i:i + self.window_size]
            chunk_text = ''.join(chunk_words).strip()
            
            if chunk_text:
                chunks.append({
                    'text': chunk_text,
                    'start_word': i,
                    'end_word': min(i + self.window_size, len(words)),
                    'word_count': len(chunk_text.split()),
                })
            
            # Check if we've reached the end
            remaining = len(words) - (i + self.window_size)
            if remaining <= self.stride:
                break
            
            i += self.stride
        
        return chunks
    
    def chunk_bioc_document(self, bioc_doc: bioc.BioCDocument) -> List[Dict[str, Any]]:
        """
        Chunk a BioC document into smaller pieces.
        
        Args:
            bioc_doc: BioC document to chunk
            
        Returns:
            List of chunk dictionaries
        """
        all_chunks = []
        
        for passage_idx, passage in enumerate(bioc_doc.passages):
            passage_text = passage.text or ""
            passage_type = passage.infons.get("type", "body_text")
            section_title = passage.infons.get("section", "")
            
            # Skip very short passages
            if len(passage_text.split()) < 10:
                continue
            
            # Chunk the passage
            passage_chunks = self.sliding_window_chunk(passage_text)
            
            for chunk_idx, chunk in enumerate(passage_chunks):
                all_chunks.append({
                    'text': chunk['text'],
                    'passage_idx': passage_idx,
                    'chunk_idx': chunk_idx,
                    'section_type': passage_type,
                    'section_title': section_title,
                    'word_count': chunk['word_count'],
                    'annotations': [],  # No NER annotations
                })
        
        return all_chunks
    
    def generate_embeddings(self, texts: List[str]) -> Optional[List[List[float]]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings
            
        Returns:
            List of embedding vectors, or None if model not available
        """
        if not texts:
            return None
        
        model, tokenizer = self._load_embeddings_model()
        
        if model is None:
            logger.warning("Embeddings model not available, skipping embedding generation")
            return None
        
        try:
            from src.pubtator_utils.embeddings_handler.embeddings_generator import (
                get_embeddings,
            )
            
            embeddings = get_embeddings(
                model_name=self.embeddings_model,
                texts=texts,
                model=model,
                tokenizer=tokenizer,
            )
            
            # Convert to list of lists
            return [emb.tolist() for emb in embeddings]
            
        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}")
            return None
    
    def process_bioc_file(
        self,
        bioc_file_path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Process a single BioC XML file.
        
        Args:
            bioc_file_path: Path to BioC XML file
            metadata: Additional metadata to include
            
        Returns:
            List of processed chunks with embeddings
        """
        logger.info(f"Processing BioC file: {bioc_file_path}")
        
        # Read BioC file
        with open(bioc_file_path, "r", encoding="utf-8") as f:
            collection = bioc.load(f)
        
        all_processed_chunks = []
        
        for bioc_doc in collection.documents:
            document_id = bioc_doc.id
            
            # Get document metadata
            doc_metadata = dict(bioc_doc.infons)
            if metadata:
                doc_metadata.update(metadata)
            
            # Chunk the document
            chunks = self.chunk_bioc_document(bioc_doc)
            
            if not chunks:
                logger.warning(f"No chunks created for document: {document_id}")
                continue
            
            # Generate embeddings
            chunk_texts = [c['text'] for c in chunks]
            embeddings = self.generate_embeddings(chunk_texts)
            
            # Create processed chunk records
            for i, chunk in enumerate(chunks):
                chunk_id = str(uuid.uuid4())
                
                processed_chunk = {
                    'chunk_id': chunk_id,
                    'document_grsar_id': document_id,
                    'chunk_sequence': i + 1,
                    'workflow_id': self.workflow_id,
                    'source': self.source,
                    'chunk_text': chunk['text'],
                    'merged_text': chunk['text'],  # No annotation merging
                    'embeddings': embeddings[i] if embeddings else [],
                    'chunk_type': 'article_chunk',
                    'chunk_length': len(chunk['text']),
                    'token_count': chunk['word_count'],
                    'section_title': chunk.get('section_title', ''),
                    'section_type': chunk.get('section_type', ''),
                    'keywords': [],  # No NER keywords
                    'article_id': document_id,
                    'file_name': doc_metadata.get('file_name', ''),
                    'file_path': doc_metadata.get('file_path', ''),
                }
                
                all_processed_chunks.append(processed_chunk)
        
        logger.info(f"Created {len(all_processed_chunks)} chunks from {bioc_file_path}")
        return all_processed_chunks
    
    def process_table_file(
        self,
        tables_file_path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Process a tables JSON file (from XLSX/PPTX processing).
        
        Args:
            tables_file_path: Path to tables JSON file
            metadata: Additional metadata
            
        Returns:
            List of processed chunks with embeddings
        """
        logger.info(f"Processing tables file: {tables_file_path}")
        
        with open(tables_file_path, "r", encoding="utf-8") as f:
            table_details = json.load(f)
        
        if not table_details:
            return []
        
        # Get text from tables
        chunk_texts = []
        for table in table_details:
            payload = table.get('payload', table)
            text = payload.get('clean_flat_text', '') or payload.get('context_flat_text', '')
            chunk_texts.append(text)
        
        # Generate embeddings
        embeddings = self.generate_embeddings(chunk_texts)
        
        # Create processed chunk records
        processed_chunks = []
        for i, table in enumerate(table_details):
            payload = table.get('payload', table)
            chunk_id = payload.get('table_id', str(uuid.uuid4()))
            document_id = payload.get('document_grsar_id', '')
            
            processed_chunk = {
                'chunk_id': chunk_id,
                'document_grsar_id': document_id,
                'chunk_sequence': i + 1,
                'workflow_id': self.workflow_id,
                'source': self.source,
                'chunk_text': chunk_texts[i],
                'merged_text': chunk_texts[i],
                'embeddings': embeddings[i] if embeddings else [],
                'chunk_type': 'table_chunk',
                'chunk_length': len(chunk_texts[i]),
                'token_count': len(chunk_texts[i].split()),
                'section_title': payload.get('sheet_name', ''),
                'keywords': [],
                'article_id': document_id,
                'file_name': payload.get('file_name', ''),
                'file_path': payload.get('file_path', ''),
                # Table-specific metadata
                'row_count': payload.get('row_count', 0),
                'column_count': payload.get('column_count', 0),
            }
            
            if metadata:
                processed_chunk.update(metadata)
            
            processed_chunks.append(processed_chunk)
        
        logger.info(f"Created {len(processed_chunks)} table chunks")
        return processed_chunks
    
    def save_chunks_to_json(self, chunks: List[Dict], output_path: str):
        """Save chunks to JSON file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Saved {len(chunks)} chunks to {output_path}")
    
    def save_chunks_to_delta(self, chunks: List[Dict]) -> int:
        """Save chunks to Delta table."""
        if not self.delta_handler:
            logger.warning("Delta handler not available, skipping Delta save")
            return 0
        
        return self.delta_handler.batch_insert_chunks(chunks)
    
    def process_all(self, save_to_json: bool = True, save_to_delta: bool = True) -> Dict[str, Any]:
        """
        Process all BioC XML and table files in the workflow.
        
        Args:
            save_to_json: Whether to save chunks to JSON files
            save_to_delta: Whether to save chunks to Delta table
            
        Returns:
            Processing summary
        """
        logger.info(f"Starting processing for workflow: {self.workflow_id}")
        
        bioc_path = self.paths["bioc_path"]
        embeddings_path = self.paths["embeddings_path"]
        chunks_path = self.paths["chunks_path"]
        
        results = {
            "workflow_id": self.workflow_id,
            "bioc_files_processed": 0,
            "table_files_processed": 0,
            "total_chunks": 0,
            "chunks_saved_to_delta": 0,
        }
        
        all_chunks = []
        
        # Process BioC XML files
        if os.path.exists(bioc_path):
            for bioc_file in os.listdir(bioc_path):
                if bioc_file.endswith(".xml"):
                    bioc_file_path = os.path.join(bioc_path, bioc_file)
                    chunks = self.process_bioc_file(bioc_file_path)
                    all_chunks.extend(chunks)
                    results["bioc_files_processed"] += 1
        
        # Process table files (from XLSX/PPTX)
        if os.path.exists(embeddings_path):
            for table_file in os.listdir(embeddings_path):
                if table_file.endswith("_tables.json"):
                    table_file_path = os.path.join(embeddings_path, table_file)
                    chunks = self.process_table_file(table_file_path)
                    all_chunks.extend(chunks)
                    results["table_files_processed"] += 1
        
        results["total_chunks"] = len(all_chunks)
        
        # Save to JSON
        if save_to_json and all_chunks:
            Path(chunks_path).mkdir(parents=True, exist_ok=True)
            chunks_output_path = os.path.join(chunks_path, f"{self.workflow_id}_all_chunks.json")
            self.save_chunks_to_json(all_chunks, chunks_output_path)
            
            # Also save embeddings separately
            embeddings_output_path = os.path.join(embeddings_path, f"{self.workflow_id}_embeddings.json")
            self.save_chunks_to_json(all_chunks, embeddings_output_path)
        
        # Save to Delta
        if save_to_delta and self.write_to_delta and all_chunks:
            results["chunks_saved_to_delta"] = self.save_chunks_to_delta(all_chunks)
        
        logger.info(f"Processing complete: {results}")
        return results


def run_benchling_processing(
    workflow_id: str,
    config: Optional[BenchlingConfig] = None,
    source: str = "benchling",
    write_to_delta: bool = True,
    save_to_json: bool = True,
) -> Dict[str, Any]:
    """
    Run Benchling processing pipeline.
    
    Convenience function for running the full processing pipeline.
    
    Args:
        workflow_id: Workflow identifier
        config: Benchling configuration
        source: Source name
        write_to_delta: Whether to write to Delta tables
        save_to_json: Whether to save to JSON files
        
    Returns:
        Processing results summary
    """
    processor = BenchlingArticleProcessor(
        workflow_id=workflow_id,
        config=config,
        source=source,
        write_to_delta=write_to_delta,
    )
    
    return processor.process_all(
        save_to_json=save_to_json,
        save_to_delta=write_to_delta,
    )
