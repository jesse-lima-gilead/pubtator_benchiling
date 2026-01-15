"""
Databricks Delta Table Handler

Handles writing documents and chunks to Delta tables instead of PostgreSQL.
Designed for Databricks Unity Catalog.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid
import logging

logger = logging.getLogger(__name__)


class DatabricksDeltaHandler:
    """
    Handler for writing chunks and metadata to Databricks Delta tables.
    Replaces PostgreSQL for Databricks-native storage.
    """
    
    def __init__(
        self,
        catalog: str = "kite_rd_dev",
        schema: str = "pubtator",
        documents_table: str = "benchling_documents",
        chunks_table: str = "benchling_chunks",
        use_spark: bool = True,
    ):
        """
        Initialize Delta handler.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema/database name
            documents_table: Table name for documents
            chunks_table: Table name for chunks
            use_spark: Whether to use Spark (True in Databricks, False for testing)
        """
        self.catalog = catalog
        self.schema = schema
        self.documents_table_name = documents_table
        self.chunks_table_name = chunks_table
        self.use_spark = use_spark
        
        self.documents_table = f"{catalog}.{schema}.{documents_table}"
        self.chunks_table = f"{catalog}.{schema}.{chunks_table}"
        
        # Initialize Spark session if in Databricks
        self.spark = None
        if use_spark:
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.builder.getOrCreate()
                logger.info(f"Initialized Spark session for Delta tables")
            except ImportError:
                logger.warning("PySpark not available, Delta operations will be mocked")
                self.use_spark = False
    
    def create_tables_if_not_exists(self):
        """Create Delta tables if they don't exist."""
        if not self.spark:
            logger.warning("Spark not available, skipping table creation")
            return
        
        # Create documents table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.documents_table} (
                document_grsar_id STRING NOT NULL,
                source STRING NOT NULL,
                file_name STRING,
                file_path STRING,
                safe_file_name STRING,
                file_extension STRING,
                workflow_id STRING,
                size_bytes BIGINT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                status STRING DEFAULT 'pending',
                error_message STRING
            ) USING DELTA
        """)
        
        # Create chunks table with embeddings
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.chunks_table} (
                chunk_id STRING NOT NULL,
                document_grsar_id STRING NOT NULL,
                chunk_sequence INT,
                workflow_id STRING,
                source STRING,
                chunk_text STRING,
                merged_text STRING,
                embeddings ARRAY<FLOAT>,
                chunk_type STRING,
                chunk_length INT,
                token_count INT,
                section_title STRING,
                keywords ARRAY<STRING>,
                created_at TIMESTAMP,
                article_id STRING,
                file_name STRING,
                file_path STRING
            ) USING DELTA
        """)
        
        logger.info(f"Created/verified tables: {self.documents_table}, {self.chunks_table}")
    
    def insert_document(
        self,
        document_grsar_id: str,
        source: str,
        file_name: str,
        file_path: str,
        safe_file_name: str,
        workflow_id: Optional[str] = None,
        size_bytes: Optional[int] = None,
        **metadata
    ) -> bool:
        """
        Insert document record into Delta table.
        
        Args:
            document_grsar_id: Unique document identifier (hash of path)
            source: Data source name (e.g., "benchling")
            file_name: Original file name
            file_path: Full S3 path
            safe_file_name: Safe file name for local storage
            workflow_id: Processing workflow ID
            size_bytes: File size in bytes
            **metadata: Additional metadata fields
            
        Returns:
            True if successful
        """
        now = datetime.utcnow()
        file_extension = file_name.split(".")[-1].lower() if "." in file_name else ""
        
        doc_data = {
            "document_grsar_id": document_grsar_id,
            "source": source,
            "file_name": file_name,
            "file_path": file_path,
            "safe_file_name": safe_file_name,
            "file_extension": file_extension,
            "workflow_id": workflow_id,
            "size_bytes": size_bytes,
            "created_at": now,
            "updated_at": now,
            "status": "pending",
            "error_message": None,
        }
        
        if self.spark:
            try:
                df = self.spark.createDataFrame([doc_data])
                df.write.format("delta").mode("append").saveAsTable(self.documents_table)
                logger.info(f"Inserted document: {document_grsar_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to insert document {document_grsar_id}: {e}")
                return False
        else:
            # Mock mode for testing
            logger.info(f"[MOCK] Would insert document: {document_grsar_id}")
            return True
    
    def update_document_status(
        self,
        document_grsar_id: str,
        status: str,
        workflow_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update document status in Delta table."""
        if self.spark:
            try:
                updates = [f"status = '{status}'", f"updated_at = current_timestamp()"]
                if workflow_id:
                    updates.append(f"workflow_id = '{workflow_id}'")
                if error_message:
                    updates.append(f"error_message = '{error_message}'")
                
                update_sql = ", ".join(updates)
                self.spark.sql(f"""
                    UPDATE {self.documents_table}
                    SET {update_sql}
                    WHERE document_grsar_id = '{document_grsar_id}'
                """)
                return True
            except Exception as e:
                logger.error(f"Failed to update document {document_grsar_id}: {e}")
                return False
        return True
    
    def insert_chunk(
        self,
        chunk_id: str,
        document_grsar_id: str,
        chunk_sequence: int,
        workflow_id: str,
        source: str,
        chunk_text: str,
        merged_text: str,
        embeddings: List[float],
        chunk_type: str = "article_chunk",
        **payload
    ) -> bool:
        """
        Insert single chunk into Delta table.
        
        Args:
            chunk_id: Unique chunk identifier
            document_grsar_id: Parent document ID
            chunk_sequence: Chunk order in document
            workflow_id: Processing workflow ID
            source: Data source
            chunk_text: Raw chunk text
            merged_text: Text used for embedding
            embeddings: Vector embeddings
            chunk_type: Type of chunk
            **payload: Additional metadata
            
        Returns:
            True if successful
        """
        chunk_data = {
            "chunk_id": chunk_id,
            "document_grsar_id": document_grsar_id,
            "chunk_sequence": chunk_sequence,
            "workflow_id": workflow_id,
            "source": source,
            "chunk_text": chunk_text,
            "merged_text": merged_text,
            "embeddings": embeddings,
            "chunk_type": chunk_type,
            "chunk_length": len(chunk_text) if chunk_text else 0,
            "token_count": len(chunk_text.split()) if chunk_text else 0,
            "created_at": datetime.utcnow(),
            **payload
        }
        
        if self.spark:
            try:
                df = self.spark.createDataFrame([chunk_data])
                df.write.format("delta").mode("append").saveAsTable(self.chunks_table)
                return True
            except Exception as e:
                logger.error(f"Failed to insert chunk {chunk_id}: {e}")
                return False
        else:
            logger.info(f"[MOCK] Would insert chunk: {chunk_id}")
            return True
    
    def batch_insert_chunks(self, chunks: List[Dict[str, Any]]) -> int:
        """
        Batch insert chunks for better performance.
        
        Args:
            chunks: List of chunk dictionaries
            
        Returns:
            Number of chunks inserted
        """
        if not chunks:
            return 0
        
        # Add timestamps
        now = datetime.utcnow()
        for chunk in chunks:
            chunk["created_at"] = now
            if "chunk_length" not in chunk:
                chunk["chunk_length"] = len(chunk.get("chunk_text", ""))
            if "token_count" not in chunk:
                chunk["token_count"] = len(chunk.get("chunk_text", "").split())
        
        if self.spark:
            try:
                df = self.spark.createDataFrame(chunks)
                df.write.format("delta").mode("append").saveAsTable(self.chunks_table)
                logger.info(f"Batch inserted {len(chunks)} chunks")
                return len(chunks)
            except Exception as e:
                logger.error(f"Failed to batch insert chunks: {e}")
                return 0
        else:
            logger.info(f"[MOCK] Would batch insert {len(chunks)} chunks")
            return len(chunks)
    
    def get_document_by_id(self, document_grsar_id: str) -> Optional[Dict]:
        """Get document record by ID."""
        if self.spark:
            try:
                df = self.spark.sql(f"""
                    SELECT * FROM {self.documents_table}
                    WHERE document_grsar_id = '{document_grsar_id}'
                """)
                rows = df.collect()
                if rows:
                    return rows[0].asDict()
            except Exception as e:
                logger.error(f"Failed to get document {document_grsar_id}: {e}")
        return None
    
    def get_chunks_by_document(self, document_grsar_id: str) -> List[Dict]:
        """Get all chunks for a document."""
        if self.spark:
            try:
                df = self.spark.sql(f"""
                    SELECT * FROM {self.chunks_table}
                    WHERE document_grsar_id = '{document_grsar_id}'
                    ORDER BY chunk_sequence
                """)
                return [row.asDict() for row in df.collect()]
            except Exception as e:
                logger.error(f"Failed to get chunks for {document_grsar_id}: {e}")
        return []
