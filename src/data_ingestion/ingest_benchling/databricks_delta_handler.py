"""
Databricks Delta Table Handler

Handles writing documents and chunks to Delta tables instead of PostgreSQL.
Designed for Databricks Unity Catalog.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid
import json
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
        """
        Create Delta tables if they don't exist.
        
        Note: If tables already exist with a different schema, this will NOT alter them.
        To add new columns to existing tables, use ALTER TABLE ADD COLUMN statements
        or recreate the tables.
        """
        if not self.spark:
            logger.warning("Spark not available, skipping table creation")
            return
        
        # Create documents table with all PostgreSQL + JSON metadata fields
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.documents_table} (
                -- Primary key and identifiers
                document_grsar_id STRING NOT NULL,
                source STRING NOT NULL,
                workflow_id STRING,
                
                -- PostgreSQL document table fields
                document_name STRING,
                document_type STRING,
                document_grsar_name STRING,
                source_path STRING,
                document_file_size_in_bytes BIGINT,
                created_dt TIMESTAMP,
                last_update_dt TIMESTAMP,
                
                -- Benchling JSON metadata fields
                entry_id STRING,
                name STRING,
                display_id STRING,
                folder_id STRING,
                benchling_created_at TIMESTAMP,
                benchling_modified_at TIMESTAMP,
                authors STRING,  -- JSON string representation of array
                creator STRING,  -- JSON string representation of object
                project STRING,
                date STRING,
                
                -- Aggregated chunk statistics
                total_chunks INT,
                total_chunk_annotations_count INT,
                vector_field_name STRING,
                
                -- Processing status fields
                status STRING DEFAULT 'pending',
                error_message STRING,
                
                -- Legacy fields for backward compatibility
                file_name STRING,
                file_path STRING,
                safe_file_name STRING,
                file_extension STRING,
                size_bytes BIGINT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
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
    
    def add_missing_columns_if_needed(self):
        """
        Add missing columns to existing Delta tables if they don't exist.
        This is useful when updating schema without recreating tables.
        
        Note: Delta tables support schema evolution, but this method explicitly
        adds columns that might be missing from older table definitions.
        """
        if not self.spark:
            logger.warning("Spark not available, skipping column addition")
            return
        
        try:
            # Check if table exists
            try:
                self.spark.sql(f"DESCRIBE {self.documents_table}").collect()
                table_exists = True
            except Exception:
                table_exists = False
            
            if not table_exists:
                logger.info("Table doesn't exist, skipping column addition")
                return
            
            # List of columns that should exist (new fields)
            new_columns = [
                ("entry_id", "STRING"),
                ("name", "STRING"),
                ("display_id", "STRING"),
                ("folder_id", "STRING"),
                ("benchling_created_at", "TIMESTAMP"),
                ("benchling_modified_at", "TIMESTAMP"),
                ("authors", "STRING"),
                ("creator", "STRING"),
                ("project", "STRING"),
                ("date", "STRING"),
                ("total_chunks", "INT"),
                ("total_chunk_annotations_count", "INT"),
                ("vector_field_name", "STRING"),
                ("document_name", "STRING"),
                ("document_type", "STRING"),
                ("document_grsar_name", "STRING"),
                ("source_path", "STRING"),
                ("document_file_size_in_bytes", "BIGINT"),
                ("created_dt", "TIMESTAMP"),
                ("last_update_dt", "TIMESTAMP"),
            ]
            
            # Get existing columns
            existing_cols = [row.columnName.lower() for row in 
                           self.spark.sql(f"DESCRIBE {self.documents_table}").collect()]
            
            # Add missing columns
            for col_name, col_type in new_columns:
                if col_name.lower() not in existing_cols:
                    try:
                        self.spark.sql(f"""
                            ALTER TABLE {self.documents_table}
                            ADD COLUMN {col_name} {col_type}
                        """)
                        logger.info(f"Added column {col_name} to {self.documents_table}")
                    except Exception as e:
                        logger.warning(f"Failed to add column {col_name}: {e}")
            
        except Exception as e:
            logger.error(f"Failed to add missing columns: {e}")
    
    def insert_document(
        self,
        document_grsar_id: str,
        source: str,
        file_name: str,
        file_path: str,
        safe_file_name: str,
        workflow_id: Optional[str] = None,
        size_bytes: Optional[int] = None,
        # PostgreSQL document table fields
        document_name: Optional[str] = None,
        document_type: Optional[str] = None,
        document_grsar_name: Optional[str] = None,
        source_path: Optional[str] = None,
        document_file_size_in_bytes: Optional[int] = None,
        created_dt: Optional[datetime] = None,
        last_update_dt: Optional[datetime] = None,
        # Benchling JSON metadata fields
        entry_id: Optional[str] = None,
        name: Optional[str] = None,
        display_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        benchling_created_at: Optional[datetime] = None,
        benchling_modified_at: Optional[datetime] = None,
        authors: Optional[List[Dict[str, str]]] = None,
        creator: Optional[Dict[str, str]] = None,
        project: Optional[str] = None,
        date: Optional[str] = None,
        # Aggregated chunk statistics
        total_chunks: Optional[int] = None,
        total_chunk_annotations_count: Optional[int] = None,
        vector_field_name: Optional[str] = None,
        # Processing status
        status: str = "pending",
        error_message: Optional[str] = None,
        **metadata
    ) -> bool:
        """
        Insert document record into Delta table with all PostgreSQL + JSON metadata fields.
        
        Args:
            document_grsar_id: Unique document identifier (hash of path)
            source: Data source name (e.g., "benchling")
            file_name: Original file name
            file_path: Full S3 path
            safe_file_name: Safe file name for local storage
            workflow_id: Processing workflow ID
            size_bytes: File size in bytes
            
            # PostgreSQL document table fields
            document_name: Original filename (same as file_name if not provided)
            document_type: File extension (e.g., .pdf, .docx)
            document_grsar_name: Safe filename (same as safe_file_name if not provided)
            source_path: File path location (same as file_path if not provided)
            document_file_size_in_bytes: File size in bytes (same as size_bytes if not provided)
            created_dt: Document creation timestamp
            last_update_dt: Last update timestamp
            
            # Benchling JSON metadata fields
            entry_id: Benchling entry ID
            name: Benchling document name
            display_id: Benchling display ID
            folder_id: Benchling folder ID
            benchling_created_at: Creation timestamp from Benchling
            benchling_modified_at: Modified timestamp from Benchling
            authors: List of author dictionaries with id, handle, name
            creator: Creator dictionary with id, handle, name
            project: Project name
            date: Date string from file path
            
            # Aggregated chunk statistics
            total_chunks: Total number of chunks for this document
            total_chunk_annotations_count: Total annotations across all chunks
            vector_field_name: Vector field name ('vector' or 'smiles_vector')
            
            # Processing status
            status: Processing status (default: 'pending')
            error_message: Error message if processing failed
            
            **metadata: Additional metadata fields
            
        Returns:
            True if successful
        """
        now = datetime.utcnow()
        file_extension = file_name.split(".")[-1].lower() if "." in file_name else ""
        
        # Use provided values or fall back to defaults
        doc_data = {
            # Primary key and identifiers
            "document_grsar_id": document_grsar_id,
            "source": source,
            "workflow_id": workflow_id,
            
            # PostgreSQL document table fields
            "document_name": document_name or file_name,
            "document_type": document_type or file_extension,
            "document_grsar_name": document_grsar_name or safe_file_name,
            "source_path": source_path or file_path,
            "document_file_size_in_bytes": document_file_size_in_bytes or size_bytes,
            "created_dt": created_dt or now,
            "last_update_dt": last_update_dt or now,
            
            # Benchling JSON metadata fields
            "entry_id": entry_id,
            "name": name,
            "display_id": display_id,
            "folder_id": folder_id,
            "benchling_created_at": benchling_created_at,
            "benchling_modified_at": benchling_modified_at,
            "authors": json.dumps(authors) if authors else None,  # Store as JSON string
            "creator": json.dumps(creator) if creator else None,  # Store as JSON string
            "project": project,
            "date": date,
            
            # Aggregated chunk statistics
            "total_chunks": total_chunks,
            "total_chunk_annotations_count": total_chunk_annotations_count,
            "vector_field_name": vector_field_name or ("smiles_vector" if source == "eln" else "vector"),
            
            # Processing status fields
            "status": status,
            "error_message": error_message,
            
            # Legacy fields for backward compatibility
            "file_name": file_name,
            "file_path": file_path,
            "safe_file_name": safe_file_name,
            "file_extension": file_extension,
            "size_bytes": size_bytes,
            "created_at": now,
            "updated_at": now,
        }
        
        # Add any additional metadata
        doc_data.update(metadata)
        
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
                updates = [
                    f"status = '{status}'",
                    f"updated_at = current_timestamp()",
                    f"last_update_dt = current_timestamp()"
                ]
                if workflow_id:
                    updates.append(f"workflow_id = '{workflow_id.replace(\"'\", \"''\")}'")
                if error_message:
                    updates.append(f"error_message = '{error_message.replace(\"'\", \"''\")}'")
                
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
    
    def update_document_with_metadata(
        self,
        document_grsar_id: str,
        entry_id: Optional[str] = None,
        name: Optional[str] = None,
        display_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        benchling_created_at: Optional[datetime] = None,
        benchling_modified_at: Optional[datetime] = None,
        authors: Optional[List[Dict[str, str]]] = None,
        creator: Optional[Dict[str, str]] = None,
        project: Optional[str] = None,
        date: Optional[str] = None,
    ) -> bool:
        """
        Update document with Benchling JSON metadata.
        
        Args:
            document_grsar_id: Document identifier
            entry_id: Benchling entry ID
            name: Benchling document name
            display_id: Benchling display ID
            folder_id: Benchling folder ID
            benchling_created_at: Creation timestamp from Benchling
            benchling_modified_at: Modified timestamp from Benchling
            authors: List of author dictionaries
            creator: Creator dictionary
            project: Project name
            date: Date string
            
        Returns:
            True if successful
        """
        if not self.spark:
            return True
        
        try:
            updates = ["last_update_dt = current_timestamp()", "updated_at = current_timestamp()"]
            
            # Helper function to safely escape SQL strings
            def escape_sql_string(value: str) -> str:
                """Escape single quotes for SQL."""
                return value.replace("'", "''")
            
            if entry_id is not None:
                updates.append(f"entry_id = '{escape_sql_string(entry_id)}'")
            if name is not None:
                updates.append(f"name = '{escape_sql_string(name)}'")
            if display_id is not None:
                updates.append(f"display_id = '{escape_sql_string(display_id)}'")
            if folder_id is not None:
                updates.append(f"folder_id = '{escape_sql_string(folder_id)}'")
            if benchling_created_at is not None:
                updates.append(f"benchling_created_at = TIMESTAMP('{benchling_created_at.isoformat()}')")
            if benchling_modified_at is not None:
                updates.append(f"benchling_modified_at = TIMESTAMP('{benchling_modified_at.isoformat()}')")
            if project is not None:
                updates.append(f"project = '{escape_sql_string(project)}'")
            if date is not None:
                updates.append(f"date = '{escape_sql_string(date)}'")
            
            # For JSON string fields (authors, creator)
            if authors is not None:
                authors_json = json.dumps(authors)
                updates.append(f"authors = '{escape_sql_string(authors_json)}'")
            if creator is not None:
                creator_json = json.dumps(creator)
                updates.append(f"creator = '{escape_sql_string(creator_json)}'")
            
            if len(updates) > 2:  # More than just timestamp updates
                update_sql = ", ".join(updates)
                self.spark.sql(f"""
                    UPDATE {self.documents_table}
                    SET {update_sql}
                    WHERE document_grsar_id = '{document_grsar_id}'
                """)
                logger.info(f"Updated document metadata: {document_grsar_id}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to update document metadata {document_grsar_id}: {e}")
            return False
    
    def update_document_with_chunk_stats(
        self,
        document_grsar_id: str,
        total_chunks: Optional[int] = None,
        total_chunk_annotations_count: Optional[int] = None,
        vector_field_name: Optional[str] = None,
    ) -> bool:
        """
        Update document with aggregated chunk statistics.
        Can also compute stats from chunks table if not provided.
        
        Args:
            document_grsar_id: Document identifier
            total_chunks: Total number of chunks (computed if None)
            total_chunk_annotations_count: Total annotations (computed if None)
            vector_field_name: Vector field name
            
        Returns:
            True if successful
        """
        if not self.spark:
            return True
        
        try:
            # Compute stats from chunks table if not provided
            if total_chunks is None or total_chunk_annotations_count is None:
                chunks = self.get_chunks_by_document(document_grsar_id)
                if total_chunks is None:
                    total_chunks = len(chunks)
                if total_chunk_annotations_count is None:
                    # Sum up annotations from chunks if available
                    total_chunk_annotations_count = sum(
                        chunk.get("chunk_annotations_count", 0) for chunk in chunks
                    )
            
            updates = [
                "last_update_dt = current_timestamp()",
                "updated_at = current_timestamp()"
            ]
            
            if total_chunks is not None:
                updates.append(f"total_chunks = {total_chunks}")
            if total_chunk_annotations_count is not None:
                updates.append(f"total_chunk_annotations_count = {total_chunk_annotations_count}")
            if vector_field_name is not None:
                updates.append(f"vector_field_name = '{vector_field_name}'")
            
            update_sql = ", ".join(updates)
            self.spark.sql(f"""
                UPDATE {self.documents_table}
                SET {update_sql}
                WHERE document_grsar_id = '{document_grsar_id}'
            """)
            logger.info(f"Updated document chunk stats: {document_grsar_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update document chunk stats {document_grsar_id}: {e}")
            return False
    
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
