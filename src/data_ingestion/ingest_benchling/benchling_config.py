"""
Benchling Configuration Module

Provides inline configuration for Databricks environment.
No YAML files or AWS secrets required.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class BenchlingS3Config:
    """S3 configuration for Benchling data source."""
    bucket_name: str = "gilead-edp-kite-rd-dev-us-west-2-kite-benchling-text-sql"
    bucket_region: str = "us-west-2"
    source_prefix: str = "benchling_unstructured/"
    

@dataclass
class BenchlingDeltaConfig:
    """Delta table configuration for storing chunks and metadata."""
    catalog: str = "kite_rd_dev"
    schema: str = "pubtator"
    documents_table: str = "benchling_documents"
    chunks_table: str = "benchling_chunks"
    

@dataclass
class BenchlingPathsConfig:
    """Local paths configuration for processing."""
    base_path: str = "/tmp/benchling_processing"
    
    def get_paths(self, workflow_id: str, source: str = "benchling") -> Dict[str, str]:
        """Generate paths for a specific workflow."""
        base = f"{self.base_path}/{workflow_id}/{source}"
        return {
            "ingestion_path": f"{base}/ingestion",
            "ingestion_interim_path": f"{base}/interim",
            "failed_ingestion_path": f"{base}/failed",
            "bioc_path": f"{base}/bioc_xml",
            "metadata_path": f"{base}/metadata",
            "chunks_path": f"{base}/chunks",
            "embeddings_path": f"{base}/embeddings",
        }


@dataclass
class BenchlingConfig:
    """Main configuration class for Benchling ingestion."""
    s3: BenchlingS3Config = field(default_factory=BenchlingS3Config)
    delta: BenchlingDeltaConfig = field(default_factory=BenchlingDeltaConfig)
    paths: BenchlingPathsConfig = field(default_factory=BenchlingPathsConfig)
    
    # File types to process
    allowed_file_types: List[str] = field(
        default_factory=lambda: ["pdf", "docx", "xlsx", "pptx", "txt", "csv"]
    )
    
    # Embedding model configuration
    embeddings_model: str = "pubmedbert"
    embeddings_model_path: str = "s3://gilead-edp-kite-rd-dev-us-west-2-kite-benchling-text-sql/models/pubmedbert-base-embeddings/"
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> "BenchlingConfig":
        """Create config from dictionary (useful for Databricks widgets)."""
        # Define all defaults inline since dataclass fields are instance attributes, not class attributes
        # S3 defaults
        default_bucket_name = "gilead-edp-kite-rd-dev-us-west-2-kite-benchling-text-sql"
        default_bucket_region = "us-west-2"
        default_source_prefix = "benchling_unstructured/"
        
        # Delta defaults
        default_catalog = "kite_rd_dev"
        default_schema = "pubtator"
        default_documents_table = "benchling_documents"
        default_chunks_table = "benchling_chunks"
        
        # Paths defaults
        default_base_path = "/tmp/benchling_processing"
        
        # Config defaults
        default_allowed_file_types = ["pdf", "docx", "xlsx", "pptx", "txt", "csv"]
        default_embeddings_model = "pubmedbert"
        default_embeddings_model_path = "s3://gilead-edp-kite-rd-dev-us-west-2-kite-benchling-text-sql/models/pubmedbert-base-embeddings/"
        
        s3_config = BenchlingS3Config(
            bucket_name=config_dict.get("s3_bucket", default_bucket_name),
            bucket_region=config_dict.get("s3_region", default_bucket_region),
            source_prefix=config_dict.get("s3_prefix", default_source_prefix),
        )
        
        delta_config = BenchlingDeltaConfig(
            catalog=config_dict.get("delta_catalog", default_catalog),
            schema=config_dict.get("delta_schema", default_schema),
            documents_table=config_dict.get("documents_table", default_documents_table),
            chunks_table=config_dict.get("chunks_table", default_chunks_table),
        )
        
        paths_config = BenchlingPathsConfig(
            base_path=config_dict.get("base_path", default_base_path),
        )
        
        return cls(
            s3=s3_config,
            delta=delta_config,
            paths=paths_config,
            allowed_file_types=config_dict.get("allowed_file_types", default_allowed_file_types),
            embeddings_model=config_dict.get("embeddings_model", default_embeddings_model),
            embeddings_model_path=config_dict.get("embeddings_model_path", default_embeddings_model_path),
        )


def get_default_config() -> BenchlingConfig:
    """Get default configuration for Benchling ingestion."""
    return BenchlingConfig()
