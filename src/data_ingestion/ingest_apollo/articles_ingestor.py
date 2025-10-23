from src.data_ingestion.ingest_apollo.apollo_articles_uploader import (
    upload_apollo_articles,
)
from src.data_ingestion.ingest_apollo.ingest_pptx.pptx_articles_ingestor import (
    apolloPPTXIngestor,
)
from src.data_ingestion.ingest_apollo.ingest_docx.docx_articles_ingestor import (
    apolloDOCXIngestor,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class APOLLOIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        apollo_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "apollo",
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.source = source
        self.workflow_id = workflow_id
        self.paths_config = paths_config
        self.file_handler = file_handler
        self.apollo_source_config = apollo_source_config

        self.apollo_path = (
            paths_config["ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.ingestion_interim_path = (
            paths_config["ingestion_interim_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.bioc_path = (
            paths_config["bioc_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.article_metadata_path = (
            paths_config["metadata_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.summary_path = (
            paths_config["summary_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.embeddings_path = (
            paths_config["embeddings_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.failed_ingestion_path = (
            paths_config["failed_ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler
        self.apollo_source_config = apollo_source_config
        self.source = source

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_apollo_path = self.s3_paths_config.get(
                "ingestion_path", ""
            ).replace("{source}", source)
            self.s3_interim_path = self.s3_paths_config.get(
                "ingestion_interim_path", ""
            ).replace("{source}", source)
            self.s3_bioc_path = self.s3_paths_config.get("bioc_path", "").replace(
                "{source}", source
            )
            self.s3_article_metadata_path = self.s3_paths_config.get(
                "metadata_path", ""
            ).replace("{source}", source)
            self.s3_summary_path = self.s3_paths_config.get("summary_path", "").replace(
                "{source}", source
            )
            self.s3_embeddings_path = self.s3_paths_config.get(
                "embeddings_path", ""
            ).replace("{source}", source)
            self.s3_failed_ingestion_path = self.s3_paths_config.get(
                "failed_ingestion_path", ""
            ).replace("{source}", source)
        else:
            self.s3_apollo_path = (
                self.s3_bioc_path
            ) = (
                self.s3_article_metadata_path
            ) = (
                self.s3_summary_path
            ) = (
                self.s3_interim_path
            ) = self.embeddings_path = self.s3_failed_ingestion_path = None

    # Runs the combined process
    def run(
        self,
    ):
        logger.info("Instantiating apolloDOCXIngestor...")
        apollo_docx_ingestor = apolloDOCXIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Instantiating apolloPPTXIngestor...")
        apollo_pptx_ingestor = apolloPPTXIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )

        logger.info("Processing Apollo Articles...")
        for file_name in self.file_handler.list_files(self.apollo_path):
            logger.info(f"Processing Apollo file {file_name}")
            file_type = file_name.split(".")[-1]

            if file_name.endswith(".pptx"):
                apollo_pptx_ingestor.run(file_name=file_name)
            elif file_name.endswith(".docx"):
                apollo_docx_ingestor.run(file_name=file_name)
            else:
                logger.info(f"Skipping file because it is of file type: {file_type}")

        logger.info("Uploading All the Processed files from Local to S3")
        upload_apollo_articles(
            apollo_path=self.apollo_path,
            s3_apollo_path=self.s3_apollo_path,
            bioc_path=self.bioc_path,
            s3_bioc_path=self.s3_bioc_path,
            interim_path=self.ingestion_interim_path,
            s3_interim_path=self.s3_interim_path,
            summary_path=self.summary_path,
            s3_summary_path=self.s3_summary_path,
            embeddings_path=self.embeddings_path,
            s3_embeddings_path=self.s3_embeddings_path,
            failed_ingestion_path=self.failed_ingestion_path,
            s3_failed_ingestion_path=self.s3_failed_ingestion_path,
            file_handler=self.file_handler,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Completed Uploading All the files from Local to S3")
