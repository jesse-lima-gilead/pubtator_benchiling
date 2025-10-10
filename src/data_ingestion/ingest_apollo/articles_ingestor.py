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

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

    # Runs the combined process
    def run(
        self,
    ):
        logger.info("Running apolloDOCXIngestor...")
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
        apollo_docx_ingestor.run()
        logger.info("apolloDOCXIngestor Completed...")

        logger.info("Running apolloPPTXIngestor...")
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
        apollo_pptx_ingestor.run()
        logger.info("apolloPPTXIngestor Completed...")
