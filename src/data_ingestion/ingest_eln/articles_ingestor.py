import argparse
from pathlib import Path

from src.data_ingestion.ingest_eln.eln_articles_extractor import extract_eln_articles

from src.data_ingestion.ingest_eln.eln_articles_preprocessor import (
    generate_safe_filename,
    preprocess_eln_files,
)
from src.data_ingestion.ingest_eln.eln_to_bioc_converter import (
    convert_eln_html_to_bioc,
)
from src.data_ingestion.ingest_eln.eln_articles_formatter import (
    eln_article_json_formatter,
)
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_eln.eln_articles_uploader import upload_eln_articles
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class ELNIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        eln_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "eln",
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.source = source
        self.workflow_id = workflow_id
        self.eln_path = (
            paths_config["ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.ingestion_interim_path = (
            paths_config["ingestion_interim_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.failed_ingestion_path = (
            paths_config["failed_ingestion_path"]
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
        self.chunks_path = (
            paths_config["chunks_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.embeddings_path = (
            paths_config["embeddings_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler
        self.eln_source_config = eln_source_config
        self.pandoc_processor = PandocProcessor(pandoc_executable="pandoc")

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_eln_path = self.s3_paths_config.get("ingestion_path", "").replace(
                "{source}", source
            )
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
            self.s3_chunks_path = self.s3_paths_config.get("chunks_path", "").replace(
                "{source}", source
            )
            self.s3_failed_ingestion_path = self.s3_paths_config.get(
                "failed_ingestion_path", ""
            ).replace("{source}", source)
        else:
            self.s3_eln_path = (
                self.s3_bioc_path
            ) = (
                self.s3_interim_path
            ) = (
                self.s3_chunks_path
            ) = (
                self.s3_failed_ingestion_path
            ) = self.s3_article_metadata_path = self.s3_summary_path = None

    def eln_articles_extractor(self):
        # Extract the ELN Articles:
        logger.info("Extracting ELN Articles...")
        extracted_articles_count = extract_eln_articles(
            eln_path=self.eln_path,
            file_handler=self.file_handler,
            eln_source_config=self.eln_source_config,
            source=self.source,
        )
        logger.info(f"{extracted_articles_count} eln Articles Extracted Successfully!")

    def eln_safe_filenames_generator(self):
        # Generate Safe file name for the extracted articles
        logger.info("Generating Safe file names for the extracted articles...")
        safe_file_name_cnt = generate_safe_filename(self.eln_path)
        logger.info(
            f"Safe file names generated for {safe_file_name_cnt} articles successfully!"
        )

    def eln_formatter(self):
        # Format the ELN JSON to UTF8 JSON
        logger.info("Formatting ELN Articles to UTF8 JSON...")
        formatted_files_cnt, not_formatted_files_cnt = eln_article_json_formatter(
            eln_path=self.eln_path,
            failed_path=self.failed_ingestion_path,
            eln_interim_path=self.ingestion_interim_path,
            file_handler=self.file_handler,
        )
        logger.info(
            f"ELN Articles Formatting Completed! {formatted_files_cnt} files formatted successfully."
            f"{not_formatted_files_cnt} files could not be formatted."
        )

    def eln_articles_preprocessor(self):
        # Preprocess the ELN Articles to extract Mol and SDF of Chemical Structures and Extract Metadata
        logger.info(
            "Preprocessing ELN Articles to extract Chemical Structures and Metadata Extraction..."
        )
        preprocess_eln_files(
            eln_path=self.eln_path,
            eln_interim_path=self.ingestion_interim_path,
            eln_metadata_path=self.article_metadata_path,
            eln_chunks_path=self.chunks_path,
            file_handler=self.file_handler,
            source=self.source,
            workflow_id=self.workflow_id,
        )
        logger.info(f"ELN Articles Preprocessed Successfully!")

    def upload_to_s3(self):
        logger.info(f"Uploading {self.bioc_path} to S3...")
        uploaded_articles_count = upload_eln_articles(
            eln_path=self.eln_path,
            s3_eln_path=self.s3_eln_path,
            bioc_path=self.bioc_path,
            s3_bioc_path=self.s3_bioc_path,
            interim_path=self.ingestion_interim_path,
            s3_interim_path=self.s3_interim_path,
            metadata_path=self.article_metadata_path,
            s3_article_metadata_path=self.s3_article_metadata_path,
            failed_ingestion_path=self.failed_ingestion_path,
            s3_failed_ingestion_path=self.s3_failed_ingestion_path,
            chunks_path=self.chunks_path,
            s3_chunks_path=self.s3_chunks_path,
            file_handler=self.file_handler,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info(
            f"{uploaded_articles_count} Processed ELN Files uploaded to S3 Successfully!"
        )

    # Runs the combined process
    def run(
        self,
    ):
        self.eln_safe_filenames_generator()
        self.eln_formatter()
        self.eln_articles_preprocessor()
        if self.write_to_s3:
            self.upload_to_s3()


def main():
    """
    Main function to run the eln Ingestor with improved command-line interface.
    """
    logger.info("Execution Started")

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    write_to_s3 = False
    s3_paths = {}
    s3_file_handler = None
    if write_to_s3:
        # Get S3 Paths and file handler for writing to S3
        storage_type = "s3"
        s3_paths = paths_config["storage"][storage_type]
        s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    parser = argparse.ArgumentParser(
        description="Ingest eln articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_eln.articles_ingestor --workflow_id workflow1 --source eln",
    )

    parser.add_argument(
        "--workflow_id",
        "-wid",
        type=str,
        help="Workflow ID of JIT pipeline run",
    )

    parser.add_argument(
        "--source",
        "-src",
        type=str,
        help="Article source (e.g., pmc, ct, eln etc.)",
        default="eln",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    if not args.source:
        logger.error("No source provided. Defaulting to 'ct'.")
        source = "ct"
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    eln_source_config = paths_config["ingestion_source"][source]

    eln_ingestor = ELNIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        eln_source_config=eln_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    eln_ingestor.run()

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
