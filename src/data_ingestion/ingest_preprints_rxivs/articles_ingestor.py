import argparse

from src.data_ingestion.ingest_preprints_rxivs.fetch_metadata import (
    preprints_articles_metadata_extractor,
)
from src.data_ingestion.ingest_preprints_rxivs.preprint_articles_summarizer import (
    preprint_articles_summarizer,
)
from src.data_ingestion.ingest_preprints_rxivs.preprint_pdf_to_bioc_converter import (
    convert_preprint_pdf_to_bioc,
)
from src.data_ingestion.ingest_preprints_rxivs.preprint_articles_extractor import (
    extract_preprints_articles,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PrePrintsIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        preprints_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "preprint",
        summarization_pipe=None,
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.preprints_path = (
            paths_config["ingestion_path"]
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
        self.file_handler = file_handler
        self.preprints_source_config = preprints_source_config
        self.source = source
        self.summarization_pipe = summarization_pipe

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_bioc_path = self.s3_paths_config.get("bioc_path", "").replace(
                "{source}", source
            )
            self.s3_article_metadata_path = self.s3_paths_config.get(
                "metadata_path", ""
            ).replace("{source}", source)
            self.s3_summary_path = self.s3_paths_config.get("summary_path", "").replace(
                "{source}", source
            )
        else:
            self.s3_pmc_path = (
                self.s3_bioc_path
            ) = self.s3_article_metadata_path = self.s3_summary_path = None

    # def preprints_articles_extractor(self):
    #     # Extract the preprints_bioarxiv pdfs:
    #     logger.info("Extracting Preprints Bioarxiv Articles...")
    #     extracted_articles_count = extract_preprints_articles(
    #         preprints_path=self.preprints_path,
    #         file_handler=self.file_handler,
    #         preprints_source_config=self.preprints_source_config,
    #         source=self.source,
    #     )
    #     logger.info(
    #         f"{extracted_articles_count} Preprint Articles Extracted Successfully!"
    #     )
    #     return extracted_articles_count

    def preprints_processor(self):
        logger.info("Processing Preprints Bioarxiv Articles...")

        for file in self.file_handler.list_files(self.preprints_path):
            if file.endswith(".pdf"):
                preprint_file_path = self.file_handler.get_file_path(
                    self.preprints_path, file
                )

                logger.info(f"Started Metadata extraction for {file}")
                # fetch_metadata
                metadata = preprints_articles_metadata_extractor(
                    preprint_file_path,
                    self.article_metadata_path,
                    self.file_handler,
                    self.write_to_s3,
                    self.s3_article_metadata_path,
                    self.s3_file_handler,
                )

                logger.info(f"Started PDF to BioC conversion for {file}")
                # pdf to bioc conversion
                convert_preprint_pdf_to_bioc(
                    preprint_file_path,
                    self.bioc_path,
                    metadata,
                    self.file_handler,
                    self.write_to_s3,
                    self.s3_bioc_path,
                    self.s3_file_handler,
                )

                logger.info(f"Started Summary creation for {file}")
                # Summary generation
                preprint_articles_summarizer(
                    preprint_file_path,
                    self.summary_path,
                    metadata,
                    self.file_handler,
                    self.write_to_s3,
                    self.s3_summary_path,
                    self.s3_file_handler,
                    self.summarization_pipe,
                )

    # Runs the combined process
    def run(
        self,
    ):
        # self.preprints_articles_extractor()
        self.preprints_processor()


def main():
    """
    Main function to run the Preprint Ingestor with improved command-line interface.
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

    write_to_s3 = True
    s3_paths = {}
    s3_file_handler = None
    if write_to_s3:
        # Get S3 Paths and file handler for writing to S3
        storage_type = "s3"
        s3_paths = paths_config["storage"][storage_type]
        s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    parser = argparse.ArgumentParser(
        description="Ingest preprint bioarxiv articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_preprints_rxivs.articles_ingestor --workflow_id workflow123 --source preprint",
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
        help="Article source (e.g., pmc, ct, rfd, preprint etc.)",
        default="preprint",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    if not args.source:
        logger.error("No source provided. Defaulting to 'preprint'.")
        source = "preprint"
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    preprints_source_config = paths_config["ingestion_source"][source]

    preprints_ingestor = PrePrintsIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        preprints_source_config=preprints_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    preprints_ingestor.run()

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
