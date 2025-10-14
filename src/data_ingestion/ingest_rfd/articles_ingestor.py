import argparse
from pathlib import Path
from src.data_ingestion.ingest_rfd.rfd_summarizer import (
    rfd_summarizer,
)
from src.data_ingestion.ingest_rfd.rfd_articles_extractor import extract_rfd_articles
from src.data_ingestion.ingest_rfd.rfd_articles_preprocessor import (
    generate_safe_filename,
    convert_rfd_to_html,
    extract_tables_from_rfd_html,
)
from src.data_ingestion.ingest_rfd.rfd_to_bioc_converter import (
    convert_rfd_html_to_bioc,
)

from src.data_ingestion.ingest_rfd.rfd_metadata_extractor import (
    articles_metadata_extractor,
)
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_rfd.rfd_articles_uploader import upload_rfd_articles
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class RFDIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        rfd_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "rfd",
        summarization_pipe=None,
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.rfd_path = (
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
        self.embeddings_path = (
            paths_config["embeddings_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler
        self.rfd_source_config = rfd_source_config
        self.source = source
        self.pandoc_processor = PandocProcessor(pandoc_executable="pandoc")
        self.summarization_pipe = summarization_pipe

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_rfd_path = self.s3_paths_config.get("ingestion_path", "").replace(
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
            self.s3_embeddings_path = self.s3_paths_config.get(
                "embeddings_path", ""
            ).replace("{source}", source)
        else:
            self.s3_pmc_path = (
                self.s3_bioc_path
            ) = self.s3_article_metadata_path = self.s3_summary_path = None

    # def rfd_articles_extractor(self):
    #     # Extract the RFD Articles:
    #     logger.info("Extracting RFD Articles...")
    #     extracted_articles_count = extract_rfd_articles(
    #         rfd_path=self.rfd_path,
    #         file_handler=self.file_handler,
    #         rfd_source_config=self.rfd_source_config,
    #         source=self.source,
    #     )
    #     logger.info(f"{extracted_articles_count} RFD Articles Extracted Successfully!")

    def rfd_safe_filenames_generator(self):
        # Generate Safe file name for the extracted articles
        logger.info("Generating Safe file names for the extracted articles...")
        safe_file_name_cnt = generate_safe_filename(self.rfd_path)
        logger.info(
            f"Safe file names generated for {safe_file_name_cnt} articles successfully!"
        )

    def metadata_extractor(self):
        logger.info(f"Extracting metadata...")
        extracted_metadata_count = articles_metadata_extractor(
            rfd_path=self.rfd_path,
            metadata_path=self.article_metadata_path,
            file_handler=self.file_handler,
        )
        logger.info(f"Metadata extracted for {extracted_metadata_count} RFD Articles!")

    def rfd_articles_preprocessor(self):
        # Convert RFD Articles from docx to html
        logger.info("Converting RFD Articles from docx to html...")
        convert_rfd_to_html(
            rfd_path=self.rfd_path,
            rfd_interim_path=self.ingestion_interim_path,
            failed_ingestion_path=self.failed_ingestion_path,
            input_doc_type="docx",
            output_doc_type="html",
        )
        logger.info(f"RFD Articles Converted to HTML Successfully!")

        # Extract Tables from RFD HTML Articles
        logger.info("Extracting Tables from RFD HTML Articles...")
        extract_tables_from_rfd_html(
            rfd_interim_path=self.ingestion_interim_path,
            rfd_metadata_path=self.article_metadata_path,
            rfd_embeddings_path=self.embeddings_path,
        )
        logger.info(f"Tables Extracted from RFD HTML Articles Successfully!")

    def rfd_html_to_bioc_converter(self):
        # Convert RFD HTML Articles to BioC XML
        logger.info(f"Converting RFD HTML Articles to BioC XML...")
        converted_articles_count = convert_rfd_html_to_bioc(
            rfd_interim_path=self.ingestion_interim_path,
            bioc_path=self.bioc_path,
        )
        logger.info(
            f"{converted_articles_count} RFD Articles Converted to BioC Successfully!"
        )

    def generate_summary(self):
        logger.info(f"Generating Summary from {self.bioc_path}...")
        rfd_summaries_count = rfd_summarizer(
            bioc_path=self.bioc_path,
            summary_path=self.summary_path,
            file_handler=self.file_handler,
            summarization_pipe=self.summarization_pipe,
        )
        logger.info(f"Summary generated for {rfd_summaries_count} RFD Articles!")

    def upload_to_s3(self):
        logger.info(f"Uploading {self.bioc_path} to S3...")
        uploaded_articles_count = upload_rfd_articles(
            rfd_path=self.rfd_path,
            s3_rfd_path=self.s3_rfd_path,
            bioc_path=self.bioc_path,
            s3_bioc_path=self.s3_bioc_path,
            interim_path=self.ingestion_interim_path,
            s3_interim_path=self.s3_interim_path,
            metadata_path=self.article_metadata_path,
            s3_article_metadata_path=self.s3_article_metadata_path,
            summary_path=self.summary_path,
            s3_summary_path=self.s3_summary_path,
            embeddings_path=self.embeddings_path,
            s3_embeddings_path=self.s3_embeddings_path,
            file_handler=self.file_handler,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info(
            f"{uploaded_articles_count} Processed RFD Files uploaded to S3 Successfully!"
        )

    # Runs the combined process
    def run(
        self,
    ):
        # self.rfd_articles_extractor()
        self.rfd_safe_filenames_generator()
        self.metadata_extractor()
        self.rfd_articles_preprocessor()
        self.rfd_html_to_bioc_converter()
        self.generate_summary()
        self.upload_to_s3()


def main():
    """
    Main function to run the RFD Ingestor with improved command-line interface.
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
        description="Ingest RFD articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_rfd.articles_ingestor --workflow_id workflow123 --source rfd",
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
        help="Article source (e.g., pmc, ct, rfd etc.)",
        default="rfd",
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

    rfd_source_config = paths_config["ingestion_source"][source]

    rfd_ingestor = RFDIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        rfd_source_config=rfd_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    rfd_ingestor.run()

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
