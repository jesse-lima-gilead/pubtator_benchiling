import argparse

from src.data_ingestion.ingest_apollo.fetch_metadata import (
    metadata_extractor,
)

from src.data_ingestion.ingest_apollo.ingest_pdf.apollo_pdf_articles_preprocessor import (
    convert_apollo_to_html,
    convert_apollo_md_to_html,
    extract_tables_from_apollo_html,
)

from src.data_ingestion.ingest_apollo.ingest_pdf.apollo_pdf_articles_uploader import (
    upload_apollo_articles,
)

from src.data_ingestion.ingest_apollo.ingest_pdf.apollo_pdf_to_bioc_converter import (
    convert_apollo_html_to_bioc,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class apolloPDFIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        apollo_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "apollo",
        summarization_pipe=None,
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
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
        self.apollo_source_config = apollo_source_config
        self.source = source
        self.summarization_pipe = summarization_pipe
        self.write_to_s3 = write_to_s3

        # Pop known keys (consumes them from kwargs)
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
                self.s3_interim_path
            ) = (
                self.s3_article_metadata_path
            ) = (
                self.s3_summary_path
            ) = self.s3_embeddings_path = self.s3_failed_ingestion_path = None

    def fetch_metadata_from_s3(self, apollo_doc: str):
        # Extract metadata from apollo Articles
        logger.info(f"Fetching metadata for {apollo_doc} from S3...")
        if apollo_doc.endswith(".md") and not apollo_doc.startswith("~$"):
            apollo_doc_metadata = metadata_extractor(
                file=apollo_doc,
                article_metadata_path=self.article_metadata_path,
                local_file_handler=self.file_handler,
                s3_article_metadata_path=self.s3_article_metadata_path,
                s3_file_handler=self.s3_file_handler,
            )
            if apollo_doc_metadata:
                logger.info(f"Metadata extracted for {apollo_doc}")
            else:
                logger.info(f"No metadata extracted for {apollo_doc}")
        else:
            logger.error(f"{apollo_doc} is not a pdf file.")

    def apollo_articles_preprocessor(self, file_name: str):
        # Convert apollo Articles from pdf to html
        logger.info(f"Converting apollo Articles from md to html for {file_name}")

        convert_apollo_md_to_html(
            apollo_doc=file_name,
            apollo_path=self.apollo_path,
            apollo_interim_path=self.ingestion_interim_path,
            failed_ingestion_path=self.failed_ingestion_path,
            input_doc_type="markdown",
            output_doc_type="html",
        )

        # Extract Tables from apollo HTML Articles
        logger.info(f"Extracting Tables from apollo HTML Articles for {file_name}")

        extract_tables_from_apollo_html(
            apollo_file_name=file_name,
            apollo_interim_path=self.ingestion_interim_path,
            apollo_metadata_path=self.article_metadata_path,
            apollo_embeddings_path=self.embeddings_path,
        )

    def apollo_html_to_bioc_converter(self, file_name: str):
        # Convert apollo HTML Articles to BioC XML
        logger.info(f"Converting apollo HTML Articles to BioC XML for {file_name}")
        convert_apollo_html_to_bioc(
            apollo_file_name=file_name,
            apollo_interim_path=self.ingestion_interim_path,
            bioc_path=self.bioc_path,
            metadata_path=self.article_metadata_path,
        )

    #
    # def generate_summary(self):
    #     logger.info(f"Generating Summary from {self.bioc_path}...")
    #     apollo_summaries_count = apollo_summarizer(
    #         bioc_path=self.bioc_path,
    #         summary_path=self.summary_path,
    #         file_handler=self.file_handler,
    #         summarization_pipe=self.summarization_pipe,
    #     )
    #     logger.info(f"Summary generated for {apollo_summaries_count} apollo Articles!")
    #

    ##Upload to S3 made common across Apollo Docs
    # def upload_to_s3(self):
    #     logger.info(f"Uploading {self.bioc_path} to S3...")
    #     uploaded_articles_count = upload_apollo_articles(
    #         apollo_path=self.apollo_path,
    #         s3_apollo_path=self.s3_apollo_path,
    #         bioc_path=self.bioc_path,
    #         s3_bioc_path=self.s3_bioc_path,
    #         interim_path=self.ingestion_interim_path,
    #         s3_interim_path=self.s3_interim_path,
    #         summary_path=self.summary_path,
    #         s3_summary_path=self.s3_summary_path,
    #         embeddings_path=self.embeddings_path,
    #         s3_embeddings_path=self.s3_embeddings_path,
    #         failed_ingestion_path=self.failed_ingestion_path,
    #         s3_failed_ingestion_path=self.s3_failed_ingestion_path,
    #         file_handler=self.file_handler,
    #         s3_file_handler=self.s3_file_handler,
    #     )
    #     logger.info(
    #         f"{uploaded_articles_count} Processed apollo Files uploaded to S3 Successfully!"
    #     )

    # Runs the combined process
    def run(
        self,
        file_name: str,
    ):
        self.fetch_metadata_from_s3(file_name)
        self.apollo_articles_preprocessor(file_name)
        self.apollo_html_to_bioc_converter(file_name)
        # self.upload_to_s3()


def main():
    """
    Main function to run the apollo Ingestor with improved command-line interface.
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
        description="Ingest apollo articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_apollo.ingest_pdf.pdf_articles_ingestor --workflow_id workflow1 --source apollo",
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
        help="Article source (e.g., pmc, ct, apollo etc.)",
        default="apollo",
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

    apollo_source_config = paths_config["ingestion_source"][source]

    apollo_ingestor = apolloPDFIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        apollo_source_config=apollo_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    apollo_ingestor.run(file_name="")

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
