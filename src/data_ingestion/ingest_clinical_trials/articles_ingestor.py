import argparse

from src.data_ingestion.ingest_clinical_trials.ct_articles_summarizer import (
    articles_summarizer,
)
from src.data_ingestion.ingest_clinical_trials.ct_articles_extractor import (
    extract_ct_articles,
)
from src.data_ingestion.ingest_clinical_trials.ct_csv_to_bioc_converter import (
    convert_ct_csv_to_bioc,
)
from src.data_ingestion.ingest_clinical_trials.fetch_metadata import (
    articles_metadata_extractor,
)
from src.data_ingestion.ingest_pubmed.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class CTIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        ct_source_config: dict[str, str],
        source: str = "ct",
    ):
        self.ct_path = (
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
        self.ct_source_config = ct_source_config
        self.source = source
        # self.s3_io_util = S3IOUtil()

    def ct_articles_extractor(self):
        # Extract the CSV full clinical trials:
        logger.info("Extracting CT Articles...")
        extracted_articles_count = extract_ct_articles(
            ct_path=self.ct_path,
            file_handler=self.file_handler,
            ct_source_config=self.ct_source_config,
            source=self.source,
        )
        logger.info(f"{extracted_articles_count} CT Articles Extracted Successfully!")
        return extracted_articles_count

    def ct_articles_processor(self):
        logger.info("Processing CT Articles...")

        for file in self.file_handler.list_files(self.ct_path):
            if file.endswith(".csv"):
                file_path = self.file_handler.get_file_path(self.ct_path, file)
                ct_df = self.file_handler.read_csv_file(
                    file_path=file_path, as_pandas=True
                )
                # Normalize column names
                ct_df.columns = [col.lower().replace(" ", "_") for col in ct_df.columns]

                # Rename required columns
                ct_df = ct_df.rename(
                    columns={"nct_number": "nct_id", "study_title": "title"}
                )

                # fetch_metadata
                articles_metadata_extractor(
                    ct_df, self.article_metadata_path, self.file_handler
                )
                logger.info(f"All Metadata extracted from {file_path}")

                # Extract Summaries
                articles_summarizer(ct_df, self.summary_path, self.file_handler)
                logger.info(f"All Summary extracted from {file_path}")

                # CSV record to BioC file format conversion
                convert_ct_csv_to_bioc(ct_df, self.bioc_path, self.file_handler)
                logger.info(
                    f"All CT CSV files converted to BioC format from {file_path}"
                )

    # Runs the combined process
    def run(
        self,
    ):
        self.ct_articles_extractor()
        self.ct_articles_processor()


def main():
    """
    Main function to run the CT Ingestor with improved command-line interface.
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

    parser = argparse.ArgumentParser(
        description="Ingest CT articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_clinical_trials.articles_ingestor --workflow_id workflow123 --source ct",
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
        default="ct",
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

    ct_source_config = paths_config["ingestion_source"][source]

    ct_ingestor = CTIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        ct_source_config=ct_source_config,
    )

    ct_ingestor.run()

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
