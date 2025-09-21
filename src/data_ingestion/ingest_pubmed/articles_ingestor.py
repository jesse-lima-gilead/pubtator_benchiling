import argparse
from src.data_ingestion.ingest_pubmed.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.ingest_pubmed.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.ingest_pubmed.fetch_metadata import MetadataExtractor
from src.data_ingestion.ingest_pubmed.articles_summarizer import SummarizeArticle
from src.data_ingestion.ingest_pubmed.prettify_xml import XMLFormatter
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PMCIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        write_to_s3: bool,
        source: str = "pmc",
        summarization_pipe=None,
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.pmc_path = (
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
        self.summarization_pipe = summarization_pipe

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_pmc_path = self.s3_paths_config.get("ingestion_path", "").replace(
                "{source}", source
            )
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

    def pmc_articles_extractor(
        self,
        article_ids: list[str],
        query: str = "",
        start_date: str = "1900",
        end_date: str = "2025",
        retmax: int = 25,
    ):
        # Extract the free full text articles from PMC:
        logger.info("Extracting PMC Articles...")
        extracted_articles_count = extract_pmc_articles(
            query=query,
            article_ids=article_ids,
            start_date=start_date,
            end_date=end_date,
            pmc_path=self.pmc_path,
            file_handler=self.file_handler,
            s3_pmc_path=self.s3_pmc_path,
            s3_file_handler=self.s3_file_handler,
            write_to_s3=self.write_to_s3,
            retmax=retmax,
        )
        logger.info(f"{extracted_articles_count} PMC Articles Extracted Successfully!")
        return extracted_articles_count

    def articles_metadata_extractor(self, metadata_storage_type: str = "file"):
        # Fetch and store metadata of extracted articles
        logger.info("Fetching metadata for the articles...")
        for file in self.file_handler.list_files(self.pmc_path):
            if file.endswith(".xml"):
                file_path = self.file_handler.get_file_path(self.pmc_path, file)
                metadata_json_file_name = file.replace(".xml", "_metadata.json")
                metadata_path = self.file_handler.get_file_path(
                    self.article_metadata_path, metadata_json_file_name
                )
                s3_metadata_path = (
                    self.s3_file_handler.get_file_path(
                        self.s3_article_metadata_path, metadata_json_file_name
                    )
                    if self.write_to_s3
                    else None
                )
                metadata_extractor = MetadataExtractor(
                    file_path=file_path,
                    metadata_path=metadata_path,
                    file_handler=self.file_handler,
                    s3_metadata_path=s3_metadata_path,
                    s3_file_handler=self.s3_file_handler,
                    write_to_s3=self.write_to_s3,
                )
                if metadata_storage_type == "file":
                    metadata_extractor.save_metadata_as_json()
                    logger.info(f"Metadata for {file} saved to file")
                else:
                    logger.error("Invalid metadata storage type provided")

    def pmc_to_bioc_converter(self):
        # Convert the PMC Articles to BioC File Format:
        logger.info("Converting PMC Articles to BioC XML...")
        for file in self.file_handler.list_files(self.pmc_path):
            if file.endswith(".xml"):
                pmc_file_path = self.file_handler.get_file_path(self.pmc_path, file)
                convert_pmc_to_bioc(
                    pmc_file_path,
                    self.bioc_path,
                    self.file_handler,
                    self.s3_bioc_path,
                    self.s3_file_handler,
                    self.write_to_s3,
                )

    def prettify_bioc_xml(self):
        # Prettify the BioC XML files:
        logger.info("Prettifying the BioC XML files...")
        formatter = XMLFormatter(
            folder_path=self.bioc_path, file_handler=self.file_handler
        )
        formatter.process_folder()

    def articles_summarizer(self):
        # Generate articles summaries
        logger.info("Generating summaries for the articles using BioC XMLs...")
        for file in self.file_handler.list_files(self.bioc_path):
            if file.endswith(".xml"):
                logger.info(f"Generating summary for: {file}")
                file_path = self.file_handler.get_file_path(self.bioc_path, file)
                # file_path = os.path.join(self.bioc_local_path, file)
                summarizer = SummarizeArticle(
                    input_file_path=file_path,
                    file_handler=self.file_handler,
                    summarization_pipe=self.summarization_pipe,
                )
                summary = summarizer.summarize()
                summary_file_name = file.replace(".xml", ".txt")
                summary_file_path = self.file_handler.get_file_path(
                    self.summary_path, summary_file_name
                )
                self.file_handler.write_file(summary_file_path, summary)
                logger.info(f"Summary generated for: {file}")

                if self.write_to_s3:
                    # Save to S3
                    s3_summary_file_path = self.s3_file_handler.get_file_path(
                        self.s3_summary_path, summary_file_name
                    )
                    self.s3_file_handler.write_file(s3_summary_file_path, summary)
                    logger.info(f"Summary saved to S3: {s3_summary_file_path}")

    # Runs the combined process
    def run(
        self,
        article_ids: list,
        metadata_storage_type: str = "file",
    ):
        self.pmc_articles_extractor(article_ids=article_ids)
        self.articles_metadata_extractor(metadata_storage_type=metadata_storage_type)
        self.pmc_to_bioc_converter()
        self.articles_summarizer()


def main():
    """
    Main function to run the PMC Ingestor with improved command-line interface.
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
        description="Ingest PMC articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_pubmed.articles_ingestor --workflow_id workflow123 --source pmc",
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
        default="pmc",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    if not args.source:
        logger.error("No source provided. Defaulting to 'pmc'.")
        source = "pmc"
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    # Read article IDs from the specified file
    article_ids_file_path = (
        paths["jit_ingestion_path"]
        .replace("{workflow_id}", workflow_id)
        .replace("{source}", source)
    )
    article_ids = []
    try:
        with open(article_ids_file_path, "r") as file:
            for line in file:
                # Strip whitespace (like newline characters) and convert to int
                article_ids.append(line.strip())
    except FileNotFoundError:
        print(f"Error: The file '{article_ids_file_path}' was not found.")
    except ValueError:
        print(
            f"Error: Could not fetch the article ids from the file '{article_ids_file_path}'. Ensure it contains "
            f"valid IDs."
        )

    if not article_ids:
        logger.error("No article IDs found in the provided file.")
        return

    logger.info(f"Article IDs to ingest: {article_ids}")

    pmc_ingestor = PMCIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    pmc_ingestor.run(
        article_ids=article_ids,
        metadata_storage_type="file",
    )

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
    # logger.info("Execution Started")
    # query = ""
    # start_date = "1900"
    # end_date = "2025"
    # retmax = 25
    #
    # # Initialize the config loader
    # config_loader = YAMLConfigLoader()
    #
    # # Retrieve paths config
    # paths_config = config_loader.get_config("paths")
    # storage_type = paths_config["storage"]["type"]
    #
    # # Get file handler instance from factory
    # file_handler = FileHandlerFactory.get_handler(storage_type)
    # # Retrieve paths from config
    # paths = paths_config["storage"][storage_type]
    #
    # # Retrieve dataset config
    # dataset_config = config_loader.get_config("curated_dataset")
    # article_ids = (
    #     # dataset_config["golden_dataset_article_ids"]
    #     # +
    #     dataset_config["thalidomide_articles_ids"]
    # )
    #
    # sample_articles_id = ["2361529", "2480972"]
    #
    # pmc_ingestor = PMCIngestor(
    #     file_handler=file_handler,
    #     paths_config=paths,
    # )
    # pmc_ingestor.run(
    #     article_ids=article_ids,
    #     metadata_storage_type="file",
    # )
    # logger.info("Execution Completed")
