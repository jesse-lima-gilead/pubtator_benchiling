import os
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

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PMCIngestor:
    def __init__(
        self,
        file_handler: FileHandler,
        paths_config: dict[str, str],
    ):
        self.pmc_path = paths_config["pmc_path"]
        self.bioc_path = paths_config["bioc_path"]
        self.article_metadata_path = paths_config["metadata_path"]
        self.summary_path = paths_config["summary_path"]
        self.file_handler = file_handler
        # self.s3_io_util = S3IOUtil()

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
                metadata_extractor = MetadataExtractor(
                    file_path=file_path,
                    metadata_path=metadata_path,
                    file_handler=self.file_handler,
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
                convert_pmc_to_bioc(pmc_file_path, self.bioc_path, self.file_handler)

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
                    input_file_path=file_path, file_handler=self.file_handler
                )
                summary = summarizer.summarize()
                summary_file_name = file.replace(".xml", ".txt")
                summary_file_path = self.file_handler.get_file_path(
                    self.summary_path, summary_file_name
                )
                self.file_handler.write_file(summary_file_path, summary)
                logger.info(f"Summary generated for: {file}")

    # Runs the combined process
    def run(
        self,
        article_ids: list,
        metadata_storage_type: str = "file",
    ):
        self.pmc_articles_extractor(article_ids=article_ids)
        self.articles_metadata_extractor(metadata_storage_type=metadata_storage_type)
        self.pmc_to_bioc_converter()
        # self.articles_summarizer()


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

    parser = argparse.ArgumentParser(
        description="Ingest articles",
        epilog="Example: python3 -m articles_ingestor.py --ids_file_name article_ids.txt",
    )

    parser.add_argument(
        "--ids_file_name",
        "-i",
        default="article_ids.txt",
        help="Directories to process (if none specified, uses current directory)",
    )

    args = parser.parse_args()

    if not args.ids_file_name:
        logger.info(
            "No article IDs file path provided. Using default path: /scratch/pubtator/data/staging/article_ids.txt"
        )
        ids_file_name = "article_ids.txt"
    else:
        ids_file_name = args.ids_file_name
        logger.info(f"Article Ids file name registered at: {ids_file_name}")

    # Read article IDs from the specified file
    article_ids_base_path = paths["jit_ingestion_path"]
    article_ids_file_path = os.path.join(article_ids_base_path, ids_file_name)
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
        file_handler=file_handler,
        paths_config=paths,
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
