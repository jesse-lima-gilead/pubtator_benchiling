from src.data_ingestion.pmc_articles_extractor import extract_pmc_articles
from src.data_ingestion.pmc_to_bioc_converter import convert_pmc_to_bioc
from src.data_ingestion.fetch_metadata import MetadataExtractor
from src.data_ingestion.articles_summarizer import SummarizeArticle
from src.data_ingestion.prettify_xml import XMLFormatter
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
        query: str,
        article_ids: list,
        start_date: str,
        end_date: str,
        retmax: int = 50,
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
                    file_handler=file_handler,
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
        formatter = XMLFormatter(folder_path=self.bioc_path, file_handler=file_handler)
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
                    input_file_path=file_path, file_handler=file_handler
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
        query: str,
        article_ids: list,
        start_date: str,
        end_date: str,
        retmax: int = 50,
        metadata_storage_type: str = "file",
    ):
        self.pmc_articles_extractor(
            query=query,
            article_ids=article_ids,
            start_date=start_date,
            end_date=end_date,
            retmax=retmax,
        )
        self.articles_metadata_extractor(metadata_storage_type=metadata_storage_type)
        self.pmc_to_bioc_converter()
        self.articles_summarizer()


# Example usage
if __name__ == "__main__":
    logger.info("Execution Started")
    query = ""
    start_date = "1900"
    end_date = "2025"
    retmax = 25

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    # Retrieve dataset config
    dataset_config = config_loader.get_config("curated_dataset")
    article_ids = (
        # dataset_config["golden_dataset_article_ids"]
        # +
        dataset_config["thalidomide_articles_ids"]
    )

    sample_articles_id = ["2361529", "2480972"]

    pmc_ingestor = PMCIngestor(
        file_handler=file_handler,
        paths_config=paths,
    )
    pmc_ingestor.run(
        query=query,
        start_date=start_date,
        end_date=end_date,
        article_ids=article_ids,
        retmax=retmax,
        metadata_storage_type="file",
    )
    logger.info("Execution Completed")
