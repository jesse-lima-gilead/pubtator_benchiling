from src.data_ingestion.ingest_internal.internal_ingestor_factory import (
    InternalIngestorFactory,
)
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class InternalIngestor:
    def __init__(
        self,
        file_handler: FileHandler,
        paths_config: dict[str, str],
    ):
        self.internal_docs_path = paths_config["internal_docs_path"]
        self.internal_docs_interim_path = paths_config["internal_docs_interim_path"]
        self.bioc_path = f'../{paths_config["bioc_path"]}'
        self.article_metadata_path = f'../{paths_config["metadata_path"]}'
        self.summary_path = f'../{paths_config["summary_path"]}'
        self.file_handler = file_handler

    def _get_doc_processor(self, file_name):
        if ".docx" in file_name:
            return InternalIngestorFactory.get_processor("docx")
        elif ".pptx" in file_name:
            return InternalIngestorFactory.get_processor("pptx")
        raise ValueError(f"Unsupported file extension: {file_name}")

    def process_internal_documents(self):
        logger.info("Started processing of internal docs to convert to BioC")

        for internal_doc in self.file_handler.list_files(self.internal_docs_path):
            logger.info(f"Started file format conversion for doc: {internal_doc}")
            doc_processor = self._get_doc_processor(internal_doc)
            doc_processor.run(
                self.file_handler,
                internal_doc,
                self.internal_docs_path,
                self.bioc_path,
                internal_docs_interim_path=self.internal_docs_interim_path,
            )

        logger.info(
            f"Succesfully completed BioC file format conversions for internal docs"
        )

    # dummy method
    def articles_metadata_extractor(self):
        # Fetch and store metadata of extracted articles
        # logger.info("Fetching metadata for the articles...")
        for file in self.file_handler.list_files(self.bioc_path):
            metadata_json_file_name = file.replace(".xml", "_metadata.json")
            metadata_path = self.file_handler.get_file_path(
                self.article_metadata_path, metadata_json_file_name
            )
            # temporary empty metadata dic for internal documents
            metadata_dic = {}
            self.file_handler.write_file_as_json(metadata_path, metadata_dic)
            logger.info(f"Successfully fetched metadata for the doc: {metadata_path}")

    # dummy method
    def articles_summarizer(self):
        # Generate articles summaries
        # logger.info("Generating summaries for the articles using BioC XMLs...")
        for file in self.file_handler.list_files(self.bioc_path):
            # temporary empty summary for internal documents
            summary = ""
            summary_file_name = file.replace(".xml", ".txt")
            summary_file_path = self.file_handler.get_file_path(
                self.summary_path, summary_file_name
            )
            self.file_handler.write_file(summary_file_path, summary)
            logger.info(f"Summary generated for: {file}")

    def run(self):
        self.process_internal_documents()
        self.articles_metadata_extractor()
        self.articles_summarizer()


# Example usage
if __name__ == "__main__":
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

    internal_ingestor = InternalIngestor(
        file_handler=file_handler,
        paths_config=paths,
    )
    internal_ingestor.run()

    logger.info("Execution Completed")

# ToDo:
# - What could be best the output type of file.
# - Which type is easier to convert to pubtator/bioc, is it txt or xml or md or html or something else.
# - Test out the pipelines to see which one works best.
# - Add the following:
# 1. Add pubtator_utils -> logger, config_loader, file_handler
# 2. Add logic to write the Metadata as JSON
# 3. Test the pipeline with sample docs
# 4. Add pdf extraction logic
