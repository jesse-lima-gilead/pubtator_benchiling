import os
import uuid
from pathlib import Path
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_rfd.rfd_tables_processor import process_tables

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()


def get_article_metadata(article_metadata_path: str) -> dict:
    # Read the article metadata JSON file and return metadata
    try:
        with open(article_metadata_path, "r") as f:
            import json

            article_metadata = json.load(f)
        return article_metadata
    except Exception as e:
        logger.error(
            f"Error reading article metadata from {article_metadata_path}: {e}"
        )
        return {}


def get_table_docs(tables_summary_path: str, chunks_path: str, article_metadata: dict):
    # Read the tables summary JSON file and metadata file
    try:
        with open(tables_summary_path, "r") as f:
            import json

            tables_summary = json.load(f)

        # Generate documentation for each table which is present as list of dicts in tables_summary
        for table in tables_summary:
            table_docs = {}
            table_docs["table_uuid"] = uuid.uuid4()
            table_docs = {**table_docs, **article_metadata}

            # Write the table docs at chunks path

        return tables_summary
    except Exception as e:
        logger.error(f"Error reading tables summary from {tables_summary_path}: {e}")
        return {}


def generate_tables_docs(
    rfd_path: str,
    rfd_interim_path: str,
    article_metadata_path: str,
    chunks_path: str,
    file_handler: FileHandler,
    paths_config: dict[str, str],
):
    for rfd_html_dir in os.listdir(rfd_interim_path):
        rfd_article_name = rfd_html_dir
        article_metadata = get_article_metadata(article_metadata_path)
        tables_summary_path = (
            Path(rfd_interim_path) / rfd_html_dir / f"{rfd_html_dir}_tables.json"
        )
        if os.path.exists(tables_summary_path):
            logger.info(f"Generating Table Docs for article: {rfd_article_name}")
            get_table_docs(
                tables_summary_path=tables_summary_path,
                chunks_path=chunks_path,
                article_metadata=article_metadata,
            )

        else:
            tables_metadata = {}
            logger.warning(
                f"Tables summary file not found for {rfd_article_name}, skipping table docs generation."
            )
