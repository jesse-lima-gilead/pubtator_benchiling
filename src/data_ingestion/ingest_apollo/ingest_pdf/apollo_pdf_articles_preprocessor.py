import os
from pathlib import Path
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_apollo.ingest_pdf.apollo_pdf_tables_processor import (
    process_tables,
)


# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve paths config
paths = config_loader.get_config("paths")
storage_type = paths["storage"]["type"]

# Get file handler instance from factory
file_handler = FileHandlerFactory.get_handler(storage_type)
# Retrieve paths from config
paths_config = paths["storage"][storage_type]


def convert_apollo_to_html(
    apollo_doc: str,
    apollo_path: str,
    apollo_interim_path: str,
    failed_ingestion_path: str,
    input_doc_type: str = "pdf",  # ["pdf","doc"],
    output_doc_type: str = "html",
):
    pandoc_processor = PandocProcessor(pandoc_executable="pandoc")

    if apollo_doc.endswith(".pdf") and not apollo_doc.startswith("~$"):
        logger.info(f"Started file format conversion for doc: {apollo_doc}")

        input_doc_path = Path(apollo_path) / apollo_doc

        # Create an output directory for this pdf
        apollo_dir_name = apollo_doc.replace(".pdf", "")
        apollo_output_dir = Path(apollo_interim_path) / apollo_dir_name
        apollo_output_dir.mkdir(parents=True, exist_ok=True)
        output_doc_path = Path(apollo_output_dir) / apollo_doc.replace(
            f".{input_doc_type}", f".{output_doc_type}"
        )
        media_dir = Path(apollo_output_dir)
        logger.info(f"Converting {input_doc_path} to {output_doc_path}")

        pandoc_processor.convert(
            input_path=input_doc_path,
            output_path=output_doc_path,
            input_format=input_doc_type,
            output_format=output_doc_type,
            failed_ingestion_path=failed_ingestion_path,
            extract_media_dir=media_dir,
        )

        logger.info(f"{apollo_doc} Converted to HTML Successfully!")
    else:
        logger.error(f"{apollo_doc} is not a DocX file to convert.")

def convert_apollo_md_to_html(
    apollo_doc: str,
    apollo_path: str,
    apollo_interim_path: str,
    failed_ingestion_path: str,
    input_doc_type: str = "markdown",  # ["pdf","doc"],
    output_doc_type: str = "html",
):
    pandoc_processor = PandocProcessor(pandoc_executable="pandoc")

    if apollo_doc.endswith(".md") and not apollo_doc.startswith("~$"):
        logger.info(f"Started file format conversion for doc: {apollo_doc}")

        # Create an output directory for this pdf
        apollo_dir_name = apollo_doc.replace(".md", "")
        input_doc_path = Path(f"{apollo_path}") / apollo_doc
        apollo_output_dir = Path(apollo_interim_path) / apollo_dir_name
        apollo_output_dir.mkdir(parents=True, exist_ok=True)
        output_doc_path = Path(apollo_output_dir) / apollo_doc.replace(
            f".md", f".{output_doc_type}"
        )
        media_dir = Path(apollo_output_dir)
        logger.info(f"Converting {input_doc_path} to {output_doc_path}")

        pandoc_processor.convert(
            input_path=input_doc_path,
            output_path=output_doc_path,
            input_format=input_doc_type,
            output_format=output_doc_type,
            failed_ingestion_path=failed_ingestion_path,
        )

        logger.info(f"{apollo_doc} Converted to HTML Successfully!")
    else:
        logger.error(f"{apollo_doc} is not a DocX file to convert.")

def extract_tables_from_apollo_html(
    apollo_file_name: str,
    apollo_interim_path: str,
    apollo_metadata_path: str,
    apollo_embeddings_path: str,
):
    # for apollo_html_dir in os.listdir(apollo_interim_path):
    apollo_html_dir = apollo_file_name.replace(".md", "")
    logger.info(f"Processing Apollo HTML file: {apollo_html_dir}")
    apollo_html_dir_path = Path(apollo_interim_path) / apollo_html_dir
    apollo_html_file_path = apollo_html_dir_path / (apollo_html_dir + ".html")
    apollo_html_file_name = apollo_html_dir + ".html"
    if os.path.exists(apollo_html_file_path):
        logger.info(f"HTML file found: {apollo_html_file_name}. Extracting Tables...")

        # Read the HTML content
        html_content = file_handler.read_file(apollo_html_file_path)

        # Process tables in Apollo HTML
        html_with_flat_tables, table_details = process_tables(
            html_str=html_content,
            source_filename=apollo_html_file_name,
            output_tables_path=apollo_html_dir_path,
            article_metadata_path=apollo_metadata_path,
            table_state="remove",
        )

        # Make sure apollo_embeddings_path exists
        Path(apollo_embeddings_path).mkdir(parents=True, exist_ok=True)
        # Save table details as JSON
        logger.info(f"Saving extracted table details to JSON...")
        file_path = f"{apollo_embeddings_path}/{apollo_html_dir}_tables.json"
        # file_handler.write_file_as_json(file_path=file_path, data=table_details)
        file_handler.write_file_as_json(file_path=file_path, content=table_details)
        logger.info(f"Table details saved to {file_path}")

        # Write back modified HTML with flat table text
        file_handler.write_file(apollo_html_file_path, html_with_flat_tables)
        logger.info(
            f"Successfully Extracted {len(table_details)} tables from {apollo_html_file_name}"
        )
    else:
        logger.error(f"{apollo_html_file_path} does not exist for table extraction.")
