import os
from pathlib import Path
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_rfd.rfd_tables_processor import process_tables
from src.data_ingestion.ingestion_utils.document_data_insertion import insert_document_data

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


# def generate_safe_filename(rfd_path: str):
#     safe_file_name_cnt = 0
#     logger.info(f"Generating Safe FileNames...")
#     for internal_doc in file_handler.list_files(rfd_path):
#         # Replace all the special chars in the file name with '_'
#         safe_doc_name = "".join(
#             c if c.isalnum() or c in (".", "_") else "_" for c in internal_doc
#         )
#         if internal_doc != safe_doc_name:
#             logger.info(f"Renaming file {internal_doc} to {safe_doc_name}")
#             file_handler.move_file(
#                 Path(rfd_path) / internal_doc, Path(rfd_path) / safe_doc_name
#             )
#             safe_file_name_cnt += 1
#     return safe_file_name_cnt

# def generate_safe_filename_rfd(rfd_path: str, internal_doc):
#     safe_doc_name = "".join(
#         c if c.isalnum() or c in (".", "_") else "_" for c in internal_doc
#     )
#     # if internal_doc != safe_doc_name:
#     #     logger.info(f"Renaming file {internal_doc} to {safe_doc_name}")
#     #     file_handler.move_file(
#     #         Path(rfd_path) / internal_doc, Path(rfd_path) / safe_doc_name
#     #     )
    
#     return safe_doc_name


def convert_rfd_to_html(
    rfd_path: str,
    rfd_interim_path: str,
    failed_ingestion_path: str,
    input_doc_type: str = "docx",
    output_doc_type: str = "html",
):
    pandoc_processor = PandocProcessor(pandoc_executable="pandoc")

    for internal_doc in file_handler.list_files(rfd_path):
        if internal_doc.endswith(".docx") and not internal_doc.startswith("~$"):
            logger.info(f"Started file format conversion for doc: {internal_doc}")

            input_doc_path = Path(rfd_path) / internal_doc

            # Create output directory for this docx
            rfd_dir_name = internal_doc.replace(".docx", "")
            rfd_output_dir = Path(rfd_interim_path) / rfd_dir_name
            rfd_output_dir.mkdir(parents=True, exist_ok=True)
            output_doc_path = Path(rfd_output_dir) / internal_doc.replace(
                f".{input_doc_type}", f".{output_doc_type}"
            )
            media_dir = Path(rfd_output_dir)
            logger.info(f"Converting {input_doc_path} to {output_doc_path}")

            pandoc_processor.convert(
                input_path=input_doc_path,
                output_path=output_doc_path,
                input_format=input_doc_type,
                output_format=output_doc_type,
                failed_ingestion_path=failed_ingestion_path,
                extract_media_dir=media_dir,
            )


def extract_tables_from_rfd_html(
    rfd_interim_path: str,
    rfd_metadata_path: str,
    rfd_embeddings_path: str,
):
    for rfd_html_dir in os.listdir(rfd_interim_path):
        logger.info(f"Processing RFD HTML file: {rfd_html_dir}")
        rfd_html_dir_path = Path(rfd_interim_path) / rfd_html_dir
        rfd_html_file_path = rfd_html_dir_path / (rfd_html_dir + ".html")
        rfd_html_file_name = rfd_html_dir + ".html"
        if os.path.exists(rfd_html_file_path):
            logger.info(f"HTML file found: {rfd_html_file_name}. Extracting Tables...")

            # Read the HTML content
            html_content = file_handler.read_file(rfd_html_file_path)

            # Process tables in RFD HTML
            html_with_flat_tables, table_details = process_tables(
                html_str=html_content,
                source_filename=rfd_html_file_name,
                output_tables_path=rfd_html_dir_path,
                article_metadata_path=rfd_metadata_path,
                table_state="remove",
            )

            # Make sure rfd_embeddings_path exists
            Path(rfd_embeddings_path).mkdir(parents=True, exist_ok=True)
            # Save table details as JSON
            logger.info(f"Saving extracted table details to JSON...")
            file_path = f"{rfd_embeddings_path}/{rfd_html_dir}_tables.json"
            file_handler.write_file_as_json(file_path=file_path, content=table_details)
            logger.info(f"Table details saved to {file_path}")

            # Write back modified HTML with flat table text
            file_handler.write_file(rfd_html_file_path, html_with_flat_tables)
            logger.info(
                f"Extracted {len(table_details)} tables from {rfd_html_file_name}"
            )
