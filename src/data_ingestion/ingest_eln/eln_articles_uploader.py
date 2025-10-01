import os
from pathlib import Path

from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_uploader import upload_to_s3

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def upload_eln_articles(
    eln_path: str,
    s3_eln_path: str,
    bioc_path: str,
    s3_bioc_path: str,
    interim_path: str,
    s3_interim_path: str,
    metadata_path: str,
    s3_article_metadata_path: str,
    summary_path: str,
    s3_summary_path: str,
    embeddings_path: str,
    s3_embeddings_path: str,
    file_handler: FileHandler,
    s3_file_handler: FileHandler,
):
    file_upload_counter = 0

    # Re-Write the Original eln Files with Safe File Names to S3
    logger.info(f"Uploading eln Files with Safe File Names to S3")
    eln_doc_upload_counter = 0
    for eln_doc in file_handler.list_files(eln_path):
        if eln_doc.endswith(".json") and not eln_doc.startswith("~$"):
            local_file_path = file_handler.get_file_path(eln_path, eln_doc)
            s3_file_path = file_handler.get_file_path(s3_eln_path, eln_doc)
            logger.info(f"Uploading file {local_file_path} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                file_handler=s3_file_handler,
            )
            eln_doc_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {eln_doc} for S3 upload")
    logger.info(f"Total eln Files uploaded to S3: {eln_doc_upload_counter}")
    file_upload_counter += eln_doc_upload_counter

    # Upload the BioC XML Files to S3
    logger.info(f"Uploading BioC XML Files to S3")
    eln_bioc_upload_counter = 0
    for eln_bioc_xml in file_handler.list_files(bioc_path):
        if eln_bioc_xml.endswith(".xml"):
            local_file_path = file_handler.get_file_path(bioc_path, eln_bioc_xml)
            s3_file_path = file_handler.get_file_path(s3_bioc_path, eln_bioc_xml)
            logger.info(f"Uploading file {eln_bioc_xml} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                file_handler=s3_file_handler,
            )
            eln_bioc_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {eln_bioc_xml} for S3 upload")
    logger.info(f"Total BioC XML Files uploaded to S3: {eln_bioc_upload_counter}")
    file_upload_counter += eln_bioc_upload_counter

    # Upload the Interim HTML Files to S3
    logger.info(f"Uploading Interim Files to S3")
    eln_interim_file_upload_counter = 0
    for eln_dir in file_handler.list_files(interim_path):
        logger.info(f"Processing eln dir: {eln_dir}")
        eln_dir_path = Path(interim_path) / eln_dir
        for eln_interim_file in file_handler.list_files(eln_dir_path):
            eln_interim_file_path = file_handler.get_file_path(
                eln_dir_path, eln_interim_file
            )

            # Uploading the Tables XLSX, Article HTML, TOC Removed Passages and Table Extraction Summary
            if os.path.isfile(
                eln_interim_file_path
            ) and not eln_interim_file.startswith("~$"):
                s3_file_path = file_handler.get_file_path(
                    s3_interim_path, eln_dir_path, eln_interim_file_path
                )
                logger.info(
                    f"Uploading file {eln_interim_file_path} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    file_handler=s3_file_handler,
                )
                eln_interim_file_upload_counter += 1

            # Uploading the images in the media folder
            elif os.path.isdir(eln_interim_file_path):
                for image_file in file_handler.list_files(eln_interim_file_path):
                    image_file_path = file_handler.get_file_path(
                        eln_interim_file_path, image_file
                    )
                    if os.path.isfile(image_file_path) and not image_file.startswith(
                        "~$"
                    ):
                        s3_file_path = file_handler.get_file_path(
                            s3_interim_path,
                            eln_dir_path,
                            eln_interim_file_path,
                            image_file,
                        )
                        logger.info(
                            f"Uploading file {image_file_path} to S3 path {s3_file_path}"
                        )
                        upload_to_s3(
                            local_path=image_file_path,
                            s3_path=s3_file_path,
                            file_handler=s3_file_handler,
                        )
                        eln_interim_file_upload_counter += 1
                    else:
                        logger.warning(f"Skipping file: {image_file} for S3 upload")
            else:
                logger.warning(f"Skipping file: {eln_interim_file} for S3 upload")
    logger.info(
        f"Total Interim Files uploaded to S3: {eln_interim_file_upload_counter}"
    )
    file_upload_counter += eln_interim_file_upload_counter

    # Upload the Article Metadata JSON Files to S3
    eln_metadata_file_upload_counter = 0
    logger.info(f"Uploading Article Metadata JSON Files to S3")
    for eln_metadata_json in file_handler.list_files(metadata_path):
        if eln_metadata_json.endswith(".json"):
            local_file_path = file_handler.get_file_path(
                metadata_path, eln_metadata_json
            )
            s3_file_path = file_handler.get_file_path(
                s3_article_metadata_path, eln_metadata_json
            )
            logger.info(f"Uploading file {eln_metadata_json} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                file_handler=s3_file_handler,
            )
            eln_metadata_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {eln_metadata_json} for S3 upload")
    logger.info(
        f"Total Article Metadata JSON Files uploaded to S3: {eln_metadata_file_upload_counter}"
    )
    file_upload_counter += eln_metadata_file_upload_counter

    # Upload the Summary Files to S3
    logger.info(f"Uploading Summary Files to S3")
    eln_summary_file_upload_counter = 0
    for eln_summary in file_handler.list_files(summary_path):
        if eln_summary.endswith(".txt"):
            local_file_path = file_handler.get_file_path(summary_path, eln_summary)
            s3_file_path = file_handler.get_file_path(s3_summary_path, eln_summary)
            logger.info(f"Uploading file {eln_summary} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                file_handler=s3_file_handler,
            )
            eln_summary_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {eln_summary} for S3 upload")
    logger.info(
        f"Total Summary Files uploaded to S3: {eln_summary_file_upload_counter}"
    )
    file_upload_counter += eln_summary_file_upload_counter

    # Upload the Embeddings JSON Files to S3
    logger.info(f"Uploading Table Embeddings to S3")
    eln_tables_file_upload_counter = 0
    for eln_tables in file_handler.list_files(embeddings_path):
        if eln_tables.endswith(".json"):
            local_file_path = file_handler.get_file_path(embeddings_path, eln_tables)
            s3_file_path = file_handler.get_file_path(s3_embeddings_path, eln_tables)
            logger.info(f"Uploading file {eln_tables} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                file_handler=s3_file_handler,
            )
            eln_tables_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {eln_tables} for S3 upload")
    logger.info(f"Total Table Files uploaded to S3: {eln_tables_file_upload_counter}")
    file_upload_counter += eln_tables_file_upload_counter

    return file_upload_counter
