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


def upload_rfd_articles(
    rfd_path: str,
    s3_rfd_path: str,
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

    # Write the Original RFD Files with Safe File Names to S3 data ingestion Path
    logger.info(f"Uploading RFD Files with Safe File Names to S3")
    rfd_doc_upload_counter = 0
    for rfd_doc in file_handler.list_files(rfd_path):
        if rfd_doc.endswith(".docx") and not rfd_doc.startswith("~$"):
            local_file_path = file_handler.get_file_path(rfd_path, rfd_doc)
            s3_file_path = s3_file_handler.get_file_path(s3_rfd_path, rfd_doc)
            logger.info(f"Uploading file {local_file_path} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            rfd_doc_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {rfd_doc} for S3 upload")
    logger.info(f"Total RFD Files uploaded to S3: {rfd_doc_upload_counter}")
    file_upload_counter += rfd_doc_upload_counter

    # Upload the BioC XML Files to S3
    logger.info(f"Uploading BioC XML Files to S3")
    rfd_bioc_upload_counter = 0
    for rfd_bioc_xml in file_handler.list_files(bioc_path):
        if rfd_bioc_xml.endswith(".xml"):
            local_file_path = file_handler.get_file_path(bioc_path, rfd_bioc_xml)
            s3_file_path = file_handler.get_file_path(s3_bioc_path, rfd_bioc_xml)
            logger.info(f"Uploading file {rfd_bioc_xml} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            rfd_bioc_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {rfd_bioc_xml} for S3 upload")
    logger.info(f"Total BioC XML Files uploaded to S3: {rfd_bioc_upload_counter}")
    file_upload_counter += rfd_bioc_upload_counter

    # Upload the Interim HTML Files to S3
    logger.info(f"Uploading Interim Files to S3")
    rfd_interim_file_upload_counter = 0
    for rfd_dir in os.listdir(interim_path):
        logger.info(f"Processing rfd dir: {rfd_dir}")
        rfd_dir_path = Path(interim_path) / rfd_dir
        for rfd_interim_file in os.listdir(rfd_dir_path):
            rfd_interim_file_path = file_handler.get_file_path(
                rfd_dir_path, rfd_interim_file
            )

            # Uploading the Tables XLSX, Article HTML, TOC Removed Passages and Table Extraction Summary
            if os.path.isfile(
                rfd_interim_file_path
            ) and not rfd_interim_file.startswith("~$"):
                s3_path = str(rfd_dir) + "/" + str(rfd_interim_file)
                s3_file_path = s3_file_handler.get_file_path(s3_interim_path, s3_path)
                logger.info(
                    f"Uploading file {rfd_interim_file_path} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=rfd_interim_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                rfd_interim_file_upload_counter += 1

            # Uploading the images in the media folder
            elif os.path.isdir(rfd_interim_file_path):
                for image_file in file_handler.list_files(rfd_interim_file_path):
                    image_file_path = file_handler.get_file_path(
                        rfd_interim_file_path, image_file
                    )
                    if os.path.isfile(image_file_path) and not image_file.startswith(
                        "~$"
                    ):
                        s3_path = (
                            str(rfd_dir)
                            + "/"
                            + str(rfd_interim_file)
                            + "/"
                            + str(image_file)
                        )
                        s3_file_path = s3_file_handler.get_file_path(
                            s3_interim_path, s3_path
                        )
                        logger.info(
                            f"Uploading file {image_file_path} to S3 path {s3_file_path}"
                        )
                        upload_to_s3(
                            local_path=image_file_path,
                            s3_path=s3_file_path,
                            s3_file_handler=s3_file_handler,
                        )
                        rfd_interim_file_upload_counter += 1
                    else:
                        logger.warning(f"Skipping file: {image_file} for S3 upload")
            else:
                logger.warning(f"Skipping file: {rfd_interim_file} for S3 upload")

    logger.info(
        f"Total Interim Files uploaded to S3: {rfd_interim_file_upload_counter}"
    )
    file_upload_counter += rfd_interim_file_upload_counter

    # Upload the Article Metadata JSON Files to S3
    rfd_metadata_file_upload_counter = 0
    logger.info(f"Uploading Article Metadata JSON Files to S3")
    for rfd_metadata_json in file_handler.list_files(metadata_path):
        if rfd_metadata_json.endswith(".json"):
            local_file_path = file_handler.get_file_path(
                metadata_path, rfd_metadata_json
            )
            s3_file_path = file_handler.get_file_path(
                s3_article_metadata_path, rfd_metadata_json
            )
            logger.info(f"Uploading file {rfd_metadata_json} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            rfd_metadata_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {rfd_metadata_json} for S3 upload")
    logger.info(
        f"Total Article Metadata JSON Files uploaded to S3: {rfd_metadata_file_upload_counter}"
    )
    file_upload_counter += rfd_metadata_file_upload_counter

    # Upload the Summary Files to S3
    logger.info(f"Uploading Summary Files to S3")
    rfd_summary_file_upload_counter = 0
    for rfd_summary in file_handler.list_files(summary_path):
        if rfd_summary.endswith(".txt"):
            local_file_path = file_handler.get_file_path(summary_path, rfd_summary)
            s3_file_path = file_handler.get_file_path(s3_summary_path, rfd_summary)
            logger.info(f"Uploading file {rfd_summary} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            rfd_summary_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {rfd_summary} for S3 upload")
    logger.info(
        f"Total Summary Files uploaded to S3: {rfd_summary_file_upload_counter}"
    )
    file_upload_counter += rfd_summary_file_upload_counter

    # Upload the Embeddings JSON Files to S3
    logger.info(f"Uploading Table Embeddings to S3")
    rfd_tables_file_upload_counter = 0
    for rfd_tables in file_handler.list_files(embeddings_path):
        if rfd_tables.endswith(".json"):
            local_file_path = file_handler.get_file_path(embeddings_path, rfd_tables)
            s3_file_path = file_handler.get_file_path(s3_embeddings_path, rfd_tables)
            logger.info(f"Uploading file {rfd_tables} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            rfd_tables_file_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {rfd_tables} for S3 upload")
    logger.info(f"Total Table Files uploaded to S3: {rfd_tables_file_upload_counter}")
    file_upload_counter += rfd_tables_file_upload_counter

    return file_upload_counter
