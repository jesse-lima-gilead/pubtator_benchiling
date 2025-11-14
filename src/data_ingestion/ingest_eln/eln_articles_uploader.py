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
    failed_ingestion_path: str,
    s3_failed_ingestion_path: str,
    chunks_path: str,
    s3_chunks_path: str,
    file_handler: FileHandler,
    s3_file_handler: FileHandler,
):
    file_upload_counter = 0

    if file_handler.exists(eln_path):
        # Write the Original eln Files to S3 data ingestion path
        logger.info(f"Uploading eln Files with Safe File Names to S3")
        eln_doc_upload_counter = 0
        for eln_doc in file_handler.list_files(eln_path):
            if eln_doc.endswith(".json") and not eln_doc.startswith("~$"):
                local_file_path = file_handler.get_file_path(eln_path, eln_doc)
                s3_file_path = s3_file_handler.get_file_path(s3_eln_path, eln_doc)
                logger.info(
                    f"Uploading file {local_file_path} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                eln_doc_upload_counter += 1
            else:
                logger.warning(f"Skipping file: {eln_doc} for S3 upload")
        logger.info(f"Total eln Files uploaded to S3: {eln_doc_upload_counter}")
        file_upload_counter += eln_doc_upload_counter

    if file_handler.exists(bioc_path):
        # Upload the BioC XML Files to S3
        logger.info(f"Uploading BioC XML Files to S3")
        eln_bioc_upload_counter = 0
        for eln_bioc_xml in file_handler.list_files(bioc_path):
            if eln_bioc_xml.endswith(".xml"):
                local_file_path = file_handler.get_file_path(bioc_path, eln_bioc_xml)
                s3_file_path = s3_file_handler.get_file_path(s3_bioc_path, eln_bioc_xml)
                logger.info(f"Uploading file {eln_bioc_xml} to S3 path {s3_file_path}")
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                eln_bioc_upload_counter += 1
            else:
                logger.warning(f"Skipping file: {eln_bioc_xml} for S3 upload")
        logger.info(f"Total BioC XML Files uploaded to S3: {eln_bioc_upload_counter}")
        file_upload_counter += eln_bioc_upload_counter

    if file_handler.exists(interim_path):
        # Upload the Interim Files to S3
        logger.info(f"Uploading ELN Interim Files to S3")
        eln_interim_file_upload_counter = 0
        for eln_interim_file in file_handler.list_files(interim_path):
            local_file_path = file_handler.get_file_path(interim_path, eln_interim_file)
            s3_file_path = s3_file_handler.get_file_path(
                s3_interim_path, eln_interim_file
            )
            logger.info(f"Uploading file {eln_interim_file} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            eln_interim_file_upload_counter += 1
        logger.info(
            f"Total Interim Files uploaded to S3: {eln_interim_file_upload_counter}"
        )
        file_upload_counter += eln_interim_file_upload_counter

    if file_handler.exists(metadata_path):
        # Upload the Article Metadata JSON Files to S3
        eln_metadata_file_upload_counter = 0
        logger.info(f"Uploading Article Metadata JSON Files to S3")
        for eln_metadata_json in file_handler.list_files(metadata_path):
            if eln_metadata_json.endswith(".json"):
                local_file_path = file_handler.get_file_path(
                    metadata_path, eln_metadata_json
                )
                s3_file_path = s3_file_handler.get_file_path(
                    s3_article_metadata_path, eln_metadata_json
                )
                logger.info(
                    f"Uploading file {eln_metadata_json} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                eln_metadata_file_upload_counter += 1
            else:
                logger.warning(f"Skipping file: {eln_metadata_json} for S3 upload")
        logger.info(
            f"Total Article Metadata JSON Files uploaded to S3: {eln_metadata_file_upload_counter}"
        )
        file_upload_counter += eln_metadata_file_upload_counter

    if file_handler.exists(failed_ingestion_path):
        # Upload the Failed ELN Files to S3
        logger.info(f"Uploading Failed ELN Files to S3")
        eln_summary_file_upload_counter = 0
        for eln_failed in file_handler.list_files(failed_ingestion_path):
            local_file_path = file_handler.get_file_path(
                failed_ingestion_path, eln_failed
            )
            s3_file_path = s3_file_handler.get_file_path(
                s3_failed_ingestion_path, eln_failed
            )
            logger.info(f"Uploading file {eln_failed} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            eln_summary_file_upload_counter += 1
        logger.info(
            f"Total Summary Files uploaded to S3: {eln_summary_file_upload_counter}"
        )
        file_upload_counter += eln_summary_file_upload_counter

    if file_handler.exists(chunks_path):
        # Upload the Chunks JSON Files to S3
        logger.info(f"Uploading Smiles Chunks to S3")
        eln_chunks_file_upload_counter = 0
        for eln_chunks in file_handler.list_files(chunks_path):
            if eln_chunks.endswith(".json"):
                local_file_path = file_handler.get_file_path(chunks_path, eln_chunks)
                s3_file_path = s3_file_handler.get_file_path(s3_chunks_path, eln_chunks)
                logger.info(f"Uploading file {eln_chunks} to S3 path {s3_file_path}")
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                eln_chunks_file_upload_counter += 1
            else:
                logger.warning(f"Skipping file: {eln_chunks} for S3 upload")
        logger.info(
            f"Total ELN Chunk Files uploaded to S3: {eln_chunks_file_upload_counter}"
        )
        file_upload_counter += eln_chunks_file_upload_counter

    return file_upload_counter
