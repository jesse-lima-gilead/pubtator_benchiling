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


def upload_apollo_articles(
    apollo_path: str,
    s3_apollo_path: str,
    bioc_path: str,
    s3_bioc_path: str,
    interim_path: str,
    s3_interim_path: str,
    summary_path: str,
    s3_summary_path: str,
    embeddings_path: str,
    s3_embeddings_path: str,
    failed_ingestion_path: str,
    s3_failed_ingestion_path: str,
    file_handler: FileHandler,
    s3_file_handler: FileHandler,
):
    file_upload_counter = 0

    # Upload the Apollo Docs Files to S3
    logger.info(f"Uploading Apollo Docs Ingestion Files to S3")
    apollo_docs_upload_counter = 0
    for apollo_docx in file_handler.list_files(apollo_path):
        local_file_path = file_handler.get_file_path(apollo_path, apollo_docx)
        s3_file_path = s3_file_handler.get_file_path(s3_apollo_path, apollo_docx)
        logger.info(f"Uploading file {apollo_docx} to S3 path {s3_file_path}")
        upload_to_s3(
            local_path=local_file_path,
            s3_path=s3_file_path,
            s3_file_handler=s3_file_handler,
        )
        apollo_docs_upload_counter += 1
    logger.info(f"Total Apollo Docs Files uploaded to S3: {apollo_docs_upload_counter}")
    file_upload_counter += apollo_docs_upload_counter

    # Upload the Apollo Docs Failed Files to S3
    logger.info(f"Uploading Apollo Docs Failed Ingestion Files to S3")
    apollo_failed_docx_upload_counter = 0
    for apollo_docs_failed in file_handler.list_files(failed_ingestion_path):
        local_file_path = file_handler.get_file_path(
            failed_ingestion_path, apollo_docs_failed
        )
        s3_file_path = s3_file_handler.get_file_path(
            s3_failed_ingestion_path, apollo_docs_failed
        )
        logger.info(f"Uploading file {apollo_docs_failed} to S3 path {s3_file_path}")
        upload_to_s3(
            local_path=local_file_path,
            s3_path=s3_file_path,
            s3_file_handler=s3_file_handler,
        )
        apollo_failed_docx_upload_counter += 1
    logger.info(
        f"Total Apollo Docs Ingestion Failed Files uploaded to S3: {apollo_failed_docx_upload_counter}"
    )
    file_upload_counter += apollo_failed_docx_upload_counter

    # # Upload the Summary Files to S3
    # logger.info(f"Uploading Apollo Summary Files to S3")
    # apollo_summary_upload_counter = 0
    # for apollo_summary_file in file_handler.list_files(summary_path):
    #     if apollo_summary_file.endswith(".txt"):
    #         local_file_path = file_handler.get_file_path(
    #             summary_path, apollo_summary_file
    #         )
    #         s3_file_path = s3_file_handler.get_file_path(
    #             s3_summary_path, apollo_summary_file
    #         )
    #         logger.info(
    #             f"Uploading file {apollo_summary_file} to S3 path {s3_file_path}"
    #         )
    #         upload_to_s3(
    #             local_path=local_file_path,
    #             s3_path=s3_file_path,
    #             s3_file_handler=s3_file_handler,
    #         )
    #         apollo_summary_upload_counter += 1
    #     else:
    #         logger.warning(f"Skipping file: {apollo_summary_file} for S3 upload")
    # logger.info(
    #     f"Total Apollo Summary Files uploaded to S3: {apollo_summary_upload_counter}"
    # )
    # file_upload_counter += apollo_summary_upload_counter

    # Upload the BioC XML Files to S3
    logger.info(f"Uploading Apollo BioC XML Files to S3")
    apollo_bioc_upload_counter = 0
    for apollo_bioc_xml in file_handler.list_files(bioc_path):
        if apollo_bioc_xml.endswith(".xml"):
            local_file_path = file_handler.get_file_path(bioc_path, apollo_bioc_xml)
            s3_file_path = s3_file_handler.get_file_path(s3_bioc_path, apollo_bioc_xml)
            logger.info(f"Uploading file {apollo_bioc_xml} to S3 path {s3_file_path}")
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            apollo_bioc_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {apollo_bioc_xml} for S3 upload")
    logger.info(
        f"Total Apollo BioC XML Files uploaded to S3: {apollo_bioc_upload_counter}"
    )
    file_upload_counter += apollo_bioc_upload_counter

    # Upload the Interim HTML Files to S3
    logger.info(f"Uploading Apollo Interim Files to S3")
    apollo_interim_file_upload_counter = 0
    for apollo_dir in os.listdir(interim_path):
        logger.info(f"Processing apollo dir: {apollo_dir}")
        apollo_dir_path = Path(interim_path) / apollo_dir
        for apollo_interim_file in os.listdir(apollo_dir_path):
            apollo_interim_file_path = file_handler.get_file_path(
                apollo_dir_path, apollo_interim_file
            )

            # Uploading the Tables XLSX, Article HTML, TOC Removed Passages and Table Extraction Summary
            if os.path.isfile(
                apollo_interim_file_path
            ) and not apollo_interim_file.startswith("~$"):
                s3_path = str(apollo_dir) + "/" + str(apollo_interim_file)
                s3_file_path = s3_file_handler.get_file_path(s3_interim_path, s3_path)
                logger.info(
                    f"Uploading file {apollo_interim_file_path} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=apollo_interim_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                apollo_interim_file_upload_counter += 1

            # Uploading the images in the media folder for docx
            elif os.path.isdir(apollo_interim_file_path):
                for image_file in file_handler.list_files(apollo_interim_file_path):
                    image_file_path = file_handler.get_file_path(
                        apollo_interim_file_path, image_file
                    )
                    if os.path.isfile(image_file_path) and not image_file.startswith(
                        "~$"
                    ):
                        s3_path = (
                            str(apollo_dir)
                            + "/"
                            + str(apollo_interim_file)
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
                        apollo_interim_file_upload_counter += 1
                    else:
                        logger.warning(f"Skipping file: {image_file} for S3 upload")
            else:
                logger.warning(f"Skipping file: {apollo_interim_file} for S3 upload")

    logger.info(
        f"Total Apollo Interim Files uploaded to S3: {apollo_interim_file_upload_counter}"
    )
    file_upload_counter += apollo_interim_file_upload_counter

    # Upload the Embeddings Files to S3
    logger.info(f"Uploading Apollo Embeddings Files to S3")
    apollo_embeddings_upload_counter = 0
    for apollo_embedding_file in file_handler.list_files(embeddings_path):
        if apollo_embedding_file.endswith(".json"):
            local_file_path = file_handler.get_file_path(
                embeddings_path, apollo_embedding_file
            )
            s3_file_path = s3_file_handler.get_file_path(
                s3_embeddings_path, apollo_embedding_file
            )
            logger.info(
                f"Uploading file {apollo_embedding_file} to S3 path {s3_file_path}"
            )
            upload_to_s3(
                local_path=local_file_path,
                s3_path=s3_file_path,
                s3_file_handler=s3_file_handler,
            )
            apollo_embeddings_upload_counter += 1
        else:
            logger.warning(f"Skipping file: {apollo_embedding_file} for S3 upload")
    logger.info(
        f"Total Apollo Embeddings Files uploaded to S3: {apollo_embeddings_upload_counter}"
    )
    file_upload_counter += apollo_embeddings_upload_counter

    return file_upload_counter
