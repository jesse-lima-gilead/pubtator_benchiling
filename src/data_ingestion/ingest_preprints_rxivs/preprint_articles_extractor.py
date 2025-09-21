from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_preprints_articles(
    preprints_path: str,
    file_handler: FileHandler,
    preprints_source_config: dict,
    source: str,
):
    source_type = preprints_source_config["type"]

    if source_type == "s3":
        # call the S3 extractor
        ingested_preprints_articles_cnt = extract_from_s3(
            preprints_path, file_handler, source, source_type
        )
        return ingested_preprints_articles_cnt
    elif source_type == "API":
        pass
    else:
        raise ValueError(f"Unsupported Source type: {source}")
