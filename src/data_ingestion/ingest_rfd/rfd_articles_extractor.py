from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3_rfd

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_rfd_articles(
    rfd_path: str,
    file_handler: FileHandler,
    rfd_source_config: dict,
    source: str,
):
    source_type = rfd_source_config["type"]

    if source_type == "s3":
        s3_src_path = rfd_source_config["s3_src_path"]
        # call the S3 extractor
        files_to_grsar_id_map = extract_from_s3_rfd(
            rfd_path, file_handler, source, source_type, s3_src_path
        )
        return files_to_grsar_id_map
    elif source_type == "API":
        pass
    else:
        raise ValueError(f"Unsupported Source type: {source}")
