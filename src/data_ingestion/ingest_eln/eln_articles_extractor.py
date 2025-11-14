from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3_eln

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


def extract_eln_articles(
    eln_path: str,
    file_handler: FileHandler,
    eln_source_config: dict,
    source: str,
    file_type: str = "json",
):
    source_type = eln_source_config["type"]

    if source_type == "s3":
        s3_src_path = eln_source_config["s3_src_path"]
        # call the S3 extractor
        extracted_files_to_grsar_id_map = extract_from_s3_eln(
            eln_path, file_handler, source, source_type, s3_src_path, file_type
        )
        return extracted_files_to_grsar_id_map
    elif source_type == "API":
        pass
    else:
        raise ValueError(f"Unsupported Source type: {source}")
