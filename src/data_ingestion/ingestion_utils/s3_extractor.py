from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_from_s3(
    path: str, file_handler: FileHandler, source: str, storage_type: str = "s3"
):
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")

    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    s3_paths = paths_config["storage"][storage_type]
    # Source S3 data path
    src_data_path = s3_paths["ingestion_path"].replace("{source}", source)

    src_files = s3_file_handler.list_files(src_data_path)

    for cur_src_file in src_files:
        # path of the source s3 key
        cur_s3_full_path = s3_file_handler.get_file_path(src_data_path, cur_src_file)
        # path where the files are going to be written to in the ingestion directory of HPC
        cur_staging_path = file_handler.get_file_path(path, cur_src_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_s3_full_path, cur_staging_path)

        logger.info(
            f"File downloaded from S3: {cur_s3_full_path} to local: {cur_staging_path}"
        )

    ingested_articles_cnt = len(src_files)

    return ingested_articles_cnt
