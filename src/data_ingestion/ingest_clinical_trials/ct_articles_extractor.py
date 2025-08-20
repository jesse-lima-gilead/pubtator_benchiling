from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_from_s3(
    ct_path: str, file_handler: FileHandler, source: str, storage_type: str = "s3"
):
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")

    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    s3_paths = paths_config["storage"][storage_type]
    # Source S3 CT Trials data path
    ct_src_data_path = s3_paths["ingestion_path"].replace("{source}", source)

    ct_src_files = s3_file_handler.list_files(ct_src_data_path)

    for cur_ct_src_file in ct_src_files:
        # path of the source s3 key
        cur_ct_s3_full_path = s3_file_handler.get_file_path(
            ct_src_data_path, cur_ct_src_file
        )
        # path where the files are going to be written to in the ingestion directory of HPC
        cur_ct_staging_path = file_handler.get_file_path(ct_path, cur_ct_src_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_ct_s3_full_path, cur_ct_staging_path)

        logger.info(
            f"CT File downloaded from S3: {cur_ct_s3_full_path} to local: {cur_ct_staging_path}"
        )

    ingested_ct_articles_cnt = len(ct_src_files)

    return ingested_ct_articles_cnt


def extract_ct_articles(
    ct_path: str,
    file_handler: FileHandler,
    ct_source_config: dict,
    source: str,
):
    source_type = ct_source_config["type"]

    if source_type == "s3":
        # call the S3 extractor
        ingested_ct_articles_cnt = extract_from_s3(
            ct_path, file_handler, source, source_type
        )
        return ingested_ct_articles_cnt
    elif source_type == "API":
        pass
    else:
        raise ValueError(f"Unsupported Source type: {source}")
