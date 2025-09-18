from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def upload_to_s3(local_path: str, s3_path: str, s3_file_handler: FileHandler):
    try:
        s3_file_handler.copy_file_local_to_s3(local_path=local_path, s3_path=s3_path)
        logger.info(f"File {local_path} uploaded to S3 at {s3_path}")
    except Exception as e:
        logger.error(
            f"Failed to upload file to S3: {s3_path} from local: {local_path}. Error: {e}"
        )
        raise e
