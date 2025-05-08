from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler
from src.pubtator_utils.file_handler.s3_handler import S3FileHandler

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
aws_platform_type = config_loader.get_config("aws")["aws"]["platform_type"]


class FileHandlerFactory:
    """Factory class to create and return the appropriate file handler based on storage type."""

    _handlers = {"local": LocalFileHandler, "s3": S3FileHandler}

    @staticmethod
    def get_handler(storage_type: str, platform_type: str = None) -> FileHandler:
        """Returns the appropriate file handler instance.

        Args:
            storage_type (str): The type of storage ("local", "s3", etc.)
            platform_type (str): The type of platform (e.g. "HPC", "GDNA", etc.)
        Returns:
            FileHandler: An instance of the corresponding file handler.

        Raises:
            ValueError: If an unsupported storage type is provided.
        """
        if storage_type not in FileHandlerFactory._handlers:
            raise ValueError(f"Unsupported storage type: {storage_type}")

        if storage_type == "local":
            return FileHandlerFactory._handlers[storage_type]()
        else:
            if platform_type is None:
                platform_type = aws_platform_type

            if platform_type not in ["HPC", "GDNA"]:
                raise ValueError(f"Unsupported AWS platform type: {platform_type}")

            return FileHandlerFactory._handlers[storage_type](platform_type)
