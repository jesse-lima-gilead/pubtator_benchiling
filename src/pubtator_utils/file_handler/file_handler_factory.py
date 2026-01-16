from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

# Lazy import S3FileHandler to avoid import errors when not using S3
_s3_handler_class = None


def _get_s3_handler_class():
    """Lazy load S3FileHandler to avoid import errors when not using S3."""
    global _s3_handler_class
    if _s3_handler_class is None:
        from src.pubtator_utils.file_handler.s3_handler import S3FileHandler
        _s3_handler_class = S3FileHandler
    return _s3_handler_class


def _get_aws_platform_type():
    """Get AWS platform type from config, with fallback."""
    try:
        from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
        config_loader = YAMLConfigLoader()
        aws_config = config_loader.get_config("aws")
        if aws_config and "aws" in aws_config:
            return aws_config["aws"].get("platform_type", "HPC")
    except Exception:
        pass
    return "HPC"  # Default fallback


class FileHandlerFactory:
    """Factory class to create and return the appropriate file handler based on storage type."""

    @staticmethod
    def get_handler(storage_type: str, platform_type: str = None) -> FileHandler:
        """Returns the appropriate file handler instance.

        Args:
            storage_type (str): The type of storage ("local", "s3", "test", etc.)
            platform_type (str): The type of platform (e.g. "HPC", "GDNA", etc.)
        Returns:
            FileHandler: An instance of the corresponding file handler.

        Raises:
            ValueError: If an unsupported storage type is provided.
        """
        if storage_type in ("local", "test"):
            return LocalFileHandler()
        elif storage_type == "s3":
            if platform_type is None:
                platform_type = _get_aws_platform_type()

            if platform_type not in ["HPC", "GDNA"]:
                raise ValueError(f"Unsupported AWS platform type: {platform_type}")

            S3FileHandler = _get_s3_handler_class()
            return S3FileHandler(platform_type)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
