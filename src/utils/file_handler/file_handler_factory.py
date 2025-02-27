from src.utils.file_handler.base_handler import FileHandler
from src.utils.file_handler.local_handler import LocalFileHandler
from src.utils.file_handler.s3_handler import S3FileHandler


class FileHandlerFactory:
    """Factory class to create and return the appropriate file handler based on storage type."""

    _handlers = {"local": LocalFileHandler, "s3": S3FileHandler}

    @staticmethod
    def get_handler(storage_type: str) -> FileHandler:
        """Returns the appropriate file handler instance.

        Args:
            storage_type (str): The type of storage ("local", "s3", etc.)

        Returns:
            FileHandler: An instance of the corresponding file handler.

        Raises:
            ValueError: If an unsupported storage type is provided.
        """
        if storage_type not in FileHandlerFactory._handlers:
            raise ValueError(f"Unsupported storage type: {storage_type}")

        return FileHandlerFactory._handlers[storage_type]()  # Instantiate and return
