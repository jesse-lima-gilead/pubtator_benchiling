import logging
from typing import Any, Dict

from src.vector_db_handler.qdrant_manager import QdrantManager
from src.utils.config_reader import YAMLConfigLoader
from src.utils.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class QdrantHandler:
    def __init__(self,):
        self.params = params

    def get_qdrant_manager(self) -> QdrantManager:
        """Creates a QdrantManager instance using the configuration."""
        try:
            qdrant_config = vectordb_config["text"]
            qdrant_manager = QdrantManager(
                host=qdrant_config["host"],
                port=qdrant_config["port"],
                collection_name=params["collection_name"],
                vector_size=params["vector_size"],
                distance_metric=params["distance_metric"],
            )

            # Creating Qdrant Collection if not already exists:
            if not qdrant_manager.check_if_collection_exists():
                logger.info(f"Creating New Collection {qdrant_manager.collection_name}")
                qdrant_manager.create_collection()
            else:
                logger.info(f"Collection {qdrant_manager.collection_name} already exists")

            return qdrant_manager
        except KeyError as e:
            raise ValueError(f"Configuration missing for: {e}")

