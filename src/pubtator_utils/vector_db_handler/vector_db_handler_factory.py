from typing import Any, Dict

from dotenv import load_dotenv

from src.pubtator_utils.vector_db_handler.opensearch_manager import OpenSearchManager
from src.pubtator_utils.vector_db_handler.qdrant_manager import QdrantManager
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

load_dotenv()  # Load environment variables from .env file


class VectorDBHandler:
    def __init__(self, vector_db_params: Dict[str, Any], index_params: Dict[str, Any]):
        self.vector_db_params = vector_db_params
        self.index_params = index_params

    def get_vector_db_manager(self, vector_db_type):
        """Factory method to return the appropriate vector db handler based on the vector_db_type."""

        if vector_db_type == "qdrant_cloud" or vector_db_type == "qdrant":
            qdrant_manager = QdrantManager(self.vector_db_params, self.index_params)

            # Creating Qdrant Collection if not already exists:
            if not qdrant_manager.check_if_index_exists():
                logger.info(f"Creating New Collection {qdrant_manager.collection_name}")
                qdrant_manager.create_index()
            else:
                logger.info(
                    f"Collection {qdrant_manager.collection_name} already exists"
                )

            return qdrant_manager
        elif vector_db_type == "opensearch":
            opensearch_manager = OpenSearchManager(
                self.vector_db_params, self.index_params
            )

            # Creating Qdrant Collection if not already exists:
            if not opensearch_manager.check_if_index_exists():
                logger.info(f"Creating New Index {opensearch_manager.index_name}")
                opensearch_manager.create_index()
            else:
                logger.info(
                    f"Collection {opensearch_manager.index_name} already exists"
                )

            return opensearch_manager
        else:
            raise ValueError(f"Unknown vector DB type: {vector_db_type}")
