from typing import Any, Dict

from src.pubtator_utils.vector_db_handler.opensearch_manager import OpenSearchManager
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class VectorDBHandler:
    def __init__(self, vector_db_params: Dict[str, Any], index_params: Dict[str, Any]):
        self.vector_db_params = vector_db_params
        self.index_params = index_params

    def get_vector_db_manager(self, vector_db_type):
        """Factory method to return the appropriate vector db handler based on the vector_db_type."""

        if vector_db_type == "opensearch_cloud":
            opensearch_manager = OpenSearchManager(
                self.vector_db_params, self.index_params
            )

            # Creating Opensearch Index if not already exists:
            if not opensearch_manager.check_if_index_exists():
                logger.error(f"Index {opensearch_manager.index_name} doesn't exist")
                raise Exception(
                    f"Index {opensearch_manager.index_name} is not present in the cluster"
                )
            else:
                logger.info(
                    f"Collection {opensearch_manager.index_name} already exists"
                )

            return opensearch_manager
        else:
            raise ValueError(f"Unknown vector DB type: {vector_db_type}")
