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
    def __init__(
        self, params: Dict[str, Any], collection_type: str = "processed_pubmedbert"
    ):
        self.url = params["url"]
        self.collection_type = collection_type
        if self.collection_type == "processed_pubmedbert":
            self.collection_name = params["collections"]["processed_pubmedbert"][
                "collection_name"
            ]
            self.vector_size = params["collections"]["processed_pubmedbert"][
                "vector_size"
            ]
            self.distance_metric = params["collections"]["processed_pubmedbert"][
                "distance_metric"
            ]
        elif self.collection_type == "processed_medembed":
            self.collection_name = params["collections"]["processed_medembed"][
                "collection_name"
            ]
            self.vector_size = params["collections"]["processed_medembed"][
                "vector_size"
            ]
            self.distance_metric = params["collections"]["processed_medembed"][
                "distance_metric"
            ]
        elif self.collection_type == "metadata":
            self.collection_name = params["collections"]["metadata"]["collection_name"]
            self.vector_size = params["collections"]["metadata"]["vector_size"]
            self.distance_metric = params["collections"]["metadata"]["distance_metric"]
        elif self.collection_type == "baseline":
            self.collection_name = params["collections"]["baseline"]["collection_name"]
            self.vector_size = params["collections"]["baseline"]["vector_size"]
            self.distance_metric = params["collections"]["baseline"]["distance_metric"]
        else:
            self.collection_name = params["collections"]["test"]["collection_name"]
            self.vector_size = params["collections"]["test"]["vector_size"]
            self.distance_metric = params["collections"]["test"]["distance_metric"]

    def get_qdrant_manager(self) -> QdrantManager:
        """Creates a QdrantManager instance using the configuration."""
        try:
            qdrant_manager = QdrantManager(
                url=self.url,
                collection_name=self.collection_name,
                vector_size=self.vector_size,
                distance_metric=self.distance_metric,
            )

            # Creating Qdrant Collection if not already exists:
            if not qdrant_manager.check_if_collection_exists():
                logger.info(f"Creating New Collection {qdrant_manager.collection_name}")
                qdrant_manager.create_collection()
            else:
                logger.info(
                    f"Collection {qdrant_manager.collection_name} already exists"
                )

            return qdrant_manager
        except KeyError as e:
            raise ValueError(f"Configuration missing for: {e}")
