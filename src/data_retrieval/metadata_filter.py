from typing import List, Dict
import json
from src.pubtator_utils.vector_db_handler.vector_db_handler_factory import (
    VectorDBHandler,
)
from src.pubtator_utils.prompts_handler.PromptBuilder import PromptBuilder
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
)
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.llm_handler.llm_factory import LLMFactory
from src.data_retrieval.retriever_utils import (
    initialize_qdrant_manager,
    get_user_query_embeddings,
)


# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class MetadataFilter:
    def __init__(
        self,
        metadata_collection_type: str,
    ):
        self.metadata_qdrant_manager = initialize_qdrant_manager(
            collection_type=metadata_collection_type
        )

    def get_article_ids_filtered_by_metadata(
        self, payload_filter: Dict[str, str]
    ) -> List[str]:
        # Fetch points matching the payload filter from the metadata collection
        matching_points = self.metadata_qdrant_manager.fetch_points_by_payload(
            payload_filter, limit=5000
        )

        # Extract IDs of articles that meet the metadata filter criteria
        matching_article_ids = {point["payload"]["pmcid"] for point in matching_points}
        # print(f"Matching article ids: {matching_article_ids}")

        return matching_article_ids
