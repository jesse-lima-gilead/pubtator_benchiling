import csv
import re
from collections import defaultdict
from typing import List, Dict
import json
from src.pubtator_utils.vector_db_handler.qdrant_handler import QdrantHandler
from src.pubtator_utils.prompts_handler.PromptBuilder import PromptBuilder
from src.data_processing.embedding.embeddings_handler import get_embeddings
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.llm_handler.llm_factory import LLMFactory

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def initialize_qdrant_manager(collection_type: str):
    # Initialize the QdrantHandler
    logger.info(f"Initializing Qdrant manager for collection type: {collection_type}")
    qdrant_handler = QdrantHandler(
        collection_type=collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


def get_user_query_embeddings(embeddings_model, user_query: str):
    # Get embeddings for the user query
    query_vector = get_embeddings(model_name=embeddings_model, texts=[user_query])
    return query_vector.squeeze(0).tolist()


def parse_results(user_query, result):
    parsed_output = []
    for article_id, points in result.items():
        # parsed_output[article_id] = []

        for point in points:
            payload = point.payload  # Access as attribute instead of dict key

            # Extract the desired fields from the payload
            entry = {
                "user_query": user_query,
                "article_id": article_id,
                "chunk_id": payload["chunk_id"],
                "chunk_text": payload["chunk_text"],
                "merged_text": payload["merged_text"],
                "chunk_score": point.score,  # Access score as attribute
            }
            parsed_output.append(entry)
    return parsed_output


if __name__ == "__main__":
    embeddings_model = "pubmedbert"
    user_query = "lung cancer risk from air pollution"
    query_embeddings = get_embeddings(model_name=embeddings_model, texts=[user_query])
    print(query_embeddings)
