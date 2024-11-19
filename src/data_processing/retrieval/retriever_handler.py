from typing import List, Dict
import json
from src.vector_db_handler.qdrant_handler import QdrantHandler
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
)
from src.utils.config_reader import YAMLConfigLoader
from src.utils.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def initialize_article_qdrant_manager():
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(collection_type="pubmedbert", params=vectordb_config)
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


def initialize_metadata_qdrant_manager():
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(collection_type="metadata", params=vectordb_config)
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


class Retriever:
    def __init__(
        self,
        embeddings_model: str,
        embedding_collection: str,
        metadata_collection: str,
        top_k: int,
        top_n: int,
    ):
        self.article_qdrant_manager = initialize_article_qdrant_manager()
        self.metadata_qdrant_manager = initialize_metadata_qdrant_manager()
        self.embeddings_model = embeddings_model
        self.embedding_collection = embedding_collection
        self.metadata_collection = metadata_collection
        self.top_k = top_k
        self.top_n = top_n

    def get_user_query_embeddings(self, user_query: str):
        model_info = get_model_info(self.embeddings_model)
        # Get embeddings for the user query
        query_vector = get_embeddings(
            model_name=model_info[0], token_limit=model_info[1], texts=[user_query]
        )
        return query_vector

    def retrieve_chunks(self, query_vector):
        # Search across chunks, retrieve a larger set to ensure diversity
        retrieved_chunks = self.article_qdrant_manager.search_vectors(
            query_vector=query_vector,
            limit=50,  # Fetch a higher number to ensure we meet distinct article criteria
        )

        # Collect chunks by article_id and ensure we have chunks from at least N distinct articles
        chunks_by_article = {}
        for chunk in retrieved_chunks:
            article_id = chunk.payload["article_id"]
            if article_id not in chunks_by_article:
                chunks_by_article[article_id] = []
            if len(chunks_by_article[article_id]) < self.top_k:
                chunks_by_article[article_id].append(chunk)

            # Stop if we've accumulated at least N distinct articles with chunks
            if len(chunks_by_article) >= self.top_n:
                break

        return chunks_by_article

    def get_article_ids_filtered_by_metadata(
        self, payload_filter: Dict[str, str]
    ) -> List[str]:
        """
        Filter article IDs based on metadata criteria.

        Args:
            retrieved_article_ids (List[str]): List of article IDs from initial similarity-based retrieval.
            payload_filter (Dict[str, str]): Metadata filter criteria as key-value pairs.

        Returns:
            List[str]: List of article IDs that match the metadata filter criteria.
        """
        # Fetch points matching the payload filter from the metadata collection
        matching_points = self.metadata_qdrant_manager.fetch_points_by_payload(
            payload_filter, limit=5000
        )

        # Extract IDs of articles that meet the metadata filter criteria
        matching_article_ids = {point["payload"]["pmcid"] for point in matching_points}
        print(f"Matching article ids: {matching_article_ids}")

        return matching_article_ids

    def retrieve(self, query_vector, metadata_filters):
        # Step 1: Retrieve chunks ensuring at least N distinct articles
        chunks_by_article = self.retrieve_chunks(query_vector)

        # Get article IDs of retrieved chunks
        article_ids_from_similarity = list(chunks_by_article.keys())
        print(f"Article IDs from similarity: {article_ids_from_similarity}")

        # Step 2: Filter articles by metadata criteria
        article_ids_from_metadata = self.get_article_ids_filtered_by_metadata(
            metadata_filters
        )
        print(f"Article IDs from metadata: {article_ids_from_metadata}")

        # Step 3: Take intersection of filtered article IDs
        final_article_ids = list(
            set(article_ids_from_similarity) & set(article_ids_from_metadata)
        )

        # Filter the chunks to keep only those from articles passing metadata criteria
        # final_chunks_by_article = {aid: chunks_by_article[aid] for aid in final_article_ids}
        final_chunks_by_article = {
            aid: chunks_by_article[aid] for aid in final_article_ids
        }

        return final_chunks_by_article

    def parse_results(self, result):
        parsed_output = {}

        for article_id, points in result.items():
            parsed_output[article_id] = []

            for point in points:
                payload = point.payload  # Access as attribute instead of dict key

                # Extract the desired fields from the payload
                entry = {
                    "chunk_id": payload["chunk_id"],
                    "chunk_text": payload["chunk_text"],
                    "chunk_score": point.score,  # Access score as attribute
                }

                parsed_output[article_id].append(entry)

        return parsed_output


if __name__ == "__main__":
    retriever = Retriever(
        embeddings_model="pubmedbert",
        embedding_collection=vectordb_config["collections"]["pubmedbert"][
            "collection_name"
        ],
        metadata_collection=vectordb_config["collections"]["metadata"][
            "collection_name"
        ],
        top_k=5,
        top_n=3,
    )

    # Example query
    user_query = "Effect of PM2.5 in EGFR mutation in lung cancer"
    logger.info(f"User Query: {user_query}")
    query_vector = retriever.get_user_query_embeddings(user_query)[0]

    # Example metadata filters
    metadata_filters = {
        "journal": "Nature",
        # "year": "2023"
    }

    retrieved_chunks = retriever.retrieve(query_vector, metadata_filters)
    result = retriever.parse_results(retrieved_chunks)
    print(retrieved_chunks)
    file_path = "../../../data/results/parsed_result.json"
    full_result_file_path = "../../../data/results/full_result.json"

    # Write the parsed result to a JSON file
    with open(file_path, "w") as json_file:
        json.dump(result, json_file, indent=4)

    # with open(full_result_file_path, "w") as json_file:
    #     json.dump(retrieved_chunks, json_file, indent=4)
