from src.pubtator_utils.prompts_handler.PromptBuilder import PromptBuilder

from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_retrieval.retriever_utils import (
    initialize_vectordb_manager,
    get_user_query_embeddings,
)

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PubtatorRetriever:
    def __init__(
        self,
        embeddings_model: str = "pubmedbert",
        collection_type: str = "processed_pubmedbert",
        top_k: int = 5,
        top_n: int = 5,
    ):
        self.vectordb_manager = initialize_vectordb_manager(
            collection_type=collection_type
        )
        self.embeddings_model = embeddings_model
        self.top_k = top_k
        self.top_n = top_n
        logger.info(f"Initialized the retriever!")

    def retrieve_matching_chunks(self, query_vector, metadata_filters):
        # Search across chunks, retrieve a larger set to ensure diversity
        retrieved_chunks = self.vectordb_manager.search_with_filters(
            query_vector=query_vector,
            filters=metadata_filters,
            top_k=100,  # Fetch a higher number to ensure we meet distinct article criteria
        )

        # Collect chunks by article_id and ensure we have chunks from at least N distinct articles
        chunks_by_article = {}
        for chunk in retrieved_chunks:
            article_id = chunk["metadata"].get("article_id", [])
            if article_id not in chunks_by_article:
                chunks_by_article[article_id] = []
            if len(chunks_by_article[article_id]) < self.top_k:
                chunks_by_article[article_id].append(chunk)

            # Stop if we've accumulated at least N distinct articles with chunks
            if len(chunks_by_article) >= self.top_n:
                break

        logger.info(f"Fetched article_ids: {list(chunks_by_article.keys())}")
        return chunks_by_article

    def get_distinct_field_values(self, field_name: str):
        distinct_values = self.vectordb_manager.get_distinct_values(
            field_name=field_name
        )
        return distinct_values


def search(
    user_query: str, metadata_filters: dict, embeddings_model: str = "pubmedbert"
):
    pubtator_retriever = PubtatorRetriever()
    user_query_embeddings = get_user_query_embeddings(embeddings_model, user_query)

    # Get the relevant chunks from Vector store filtered by Metadata Filters
    final_chunks_by_article = pubtator_retriever.retrieve_matching_chunks(
        user_query_embeddings, metadata_filters
    )

    return final_chunks_by_article


def get_distinct_values(field_name: str):
    pubtator_retriever = PubtatorRetriever()
    distinct_values = pubtator_retriever.get_distinct_field_values(field_name)
    return distinct_values


if __name__ == "__main__":
    embeddings_model = "pubmedbert"
    collection_type = "processed_pubmedbert"
    top_k = 5
    top_n = 3

    user_queries = [
        "Effect of PM2.5 in EGFR mutation in lung cancer",
        "PI3K/AKT/mTOR and therapy resistance",
        "PD-1 or PD-L1 biomarker and  lung cancer",
        "ScRNA seq with immune cell signatures and lung cancer",
    ]

    user_query = user_queries[1]

    metadata_filters = {
        "journal": "Nature",
        "years_after": 2005,
        "title": "Lung cancer promotion by air pollution",
        "authors": "Wiliam Hil",
    }

    # metadata_filters = {
    #     "journal": "Nature",  # Pre-filter (Low Cardinality)
    #     "year": 2022,  # Exact match
    #     "years_after": 2015,  # Range filter: year > 2015
    #     "years_before": 2020,  # Range filter: year < 2020
    #     "authors": "John Doe",  # Post-filter (High Cardinality)
    #     "keywords": "gene therapy",  # Post-filter (High Cardinality)
    #     "title": "Cancer Treatment"  # Post-filter (High Cardinality)
    # }

    # Get the relevant chunks from Vector store filtered by Metadata Filters
    final_chunks_by_article = search(
        user_query=user_query, metadata_filters=metadata_filters
    )

    print(f"Final Chunks by Article: \n{final_chunks_by_article}")
