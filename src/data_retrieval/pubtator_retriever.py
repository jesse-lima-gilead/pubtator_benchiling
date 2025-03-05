from src.pubtator_utils.prompts_handler.PromptBuilder import PromptBuilder

from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_retrieval.retriever_utils import (
    initialize_qdrant_manager,
    get_user_query_embeddings,
)
from src.data_retrieval.metadata_filter import MetadataFilter

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
vectordb_config = config_loader.get_config("vectordb")["qdrant"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PubtatorRetriever:
    def __init__(
        self,
        embeddings_collection_type: str,
        metadata_collection_type: str,
        top_k: int,
        top_n: int,
    ):
        self.article_qdrant_manager = initialize_qdrant_manager(
            collection_type=embeddings_collection_type
        )
        self.metadata_filter = MetadataFilter(
            metadata_collection_type=metadata_collection_type
        )
        self.embeddings_model = embeddings_model
        self.top_k = top_k
        self.top_n = top_n

    def retrieve_matching_chunks(self, query_vector):
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

        # print(chunks_by_article)
        return chunks_by_article

    def retrieve_filtered_chunks(self, query_vector, metadata_filters):
        # Step 1: Retrieve chunks ensuring at least N distinct articles
        matching_chunks = self.retrieve_matching_chunks(query_vector)

        # Get article IDs of retrieved chunks
        article_ids_from_match = [
            aid.split("_")[1] for aid in list(matching_chunks.keys())
        ]
        print(f"Article IDs from similarity: {article_ids_from_match}")

        if len(metadata_filters) > 0:
            # Step 2: Filter articles by metadata criteria
            article_ids_from_metadata_filter = (
                self.metadata_filter.get_article_ids_filtered_by_metadata(
                    metadata_filters
                )
            )
            print(f"Article IDs from metadata: {article_ids_from_metadata_filter}")

            # Step 3: Take intersection of filtered article IDs
            final_article_ids = [
                "PMC_" + aid
                for aid in list(
                    set(article_ids_from_match) & set(article_ids_from_metadata_filter)
                )
            ]
            print(f"Matching Articles Ids: {final_article_ids}")
        else:
            final_article_ids = ["PMC_" + aid for aid in article_ids_from_match]
            print(f"Matching Articles Ids: {final_article_ids}")

        # Filter the chunks to keep only those from articles passing metadata criteria
        # final_chunks_by_article = {aid: chunks_by_article[aid] for aid in final_article_ids}

        final_chunks_by_article = {
            aid: matching_chunks[aid] for aid in final_article_ids
        }
        return final_chunks_by_article


if __name__ == "__main__":
    embeddings_model = "pubmedbert"
    embeddings_collection_type = "processed_pubmedbert"
    metadata_collection_type = "metadata"
    top_k = 5
    top_n = 3

    retriever = PubtatorRetriever(
        embeddings_collection_type=embeddings_collection_type,
        metadata_collection_type=metadata_collection_type,
        top_k=top_k,
        top_n=top_n,
    )

    user_queries = [
        "Effect of PM2.5 in EGFR mutation in lung cancer",
        "PI3K/AKT/mTOR and therapy resistance",
        "PD-1 or PD-L1 biomarker and  lung cancer",
        "ScRNA seq with immune cell signatures and lung cancer",
    ]

    user_query = user_queries[0]

    user_query_embeddings = get_user_query_embeddings(embeddings_model, user_query)

    metadata_filters = {
        "journal": "Nature",
        # "year": "2021",
    }

    # Get the relevant chunks from Vector store filtered by Metadata Filters
    final_chunks_by_article = retriever.retrieve_filtered_chunks(
        user_query_embeddings, metadata_filters
    )

    print(f"Final Chunks by Article: \n{final_chunks_by_article}")
