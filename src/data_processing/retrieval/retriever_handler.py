import json
import os
from src.utils.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

def write_results_to_local(query, search_results, output_dir):
    results_dic = {}
    results_dic["user_query"] = query
    retrieved_results = {}

    for cur_combination, points_list in search_results.items():
        retrieved_results[cur_combination] = []
        print(cur_combination)
        for cur_point in points_list:
            cur_point_dic = {}
            cur_point_dic["score"] = cur_point.score
            cur_point_dic["chunk_text"] = cur_point.payload["chunk_text"]
            cur_point_dic["article_id"] = cur_point.payload["article_id"]
            retrieved_results[cur_combination].append(cur_point_dic)

    results_dic["search_results"] = retrieved_results

    # Specify the filename
    filename = f"{query}.json"
    # Complete file path
    file_path = os.path.join(output_dir, filename)

    # Open the file in write mode and serialize the data to JSON
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(results_dic, file, indent=4)


def retrieve_chunks(query, chunker_type, merger_type, ner_model, vector_db, model):
    # Get the appropriate indexer instance
    pass

def retrieve_query(self, query: str, limit: int = 1):
    """
    Retrieves related data to the user's question by querying the vector store.

    Args:
        query (str): The user's query.
        limit (int, optional): Defaults to 1. The no. of closest chunks to be returned.
    """
    logger.info(f"Processing query: {query}")

    # Generate the embedding for the query
    query_embedding = self.embed_llm.embed_query(query)

    # Search the vector store for relevant vectors
    search_results = self.qdrant_manager.search_vectors(query_embedding, limit)

    logger.info(f"Retrieved results: {search_results}")

    # Extract relevant texts from the search results
    retrieved_chunks = [result.payload["chunk_text"] for result in search_results]

    # Combine the relevant texts into a context for the LLM
    retrieved_chunks = "\n".join(retrieved_chunks)

    logger.info(f"Retrieved chunks: {retrieved_chunks}")

    return search_results


# Run the main function
if __name__ == "__main__":
    vector_db_list = ["qdrant"]
    llm_model = "BedrockClaude"
    chunker_list = [
        # "sliding_window",
        # "passage",
        # "annotation_aware",
        "grouped_annotation_aware_sliding_window",
    ]

    merger_list = [
        "append",
        "inline",
        # "fulltext"
    ]

    ner_model_list = [
        "bioformer",
        # "pubmedbert"
    ]

    user_queries = ["EGFR", "KRAS", "ATM"]
    output_path = "../../../test_data/retrieval_results/"

    for cur_query in user_queries:
        query_results = {}
        for cur_chunker_type in chunker_list:
            for cur_merger_type in merger_list:
                for cur_ner_model in ner_model_list:
                    for cur_vector_db in vector_db_list:
                        retrieved_result = retrieve_chunks(
                            cur_query,
                            cur_chunker_type,
                            cur_merger_type,
                            cur_ner_model,
                            cur_vector_db,
                            llm_model,
                        )
                        combination_str = (
                            f"{cur_chunker_type}_{cur_merger_type}_{cur_ner_model}"
                        )
                        query_results[combination_str] = retrieved_result
        write_results_to_local(cur_query, query_results, output_path)
