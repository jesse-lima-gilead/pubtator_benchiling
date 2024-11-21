import csv
import os.path
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


def initialize_article_qdrant_manager(embeddings_collection_type: str):
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(
        collection_type=embeddings_collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


def initialize_metadata_qdrant_manager(metadata_collection_type: str):
    # Initialize the QdrantHandler
    qdrant_handler = QdrantHandler(
        collection_type=metadata_collection_type, params=vectordb_config
    )
    qdrant_manager = qdrant_handler.get_qdrant_manager()
    return qdrant_manager


class Retriever:
    def __init__(
        self,
        embeddings_model: str,
        embeddings_collection_type: str,
        metadata_collection_type: str,
        top_k: int,
        top_n: int,
    ):
        self.article_qdrant_manager = initialize_article_qdrant_manager(
            embeddings_collection_type
        )
        self.metadata_qdrant_manager = initialize_metadata_qdrant_manager(
            metadata_collection_type
        )
        self.embeddings_model = embeddings_model
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
        # print(f"Matching article ids: {matching_article_ids}")

        return matching_article_ids

    def retrieve(self, query_vector, metadata_filters):
        # Step 1: Retrieve chunks ensuring at least N distinct articles
        chunks_by_article = self.retrieve_chunks(query_vector)

        # Get article IDs of retrieved chunks
        article_ids_from_similarity = [
            aid.split("_")[1] for aid in list(chunks_by_article.keys())
        ]
        print(f"Article IDs from similarity: {article_ids_from_similarity}")

        if len(metadata_filters) == 0:
            # Step 2: Filter articles by metadata criteria
            article_ids_from_metadata = self.get_article_ids_filtered_by_metadata(
                metadata_filters
            )
            print(f"Article IDs from metadata: {article_ids_from_metadata}")

            # Step 3: Take intersection of filtered article IDs
            final_article_ids = [
                "PMC_" + aid
                for aid in list(
                    set(article_ids_from_similarity) & set(article_ids_from_metadata)
                )
            ]
            print(f"Matching Articles Ids: {final_article_ids}")
        else:
            final_article_ids = ["PMC_" + aid for aid in article_ids_from_similarity]
            print(f"Matching Articles Ids: {final_article_ids}")

        # Filter the chunks to keep only those from articles passing metadata criteria
        # final_chunks_by_article = {aid: chunks_by_article[aid] for aid in final_article_ids}

        final_chunks_by_article = {
            aid: chunks_by_article[aid] for aid in final_article_ids
        }
        return final_chunks_by_article

    def parse_results(self, user_query, result):
        parsed_output = []

        for article_id, points in result.items():
            # parsed_output[article_id] = []

            for point in points:
                payload = point.payload  # Access as attribute instead of dict key

                # Extract the desired fields from the payload
                # entry = {
                #     "user_query": user_query,
                #     "article_id": article_id,
                #     "chunk_id": payload["chunk_id"],
                #     "chunk_text": payload["chunk_text"],
                #     "chunk_score": point.score,  # Access score as attribute
                # }

                entry = [
                    user_query,
                    article_id,
                    payload["chunk_id"],
                    payload["chunk_text"],
                    point.score,
                ]
                parsed_output.append(entry)

        return parsed_output


def flatten_list(nested_list):
    return [item for sublist in nested_list for item in sublist]


def run(run_type: str = "processed"):
    print("Runtype:", run_type)
    if run_type == "processed":
        output_path = "../../../data/results/processed/without_filter"
        results_file_path = "../../../data/results/processed_results.csv"
        retriever = Retriever(
            embeddings_model="pubmedbert",
            embeddings_collection_type="processed_pubmedbert",
            metadata_collection_type="metadata",
            top_k=5,
            top_n=3,
        )
    elif run_type == "baseline":
        output_path = "../../../data/results/baseline/without_filter"
        results_file_path = "../../../data/results/baseline_results.csv"
        retriever = Retriever(
            embeddings_model="pubmedbert",
            embeddings_collection_type="baseline",
            metadata_collection_type="metadata",
            top_k=5,
            top_n=3,
        )

    # Example query
    # user_queries = [
    #     "Effect of PM2.5 in EGFR mutation in lung cancer",
    #     "PI3K/AKT/mTOR and therapy resistance",
    # ]

    # Actual Run
    user_queries = [
        "Effect of PM2.5 in EGFR mutation in lung cancer",
        "PI3K/AKT/mTOR and therapy resistance",
        "PD-1 or PD-L1 biomarker and  lung cancer",
        "ScRNA seq with immune cell signatures and lung cancer",
        "ROS1 and lung cancer",
        "ctDNA and lung cancer",
        "mTOR/p70S6K/S6 pathway and breast cancer",
        "Caspase 3, Erk1/2 and Ovarian cancer",
        "TSC1/2, AMPK and Lung Cancer",
        "AKT-AMPK crosstalk and Diabetes",
        "EGFR, MET activation and Lung Cancer",
        "Rho activation and Ovarian cancer",
        "p27 kip and Ovarian cancer",
        "PI3K/AKT and Diabetes",
        "Nrf2 and lung cancer",
        "PI3K/AKT and lung cancer",
        "TGF b and Liver cancer",
        "p38 MAPK and diabetes",
        "IL2, VEGFR2 and Ovarian cancer",
        "TRAIL and obesity",
        "PI3/AKT and Ovarian cancer",
        "Haptoglobin and renal disease",
        "PI3K/AKT and obesity",
        "eNOS, VEGFR and Colon cancer",
        "Bcl6 lung cancer",
        "Ang II and obesity",
        "Erk and Obesity",
        "KRAS and lung cancer",
        "PI3K and obesity",
        "EGFR and lung cancer",
        "Akt1 and muscle hypertrophy",
        "CD24 and breast cancer",
        "ROS1 and lung cancer",
        "ALK4 and lung cancer",
        "EGFR and cervical lymphadenopathy",
        "PTEN and lung cancer",
        "ADAM17 and breast cancer",
        "VEGF and ovarian cancer",
        "AKT and diabetes",
        "PI3KR1 and breast cancer",
        "ERK1/2 and ovarian cancer",
        "ACE and diabetes",
        "Cyclin E and Ovarian cancer",
        "CD19 and DLBC",
        "CD138 and multiple myeloma",
        "MMP11 and foot ulcer",
        "CD44 and NSCLC",
        "PD-1 and NSCLC",
        "CTLA4 and non hodgkin lymphoma",
        "IL33 and COPD",
        "Lymphoma and RHOA",
        "Trem2 and retinal degeneration",
        "CD4 and infection",
        "BCL2/BCL6 and DLBCL",
        "Multiple myeloma and TNFSF13B",
        "Spinal cord development and CRABP1",
        "NSCLC and SP263",
        "B cell lymphoma and CREBP",
        "Cxcl2 and neurological disorders",
        "TIM3 and lung cancer",
        "STAT5B and leukemia",
        "Lrp1 and obesity",
        "PKM2 and retinopathy",
        "Serpina3n and inflammation",
        "HSP70AA1 and parkinsons disease",
    ]

    final_result = []

    for index, user_query in enumerate(user_queries):
        logger.info(f"Processing User Query: {user_query}")
        query_vector = retriever.get_user_query_embeddings(user_query)[0]

        # Example metadata filters
        metadata_filters = {
            # "journal": "Nature",
            # "year": "2023"
        }

        retrieved_chunks = retriever.retrieve(query_vector, metadata_filters)
        result = retriever.parse_results(user_query, retrieved_chunks)
        # print(result)
        final_result.append(result)

    # Flatten the list of results
    final_result = flatten_list(final_result)
    # print(final_result)

    # Write final_result to a csv
    headers = [
        "User Query",
        "Article ID",
        "Chunk ID",
        "Chunk Text",
        "Score",
    ]

    with open(results_file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(final_result)

        # result_file = f"{index}_result.json"
        # result_file_path = os.path.join(output_path, result_file)
        #
        # # Write the parsed result to a JSON file
        # with open(result_file_path, "w") as json_file:
        #     json.dump(result, json_file, indent=4)


if __name__ == "__main__":
    run_type = "processed"
    run(run_type=run_type)
