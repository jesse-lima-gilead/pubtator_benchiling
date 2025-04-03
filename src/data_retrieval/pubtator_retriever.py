import json
import pandas as pd
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_retrieval.retriever_utils import (
    initialize_vectordb_manager,
    get_user_query_embeddings,
    get_citations_count,
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

    def retrieve_matching_chunks(
        self,
        query_vector: list,
        metadata_filters: dict,
        top_k: int = 5,
        SCORE_THRESHOLD: float = 0.7,
    ):
        # Search across chunks, retrieve a larger set to ensure diversity
        retrieved_chunks = self.vectordb_manager.search_with_filters(
            query_vector=query_vector,
            filters=metadata_filters,
            top_k=top_k,  # Fetch a higher number to ensure we meet distinct article criteria
            SCORE_THRESHOLD=SCORE_THRESHOLD,
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

        # Add the citation count to metadata
        for article_id, chunks in chunks_by_article.items():
            pmid = chunks[0]["metadata"]["pmid"]
            doi = chunks[0]["metadata"]["doi"]
            citations = get_citations_count(pmid, doi)
            for chunk in chunks:
                chunk["metadata"]["citations_count_pubmed"] = citations[
                    "pubmed_citations_count"
                ]
                chunk["metadata"]["citations_count_crossref"] = citations[
                    "crossref_citations_count"
                ]

        print(f"Fetched article_ids: {list(chunks_by_article.keys())}")
        return chunks_by_article

    def get_distinct_field_values(self, field_name: str, field_value: str = None):
        distinct_values = self.vectordb_manager.get_distinct_values(
            field_name=field_name,
            field_value=field_value,
        )
        return distinct_values


def convert_to_table(user_query: str, search_results: str) -> pd.DataFrame:
    """Converts search function output to a pandas DataFrame."""

    # Convert string to dictionary
    search_results = json.loads(search_results)

    data = []

    for article_id, results in search_results.items():
        for result in results:
            metadata = result["metadata"]
            data.append(
                {
                    "User Query": user_query,
                    "Article ID": metadata.get("article_id", ""),
                    "Chunk ID": result["id"],
                    "Score": result["score"],
                    "Citations Count (PubMed)": metadata.get(
                        "citations_count_pubmed", 0
                    ),
                    "Citations Count (CrossRef)": metadata.get(
                        "citations_count_crossref", 0
                    ),
                    "Journal": metadata.get("journal", ""),
                    "Article Type": metadata.get("article_type", ""),
                    "Title": metadata.get("title", ""),
                    "PMID": metadata.get("pmid", ""),
                    "DOI": metadata.get("doi", ""),
                    "Publication Date": f"{metadata['publication_date']['year']}-"
                    f"{metadata['publication_date']['month'].zfill(2)}-"
                    f"{metadata['publication_date']['day'].zfill(2)}",
                    "Authors": ", ".join(metadata.get("authors", [])),
                    "Chunk Text": metadata.get("merged_text", ""),
                }
            )

    df = pd.DataFrame(data)
    return df


def search(
    user_query: str,
    metadata_filters: dict = None,
    show_as_table: bool = False,
    top_n: int = 5,
    top_k: int = 100,
    SCORE_THRESHOLD: float = 0.7,
    embeddings_model: str = "pubmedbert",
):
    pubtator_retriever = PubtatorRetriever()
    user_query_embeddings = get_user_query_embeddings(
        user_query=user_query, embeddings_model=embeddings_model
    )

    # Get the relevant chunks from Vector store filtered by Metadata Filters
    if metadata_filters is None:
        metadata_filters = {}

    final_chunks_by_article = pubtator_retriever.retrieve_matching_chunks(
        query_vector=user_query_embeddings,
        metadata_filters=metadata_filters,
        top_n=top_n,
        top_k=top_k,
        SCORE_THRESHOLD=SCORE_THRESHOLD,
    )

    if show_as_table:
        return convert_to_table(
            user_query=user_query,
            search_results=json.dumps(final_chunks_by_article, indent=4),
        )
    else:
        return json.dumps(final_chunks_by_article, indent=4)


def get_distinct_values(
    field_name: str, field_value: str = None, show_as_table: bool = False
):
    pubtator_retriever = PubtatorRetriever()
    distinct_values = pubtator_retriever.get_distinct_field_values(
        field_name=field_name, field_value=field_value
    )
    if show_as_table:
        return pd.DataFrame(distinct_values)
    else:
        return json.dumps(distinct_values, indent=4)


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

    # Get the relevant chunks from Vector store filtered by Metadata Filters
    final_chunks_by_article = search(
        user_query=user_query, metadata_filters=metadata_filters
    )

    print(f"Final Chunks by Article: \n{final_chunks_by_article}")
