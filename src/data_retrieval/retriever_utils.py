import requests
import xml.etree.ElementTree as ET
from src.data_processing.embedding.embeddings_handler import get_embeddings
from src.pubtator_utils.vector_db_handler.vector_db_handler_factory import (
    VectorDBHandler,
)
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve vector db specific config
vectordb_config = config_loader.get_config("vectordb")["vector_db"]
vector_db_type = vectordb_config["type"]
vector_db_params = vectordb_config[vector_db_type]["vector_db_params"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def initialize_vectordb_manager(collection_type: str):
    # Initialize the VectorDBHandler
    logger.info(f"Initializing VectorDB manager for collection type: {collection_type}")
    index_params = vectordb_config[vector_db_type]["index_params"][collection_type]

    # Get the Vector DB Handler with for specific vector db config
    vector_db_handler = VectorDBHandler(
        vector_db_params=vector_db_params, index_params=index_params
    )
    vector_db_manager = vector_db_handler.get_vector_db_manager(
        vector_db_type=vector_db_type
    )
    return vector_db_manager


def get_user_query_embeddings(embeddings_model: str, user_query: str):
    # Get embeddings for the user query
    query_vector = get_embeddings(model_name=embeddings_model, texts=[user_query])
    return query_vector.squeeze(0).tolist()


def get_pubmed_citations_count(pmid: str):
    elink_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    params = {
        "dbfrom": "pubmed",
        "linkname": "pubmed_pubmed_citedin",
        "id": pmid,
        "retmode": "xml",
    }
    response = requests.get(elink_url, params=params)
    response.raise_for_status()
    xml_content = response.text
    root = ET.fromstring(xml_content)
    citing_pmids = [
        link.find("Id").text
        for link in root.findall(".//LinkSetDb[LinkName='pubmed_pubmed_citedin']/Link")
    ]
    return len(citing_pmids)


def get_crossref_citations_count(doi: str):
    crossref_url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(crossref_url)
    response.raise_for_status()
    data = response.json()
    references = []
    if "message" in data and "reference" in data["message"]:
        references = [
            ref.get("DOI", ref.get("unstructured", "Unknown Reference"))
            for ref in data["message"]["reference"]
        ]
    return len(references)


def get_citations_count(pmid: str, doi: str):
    return {
        "pubmed_citations_count": get_pubmed_citations_count(pmid),
        "crossref_citations_count": get_crossref_citations_count(doi),
    }


if __name__ == "__main__":
    embeddings_model = "pubmedbert"
    user_query = "lung cancer risk from air pollution"
    query_embeddings = get_embeddings(model_name=embeddings_model, texts=[user_query])
    print(query_embeddings[0])
