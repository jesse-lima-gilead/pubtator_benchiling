import os
import shutil
from datetime import datetime
import boto3
import requests
import xml.etree.ElementTree as ET
from src.pubtator_utils.embeddings_handler.embeddings_generator import get_embeddings
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


def get_user_query_embeddings(user_query: str, embeddings_model: str = "pubmedbert"):
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


def build_summarization_prompt(search_results_json, user_query):
    """
    Build a summarization prompt that incorporates the user query and the retrieved context.
    It aggregates context passages (sorted by score), then includes instructions (do's and don'ts)
    so that the LLM responds directly to the user's query using only the provided context.
    """
    all_chunks = []
    for article_id, chunks in search_results_json.items():
        for chunk in chunks:
            score = chunk.get("score", 0)
            text = chunk["metadata"].get("merged_text", "")
            all_chunks.append({"article_id": article_id, "score": score, "text": text})

    # Sort the chunks so that higher-score passages are considered first.
    sorted_chunks = sorted(all_chunks, key=lambda x: x["score"], reverse=True)

    context_parts = []
    for chunk in sorted_chunks:
        # Label each passage with its article ID and score for reference.
        part = f"Article [{chunk['article_id']}] (score: {chunk['score']:.3f}):\n{chunk['text']}"
        context_parts.append(part)

    context_string = "\n\n".join(context_parts)

    # Craft the prompt including the user query.
    prompt = (
        "You are a summarization assistant that must generate responses ONLY using the provided context below. \n\n"
        "User Query:\n"
        f"{user_query}\n\n"
        "Instructions:\n"
        "Do's:\n"
        "1. Use the provided context (text passages and metadata) to generate your answer related to the user query.\n"
        "2. Highlight and mention the relevant parts of the context you used as citations.\n"
        "3. Provide the response in the following JSON format:\n"
        "   {\n"
        '       "Response": "<Your clear and concise answer to the query>",\n'
        '       "Citations": [ { "pmc_id": "<PMC_ID>", "excerpt": "<Relevant excerpt>" }, ... ]\n'
        "   }\n"
        "4. Always base your answer strictly on the provided context.\n\n"
        "Don'ts:\n"
        "1. Do not generate any information that is not present in the provided context.\n"
        "2. Do not omit citations if you use the context.\n\n"
        "Below are the text excerpts from the research articles:\n\n"
        f"{context_string}\n\n"
        "Please provide your answer by summarizing the context in relation to the user query."
    )

    return prompt


def download_s3_to_dbfs(pmc_id, txt_to_highlight):
    # s3_bucket, s3_key, dbfs_path
    """
    Downloads a file from S3 to a DBFS path (e.g., /dbfs/tmp/myfile.txt)
    """
    # Generate timestamp with microsecond precision
    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )  # e.g., 20250424_115935_123456

    s3_bucket = "gilead-edp-research-dev-us-west-2-pubmed"
    s3_key = f"static_html/{pmc_id}.html"
    output_filename = f"{pmc_id}_article_{timestamp}.html"
    dbfs_path = f"/dbfs/FileStore/pubmed_data/temp_s3_html/{output_filename}"
    s3 = boto3.client("s3")

    print(dbfs_path, "dbfs_path")

    # Ensure local directory exists
    os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)

    try:
        s3.download_file(s3_bucket, s3_key, dbfs_path)
        print(f"Downloaded s3://{s3_bucket}/{s3_key} to {dbfs_path}")
    except Exception as e:
        print(f"Error downloading file: {str(e)}")

    # Step 1: Read XML content
    with open(dbfs_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    html_content = html_content.replace("{{HIGHLIGHT_TEXT}}", txt_to_highlight)

    # Step 2: Save HTML file with Highlighted text
    with open(dbfs_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    download_dbfs_dir = "files/pubmed_data/temp_s3_html"
    download_url = f"https://{workspace_url}/{download_dbfs_dir}/{output_filename}"

    return download_url


def clear_dbfs_temp_folder(dbfs_folder_path):
    """
    Deletes all files and subdirectories in the given DBFS folder.
    """
    if not dbfs_folder_path.startswith("/dbfs/"):
        dbfs_folder_path = "/dbfs/" + dbfs_folder_path.lstrip("/")

    if os.path.exists(dbfs_folder_path):
        shutil.rmtree(dbfs_folder_path)
        print(f"Cleared: {dbfs_folder_path}")
    else:
        print(f"Path does not exist: {dbfs_folder_path}")


if __name__ == "__main__":
    embeddings_model = "pubmedbert"
    user_query = "lung cancer risk from air pollution"
    query_embeddings = get_embeddings(model_name=embeddings_model, texts=[user_query])
    print(query_embeddings[0])
