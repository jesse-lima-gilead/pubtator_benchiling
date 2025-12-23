import xml.etree.ElementTree as ET

import requests

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_text_content(root, xpath):
    element = root.find(xpath)
    element_string = (
        ET.tostring(element, encoding="unicode") if element is not None else None
    )
    if element_string is not None:
        return "".join(element.itertext())
    else:
        return None


def search_pubmed(query, start_date, end_date, retmax=50):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    logger.info(f"Specified dates: {start_date}, {end_date}")

    date_range = ""
    if start_date and end_date:
        date_range = f" AND {start_date}:{end_date}[dp]"

    params = {
        "db": "pmc",
        "term": f"{query}[All Fields]{date_range}",
        "retmode": "json",
        "retmax": retmax,
        "openaccess": "y",
        "sort": "relevance",
    }

    response = requests.get(base_url, params)
    data = response.json()

    pmc_ids = data["esearchresult"]["idlist"]
    return pmc_ids


## OLD fetch_data
# def fetch_data(article_id, only_body=False):
#     url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={article_id}"
#     response = requests.get(url)
#     xml_content = response.content
#
#     if (
#         b"<!--The publisher of this article does not allow downloading of the full text in XML form.-->"
#         in xml_content
#     ):
#         logger.info(
#             f"Article {article_id} does not allow downloading of the full text in XML form."
#         )
#         return None
#
#     root = ET.fromstring(xml_content)
#     body = extract_text_content(root, ".//body")
#     if only_body:
#         return body
#     title = extract_text_content(
#         root, ".//front/article-meta/title-group/article-title"
#     )
#     abstract = extract_text_content(root, ".//abstract")
#     if title is None or abstract is None or body is None:
#         logger.info(f"Article {article_id} does not have title, abstract or body.")
#         return None
#
#     return xml_content

import requests

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "grsar-ingestion/1.0",
        "Connection": "close",  # critical for PMC stability
    }
)

from requests.exceptions import (
    SSLError,
    ConnectionError,
    ChunkedEncodingError,
)
import time
import xml.etree.ElementTree as ET


def fetch_data(article_id, only_body=False, max_retries=6):
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pmc",
        "id": article_id,
        "retmode": "xml",
    }

    for attempt in range(max_retries):
        try:
            response = SESSION.get(url, params=params, timeout=(5, 180), stream=False)
            response.raise_for_status()

            xml_content = response.content  # safe now

            if (
                b"does not allow downloading of the full text in XML form"
                in xml_content
            ):
                logger.info(f"PMC {article_id} restricted")
                return None

            root = ET.fromstring(xml_content)

            body = extract_text_content(root, ".//body")
            if only_body:
                return body

            title = extract_text_content(
                root, ".//front/article-meta/title-group/article-title"
            )
            abstract = extract_text_content(root, ".//abstract")

            if not title or not abstract or not body:
                logger.info(
                    f"Article {article_id} does not have title, abstract or body."
                )
                return None

            return xml_content

        except (SSLError, ChunkedEncodingError, ConnectionError) as e:
            wait = min(60, 2**attempt)
            logger.warning(
                f"Network error for {article_id}: {type(e).__name__}, retrying in {wait}s"
            )
            time.sleep(wait)

        except ET.ParseError:
            logger.warning(f"XML parse error for {article_id}, retrying")
            time.sleep(2**attempt)

    logger.error(f"Failed to fetch PMC {article_id} after retries")
    return None


# def save_locally(file_name, content, pmc_local_path):
#     os.makedirs(pmc_local_path, exist_ok=True)
#     file_path = os.path.join(pmc_local_path, f"PMC_{file_name}.xml")
#
#     with open(file_path, "wb") as file:
#         file.write(content)


def extract_pmc_articles(
    query: str,
    article_ids: list,
    start_date: str,
    end_date: str,
    pmc_path: str,
    file_handler: FileHandler,
    s3_pmc_path: str,
    s3_file_handler: FileHandler,
    write_to_s3: bool,
    retmax=50,
):
    if len(article_ids) == 0 and query != "":
        article_ids = search_pubmed(query, start_date, end_date, retmax)
    # logger.info(article_ids)
    missing_count = 0
    for article_id in article_ids:
        content = fetch_data(article_id)
        if content:
            file_name = f"PMC_{article_id}.xml"
            logger.info(f"Fetching {file_name}")
            file_path = file_handler.get_file_path(pmc_path, file_name)
            file_handler.write_file(file_path, content)

            if write_to_s3:
                s3_file_path = s3_file_handler.get_file_path(s3_pmc_path, file_name)
                s3_file_handler.write_file(s3_file_path, content)
        else:
            missing_count += 1
        time.sleep(0.34)  # rate limit belongs HERE

    logger.info(f"Total number of article ids fetched: {len(article_ids)}")
    logger.info(f"Number of articles that couldn't be extracted: {missing_count}")

    extracted_articles_count = len(article_ids) - missing_count
    logger.info(f"Number of articles extracted: {extracted_articles_count}")

    return extracted_articles_count


# if __name__ == "__main__":
#     query = 'lung cancer'
#     start_date = "2019"
#     end_date = "2023"
#     pmc_local_path = '../../data/pmc_full_text_articles'
#     extract_pmc_articles(query, start_date, end_date, pmc_local_path)
