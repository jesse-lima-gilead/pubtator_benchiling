import os
import xml.etree.ElementTree as ET
from datetime import datetime

import bioc
import requests
from Bio import Entrez

from src.utils.logger import SingletonLogger
from src.utils.s3_io_util import S3IOUtil

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PubMedBioCConverter:
    def __init__(self, email, output_dir="../../data/bioc_xml"):
        Entrez.email = email
        self.output_dir = output_dir

    # Modified to use your search_pubmed logic for querying PubMed
    def search_pubmed(self, query, start_date=None, end_date=None, retmax=50):
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        date_range = (
            f" AND {start_date}:{end_date}[dp]" if start_date and end_date else ""
        )
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
        pmcids = data["esearchresult"]["idlist"]
        logger.info(f"Found {len(pmcids)} articles at Pubmed Central.")
        return pmcids

    # Fetches article XML data using your fetch_data logic
    def fetch_data(self, article_id):
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={article_id}"
        response = requests.get(url)
        xml_content = response.content

        if (
            b"<!--The publisher of this article does not allow downloading of the full text in XML form.-->"
            in xml_content
        ):
            logger.info(
                f"Article {article_id} does not allow downloading of the full text in XML form."
            )
            return None

        root = ET.fromstring(xml_content)
        body = self.extract_text_content(root, ".//body")
        title = self.extract_text_content(
            root, ".//front/article-meta/title-group/article-title"
        )
        abstract = self.extract_text_content(root, ".//abstract")

        if title is None or abstract is None or body is None:
            logger.info(f"Article {article_id} does not have title, abstract or body.")
            return None

        logger.info(f"Article {article_id} fetched successfully.")
        return xml_content

    def extract_text_content(self, root, xpath):
        element = root.find(xpath)
        element_string = (
            ET.tostring(element, encoding="unicode") if element is not None else None
        )
        if element_string is not None:
            return "".join(element.itertext())
        else:
            return None

    # Convert the fetched XML into BioC format
    def convert_to_bioc(self, pubmed_data):
        root = ET.fromstring(pubmed_data)
        pubmed_collection = bioc.BioCCollection()
        pubmed_collection.source = "PubMed"
        pubmed_collection.date = datetime.now().strftime("%Y-%m-%d")

        for article in root.findall(".//article"):
            document = bioc.BioCDocument()
            pmid = article.find(".//article-id").text
            document.id = f"PMC-{pmid}"

            title = self.extract_text_content(
                root=article, xpath=".//title-group/article-title"
            )
            abstract = self.extract_text_content(root=article, xpath=".//abstract")
            authors = []
            for author in article.findall(".//contrib-group/contrib"):
                last_name = (
                    author.find("surname").text
                    if author.find("surname") is not None
                    else ""
                )
                first_name = (
                    author.find("given-names").text
                    if author.find("given-names") is not None
                    else ""
                )
                authors.append(f"{first_name} {last_name}")
            authors_str = "; ".join(authors)

            document.infons["title"] = title
            document.infons["authors"] = authors_str
            document.infons["pmid"] = pmid

            title_passage = bioc.BioCPassage()
            title_passage.infons["type"] = "title"
            title_passage.text = title
            document.add_passage(title_passage)

            abstract_passage = bioc.BioCPassage()
            abstract_passage.infons["type"] = "abstract"
            abstract_passage.text = abstract
            document.add_passage(abstract_passage)

            pubmed_collection.add_document(document)
        logger.info(
            f"Converted {len(pubmed_collection.documents)} articles to BioC format."
        )
        return pubmed_collection

    # Save the BioC XML file locally
    def write_bioc_to_local(self, pubmed_collection):
        for document in pubmed_collection.documents:
            single_doc_collection = bioc.BioCCollection()
            single_doc_collection.source = pubmed_collection.source
            single_doc_collection.date = pubmed_collection.date
            single_doc_collection.add_document(document)

            bioc_xml = bioc.dumps(single_doc_collection, pretty_print=True)
            file_path = os.path.join(self.output_dir, f"{document.id}.xml")
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(bioc_xml)
            logger.info(f"BioC XML file saved to {file_path}")

    # Save the BioC XML file to S3
    def write_bioc_to_s3(self, pubmed_collection):
        s3_io_util = S3IOUtil()
        for document in pubmed_collection.documents:
            local_file_path = os.path.join(self.output_dir, f"{document.id}.xml")
            s3_io_util.upload_file(
                file_path=local_file_path, object_name=f"bioc_xml/{document.id}.xml"
            )
            logger.info(f"BioC XML file saved to S3: bioc_xml/{document.id}.xml")

    # Runs the combined process
    def run(self, query, start_date=None, end_date=None, retmax=50):
        article_ids = self.search_pubmed(query, start_date, end_date, retmax)
        logger.info(f"Found {len(article_ids)} articles.")
        missing_count = 0
        pubmed_collection = bioc.BioCCollection()

        for article_id in article_ids:
            xml_content = self.fetch_data(article_id)
            if xml_content:
                single_pubmed_collection = self.convert_to_bioc(xml_content)
                pubmed_collection.documents.extend(single_pubmed_collection.documents)
            else:
                missing_count += 1

        logger.info(f"Total missing articles: {missing_count}")
        self.write_bioc_to_local(pubmed_collection)
        self.write_bioc_to_s3(pubmed_collection)


# Example usage
if __name__ == "__main__":
    logger.info("Execution Started")
    email = "your.email@example.com"
    converter = PubMedBioCConverter(email=email)
    converter.run(query="lung cancer", start_date="2019", end_date="2023", retmax=50)
    logger.info("Execution Completed")
