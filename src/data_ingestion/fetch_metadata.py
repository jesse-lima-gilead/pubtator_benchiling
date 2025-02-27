import os
import uuid

from src.utils.file_handler.base_handler import FileHandler
from src.utils.vector_db_handler.qdrant_handler import QdrantHandler
from src.data_processing.embedding.embeddings_handler import (
    get_embeddings,
    get_model_info,
)
from src.utils.config_handler.config_reader import YAMLConfigLoader
from src.utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
# # Docker Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant"]
# Cloud Qdrant
vectordb_config = config_loader.get_config("vectordb")["qdrant_cloud"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class MetadataExtractor:
    def __init__(
        self,
        file_path: str,
        metadata_path: str,
        file_handler: FileHandler,
    ):
        self.file_path = file_path
        self.metadata_path = metadata_path
        self.file_handler = file_handler
        self.metadata = {}

    def parse_xml(self):
        """Parse the XML file and extract metadata from <article-meta>, <front>, and <back> tags."""
        tree = self.file_handler.parse_xml_file(self.file_path)
        # tree = ET.parse(self.file_path)
        root = tree.getroot()

        # Extract <article-meta> metadata and place it first in the output
        self.metadata["article_meta"] = self.extract_article_meta(root)

        # Extract <front> and <back> metadata
        self.metadata["front"] = self.extract_section_metadata(root, "front")
        self.metadata["back"] = self.extract_section_metadata(root, "back")

        return self.metadata

    def extract_article_meta(self, root):
        """Extract metadata from the <article-meta> tag."""
        article_meta = root.find(".//article-meta")
        if article_meta is None:
            return {}

        # Extracting common identifiers
        identifiers = {
            "pmid": self.get_text(article_meta, "article-id[@pub-id-type='pmid']"),
            "pmcid": self.get_text(article_meta, "article-id[@pub-id-type='pmc']"),
            "doi": self.get_text(article_meta, "article-id[@pub-id-type='doi']"),
            "publisher-id": self.get_text(
                article_meta, "article-id[@pub-id-type='publisher-id']"
            ),
        }

        # Adding additional article metadata like title and keywords if available
        article_data = {
            "title": self.get_text(article_meta, "title-group/article-title"),
            "keywords": [
                kwd.text.strip() for kwd in article_meta.findall(".//kwd") if kwd.text
            ],
        }

        # Merging identifiers and additional data
        return {**identifiers, **article_data}

    def extract_section_metadata(self, root, tag_name):
        """Extract metadata from a specified section (<front> or <back>)."""
        section = root.find(f".//{tag_name}")
        if section is None:
            return {}

        section_data = {
            "authors": self.extract_authors(section),
            "journal_metadata": self.extract_journal_metadata(section),
            "funding": self.extract_funding_info(section),
            "publication_date": self.extract_publication_date(section),
            "license": self.get_text(section, ".//ext-link"),
            "competing_interests": self.get_text(
                section, ".//p[@type='competing-interest']"
            ),
        }

        # Extract references if processing the back section
        if tag_name == "back":
            section_data["references"] = self.extract_references(section)

        # Remove empty fields
        return {k: v for k, v in section_data.items() if v}

    def extract_references(self, section):
        """Extract references from the <ref-list> in the <back> section."""
        references = []

        # Locate the <ref-list> tag
        ref_list = section.find(".//ref-list")
        if ref_list is None:
            print("No <ref-list> found in the <back> section.")
            return None

        # Iterate over all <ref> elements
        for ref in ref_list.findall(".//ref"):
            ref_data = {}

            # Extract the reference ID and label
            ref_data["id"] = ref.get("id", None)
            ref_data["label"] = self.get_text(ref, ".//label")

            # Check for <mixed-citation> and <element-citation>
            citation = ref.find(".//mixed-citation") or ref.find(".//element-citation")
            if citation is None:
                print(f"No citation found for ref ID: {ref.get('id')}")
                continue

            # Extract publication details
            ref_data["publication-type"] = citation.get("publication-type", None)
            ref_data["article-title"] = self.get_text(citation, ".//article-title")
            ref_data["source"] = self.get_text(citation, ".//source")
            ref_data["year"] = self.get_text(citation, ".//year")
            ref_data["volume"] = self.get_text(citation, ".//volume")
            ref_data["fpage"] = self.get_text(citation, ".//fpage")
            ref_data["lpage"] = self.get_text(citation, ".//lpage")

            # Extract publication identifiers (e.g., DOI, PMID)
            ref_data["pub-id"] = {
                pub_id.get("pub-id-type"): pub_id.text.strip()
                for pub_id in citation.findall(".//pub-id")
                if pub_id.text
            }

            # Extract author information from <person-group>
            authors = []
            person_group = citation.find(".//person-group[@person-group-type='author']")
            if person_group is not None:
                for name in person_group.findall(".//name"):
                    author_data = {
                        "surname": self.get_text(name, "surname"),
                        "given-names": self.get_text(name, "given-names"),
                    }
                    authors.append(author_data)
                if person_group.find(".//etal") is not None:
                    authors.append({"etal": True})
            ref_data["authors"] = authors

            # Append the reference data if not empty
            if ref_data:
                references.append(ref_data)

        # Return the list of references
        return references if references else None

    def extract_authors(self, section):
        """Extract author information from the <front> or <back> section."""
        authors = []
        for contrib in section.findall(".//contrib[@contrib-type='author']"):
            author_data = {
                "surname": self.get_text(contrib, "name/surname"),
                "given-names": self.get_text(contrib, "name/given-names"),
                "orcid": self.get_text(contrib, "contrib-id[@contrib-id-type='orcid']"),
            }
            authors.append({k: v for k, v in author_data.items() if v})
        return authors if authors else None

    def extract_journal_metadata(self, section):
        """Extract journal metadata from the <front> section."""
        return {
            "journal-id": self.get_text(section, ".//journal-id"),
            "journal-title": self.get_text(section, ".//journal-title"),
            "issn": self.get_text(section, ".//issn"),
        }

    def extract_article_metadata(self, section):
        """Extract article metadata from the <front> section."""
        return {
            "article-id": self.get_text(section, ".//article-id"),
            "article-title": self.get_text(section, ".//article-title"),
            "subject": self.get_text(section, ".//subject"),
        }

    def extract_funding_info(self, section):
        """Extract funding information from the <front> section."""
        funders = []
        for funding in section.findall(".//funding-source"):
            funder_data = {
                "institution": self.get_text(funding, "institution"),
                "award-id": self.get_text(funding, "award-id"),
            }
            funders.append(funder_data)
        return funders if funders else None

    def extract_publication_date(self, section):
        """Extract publication date from the <front> section."""
        return {
            "day": self.get_text(section, ".//day"),
            "month": self.get_text(section, ".//month"),
            "year": self.get_text(section, ".//year"),
        }

    def get_text(self, element, xpath):
        """Utility to get text content from an XML element."""
        tag = element.find(xpath)
        return tag.text.strip() if tag is not None and tag.text else None

    def get_metadata(self):
        metadata = self.parse_xml()
        return metadata

    def save_metadata_as_json(self):
        """Save the extracted metadata as a JSON file."""
        metadata = self.get_metadata()

        self.file_handler.write_file_as_json(self.metadata_path, self.metadata)
        logger.info(f"Metadata saved as JSON: {metadata}")
        # with open(self.metadata_path, "w") as json_file:
        #     json.dump(self.metadata, json_file, indent=4)

    def save_metadata_to_vector_db(self, embeddings_model: str = "pubmedbert"):
        """Save the extracted metadata to a vector database."""
        metadata = self.get_metadata()

        # Initialize the QdrantHandler
        model_info = get_model_info(embeddings_model)
        qdrant_handler = QdrantHandler(
            collection_type="metadata", params=vectordb_config
        )
        qdrant_manager = qdrant_handler.get_qdrant_manager()

        # Extract Fields from Metadata
        title = metadata["article_meta"].get("title", "")
        keywords = " ".join(metadata["article_meta"].get("keywords", []))
        combined_text = f"{title} {keywords}"
        combined_text_embeddings = get_embeddings(
            model_name=model_info[0],
            token_limit=model_info[1],
            texts=[combined_text],
        )[0]

        # Prepare the payload with other metadata fields
        payload = {
            "point_id": str(uuid.uuid4()),
            "pmid": metadata.get("article_meta", {}).get("pmid", ""),
            "pmcid": metadata.get("article_meta", {}).get("pmcid", ""),
            "doi": metadata.get("article_meta", {}).get("doi", ""),
            "publisher-id": metadata.get("article_meta", {}).get("publisher-id", ""),
            "title": metadata.get("article_meta", {}).get("title", ""),
            "keywords": metadata.get("article_meta", {}).get("keywords", ""),
            "authors": [
                f"{author.get('given-names', '')} {author.get('surname', '')}"
                for author in metadata.get("front", {}).get("authors", [])
            ],
            "journal": metadata.get("front", {})
            .get("journal_metadata", {})
            .get("journal-title", ""),
            "publication_date": {
                "day": metadata.get("front", {})
                .get("publication_date", {})
                .get("day", ""),
                "month": metadata.get("front", {})
                .get("publication_date", {})
                .get("month", ""),
                "year": metadata.get("front", {})
                .get("publication_date", {})
                .get("year", ""),
            },
            "license": metadata.get("front", {}).get("license", ""),
            "references": [
                {
                    "id": ref.get("id", ""),
                    "label": ref.get("label", ""),
                    "publication_type": ref.get("publication-type", ""),
                    "article_title": ref.get("article-title", ""),
                    "source": ref.get("source", ""),
                    "year": ref.get("year", ""),
                    "volume": ref.get("volume", ""),
                    "fpage": ref.get("fpage", ""),
                    "lpage": ref.get("lpage", ""),
                    "pub_id": {
                        "doi": ref.get("pub-id", {}).get("doi", ""),
                        "pmid": ref.get("pub-id", {}).get("pmid", ""),
                    },
                    "authors": [
                        {
                            "surname": author.get("surname", ""),
                            "given_names": author.get("given-names", ""),
                            "etal": author.get("etal", False),
                        }
                        for author in ref.get("authors", [])
                    ],
                }
                for ref in metadata.get("back", {}).get("references", [])
            ],
            "competing_interests": metadata.get("back", {}).get(
                "competing_interests", ""
            ),
        }

        # print(payload)

        # Insert into Qdrant
        qdrant_manager.insert_vector(vector=combined_text_embeddings, payload=payload)


if __name__ == "__main__":
    # Example usage to get metadata from a PMC XML file on local
    # file_path = (
    #     "../../data/staging/pmc_xml/PMC_6946810.xml"
    # )
    # metadata_path = "../../data/articles_metadata/metadata/PMC_6946810_metadata.json"
    # extractor = MetadataExtractor(file_path, metadata_path, "pubmedbert")
    # extractor.save_metadata_to_vector_db()
    # metadata = extractor.parse_xml()
    # extractor.save_metadata_as_json()
    # print(metadata)

    # Example usage to save metadata to a vector database
    pmc_xml_dir = "../../data/staging/pmc_xml"
    embeddings_model = "pubmedbert"
    for file in os.listdir(pmc_xml_dir):
        if file.endswith(".xml"):
            logger.info(f"Processing {file}..")
            file_path = os.path.join(pmc_xml_dir, file)
            metadata_path = os.path.join(
                "../../data/articles_metadata/metadata",
                file.replace(".xml", "_metadata.json"),
            )
            extractor = MetadataExtractor(file_path, metadata_path, embeddings_model)
            extractor.save_metadata_to_vector_db()
            # metadata = extractor.parse_xml()
            # extractor.save_metadata_as_json()
            logger.info(f"Metadata saved to Vector DB")
