from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve a specific config
# # Docker Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant"]
# # Cloud Qdrant
# vectordb_config = config_loader.get_config("vectordb")["qdrant_cloud"]

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class MetadataExtractor:
    def __init__(
        self,
        file_path: str,
        metadata_path: str,
        file_handler: FileHandler,
        s3_metadata_path: str,
        s3_file_handler: FileHandler,
    ):
        self.file_path = file_path
        self.metadata_path = metadata_path
        self.file_handler = file_handler
        self.s3_metadata_path = s3_metadata_path
        self.s3_file_handler = s3_file_handler
        self.metadata = {}

    def parse_xml(self):
        """Parse the XML file and extract metadata from <article-meta>, <front>, and <back> tags."""
        tree = self.file_handler.parse_xml_file(self.file_path)
        # tree = ET.parse(self.file_path)
        root = tree.getroot()

        # Extract <article-meta> metadata and place it first in the output
        self.metadata["article_meta"] = self.extract_article_meta(root)

        # Find the <article> tag and extract the article-type attribute
        article_element = root.find("article")
        self.metadata["article_meta"]["article_type"] = (
            article_element.get("article-type", "")
            if article_element is not None
            else ""
        )

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
            "keywords_from_source": [
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
            # citation = ref.find(".//mixed-citation") or ref.find(".//element-citation") or ref.find(".//citation")
            citation = None
            # Iterate over all descendant elements
            for child in ref.iter():
                if "citation" in child.tag:
                    citation = child
                    break
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

            # Extract authors
            authors = []

            # Case 1: Structured author names
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

            # Case 2: Authors embedded in citation text
            elif citation.text:
                # Extract text before the first "(" which likely contains author names
                author_text = (
                    citation.text.split("(")[0].strip()
                    if "(" in citation.text
                    else citation.text.strip()
                )
                authors_list = [a.strip() for a in author_text.split(";") if a.strip()]
                for author in authors_list:
                    parts = author.split()
                    if len(parts) > 1:
                        authors.append(
                            {"surname": parts[-1], "given-names": " ".join(parts[:-1])}
                        )
                    else:
                        authors.append({"surname": author, "given-names": ""})

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
            "day": (self.get_text(section, ".//day") or "").lstrip("0") or None,
            "month": (self.get_text(section, ".//month") or "").lstrip("0") or None,
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
        references = [
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
                # "authors": [
                #     {
                #         "surname": author.get("surname", ""),
                #         "given_names": author.get("given-names", ""),
                #         "etal": author.get("etal", False),
                #     }
                #     for author in ref.get("authors", [])
                # ],
                "authors": ref.get("authors", []),
            }
            for ref in metadata.get("back", {}).get("references", [])
        ]

        # Prepare the payload with other metadata fields
        payload = {
            "pmid": metadata.get("article_meta", {}).get("pmid", ""),
            "pmcid": metadata.get("article_meta", {}).get("pmcid", ""),
            "doi": metadata.get("article_meta", {}).get("doi", ""),
            "publisher-id": metadata.get("article_meta", {}).get("publisher-id", ""),
            "title": metadata.get("article_meta", {}).get("title", ""),
            "article_type": metadata.get("article_meta", {}).get("article_type", ""),
            "keywords_from_source": metadata.get("article_meta", {}).get(
                "keywords", ""
            ),
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
            "references_count": len(references),
            "references": references,
            "competing_interests": metadata.get("back", {}).get(
                "competing_interests", ""
            ),
        }

        self.file_handler.write_file_as_json(self.metadata_path, payload)
        logger.info(f"Metadata saved as JSON to {self.metadata_path}")

        self.s3_file_handler.write_file_as_json(self.s3_metadata_path, payload)
        logger.info(f"Metadata saved as JSON to S3 {self.s3_metadata_path}")


if __name__ == "__main__":
    # Example usage to get metadata from a PMC XML file on local

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)

    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    file_path = "../../data/staging/pmc_xml/PMC_6946810.xml"
    metadata_path = "../../data/articles_metadata/metadata/PMC_6946810_metadata.json"
    metadata_extractor = MetadataExtractor(
        file_path=file_path,
        metadata_path=metadata_path,
        file_handler=file_handler,
    )
    metadata_extractor.save_metadata_as_json()
