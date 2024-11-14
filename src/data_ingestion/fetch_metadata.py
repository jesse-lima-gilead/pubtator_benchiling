import json
from xml.etree import ElementTree as ET


class MetadataExtractor:
    def __init__(self, file_path: str, metadata_path: str):
        self.file_path = file_path
        self.metadata_path = metadata_path
        self.metadata = {}

    def parse_xml(self):
        """Parse the XML file and extract metadata from <article-meta>, <front>, and <back> tags."""
        tree = ET.parse(self.file_path)
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

        # Remove empty fields
        return {k: v for k, v in section_data.items() if v}

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

    def save_metadata_as_json(self):
        """Save the extracted metadata as a JSON file."""
        with open(self.metadata_path, "w") as json_file:
            json.dump(self.metadata, json_file, indent=4)


if __name__ == "__main__":
    # Example usage
    file_path = (
        "../../test_data/gilead_pubtator_results/pmc_full_text_articles/PMC_6946810.xml"
    )
    metadata_path = "../../test_data/gilead_pubtator_results/pmc_full_text_articles/PMC_6946810_metadata.json"
    extractor = MetadataExtractor(file_path)
    metadata = extractor.parse_xml()
    extractor.save_metadata_as_json()
    print(metadata)
