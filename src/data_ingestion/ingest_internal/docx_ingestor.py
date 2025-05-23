from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import date
from src.data_ingestion.ingest_internal.pandoc_processor import PandocProcessor
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class DocxProcessor:
    """
    Processor for .docx files to BioC XML.
    """

    def __init__(self, pandoc_processor: PandocProcessor):
        self.pandoc = pandoc_processor

    def _clean_text(self, text):
        """
        Clean up text by splitting on newlines, removing empty lines, and trimming spaces.
        """
        lines = text.split("\n")
        cleaned = [line.strip() for line in lines if line.strip()]
        return " ".join(cleaned)

    def _html_to_bioc(
        self,
        html_path,
        xml_path,
        file_handler: FileHandler,
        source="Gilead Internal Documents",
    ):
        logger.info(f"Reading HTML File: {html_path}")
        html_content = file_handler.read_file(html_path)
        soup = BeautifulSoup(html_content, "html.parser")

        # Prepare BioC collection and document
        collection = ET.Element("collection")
        ET.SubElement(collection, "source").text = source
        ET.SubElement(collection, "date").text = date.today().isoformat()
        document = ET.SubElement(collection, "document")
        ET.SubElement(document, "id").text = html_path.split("/")[-1].split(".")[
            0
        ]  # get the file name
        ET.SubElement(document, "infons")  # metadata placeholder

        # Extract relevant elements in order
        elements = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "div"])

        # Initialize grouping state
        heading_parts = []  # consecutive headings buffer
        current_paras = []  # paragraphs under current heading
        groups = []  # list of (heading, paras)

        def flush():
            nonlocal heading_parts, current_paras
            if current_paras:
                heading = " ".join(heading_parts).strip() or "Untitled Section"
                groups.append((heading, current_paras))
                # reset for next group
                heading_parts = []
                current_paras = []

        # Iterate through elements to form groups
        for elem in elements:
            # Skip any element inside a table
            if elem.find_parent("table"):
                continue
            tag = elem.name.lower()
            text = self._clean_text(elem.get_text())

            # Real heading tags
            if tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                # if we already have paras, flush them before starting new heading
                if current_paras:
                    flush()
                heading_parts.append(text)
                continue

            # Pseudo-heading detection in <p> or <div>
            if tag in ["p", "div"]:
                children = list(elem.children)
                if len(children) == 1 and getattr(children[0], "name", None) in [
                    "strong",
                    "b",
                    "u",
                    "mark",
                ]:
                    if current_paras:
                        flush()
                    heading_parts.append(self._clean_text(children[0].get_text()))
                    continue

            # Regular paragraph
            if tag == "p" and text:
                current_paras.append(text)

        # Flush any remaining paragraphs
        flush()

        # Build BioC passages from groups
        for heading, paras in groups:
            passage = ET.SubElement(document, "passage")
            ET.SubElement(passage, "offset").text = "0"
            inf_el = ET.SubElement(passage, "infon", key="type")
            inf_el.text = heading
            text_el = ET.SubElement(passage, "text")
            text_el.text = " ".join(paras)

        # Write out pretty XML
        raw_xml = ET.tostring(collection, encoding="utf-8")
        pretty_xml = minidom.parseString(raw_xml).toprettyxml(indent="  ")
        file_handler.write_file(xml_path, pretty_xml)

    def _interim_file_format_converter(self, input_doc_path: str, output_doc_dir: str):
        try:
            input_doc_name = input_doc_path.split("/")[-1]
            input_doc_type = "docx"
            output_doc_type = "html"
            output_file_name = f'{input_doc_name.split(".")[0]}_docx.{output_doc_type}'
            output_doc_path = f"{output_doc_dir}/{output_file_name}"
            self.pandoc.convert(
                input_path=input_doc_path,
                output_path=output_doc_path,
                input_format=input_doc_type,
                output_format=output_doc_type,
            )
            return output_doc_path
        except Exception as ex:
            logger.exception("Interim file format conversion failed")
            raise RuntimeError(f"Interim conversion error: {ex}") from ex

    def _bioc_converter(
        self, input_doc_path: str, output_doc_dir: str, file_handler: FileHandler
    ):
        try:
            input_doc_name = input_doc_path.split("/")[-1]
            output_file_name = f'{input_doc_name.split(".")[0]}.xml'
            output_doc_path = f"{output_doc_dir}/{output_file_name}"
            self._html_to_bioc(input_doc_path, output_doc_path, file_handler)
        except Exception as ex:
            logger.exception("BioC converter failed")
            raise RuntimeError(f"BioC conversion error: {ex}") from ex

    def run(
        self,
        file_handler: FileHandler,
        internal_doc_name: str,
        internal_docs_path: str,
        bioc_path: str,
        **kwargs,
    ):
        try:
            input_doc_path = file_handler.get_file_path(
                internal_docs_path, internal_doc_name
            )
            internal_docs_interim_path = kwargs.get("internal_docs_interim_path")
            full_interim_file_path = self._interim_file_format_converter(
                input_doc_path, internal_docs_interim_path
            )
            logger.info(
                f"Succesfully completed Interim file format conversion for doc: {internal_doc_name}"
            )

            self._bioc_converter(full_interim_file_path, bioc_path, file_handler)
            logger.info(
                f"Succesfully completed BioC file format conversion for doc: {internal_doc_name}"
            )
        except Exception as ex:
            logger.exception("Run encountered an unexpected error")
            raise RuntimeError(f"Run workflow error: {ex}") from ex
