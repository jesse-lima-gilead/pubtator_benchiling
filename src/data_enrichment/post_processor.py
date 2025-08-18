import argparse
import xml.etree.ElementTree as ET

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class BioCFileMerger:
    def __init__(
        self,
        workflow_id: str,
        source: str,
        paths_config: dict[str, str],
        file_handler: FileHandler,
    ):
        """
        Initialize the merger with input directories for normalizers and an output directory.
        :param workflow_id: Unique identifier for the JIT workflow, used to maintain separate path for each flow.
        :param paths_config: A dictionary where keys are normalizer types and values are directory paths.
        :param file_handler: A file handler object to do file operations.
        """
        self.input_dirs = {
            "disease": paths_config["taggerone_disease_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source),
            "chemical": paths_config["nlmchem_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source),
            "cellline": paths_config["taggerone_cellLine_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source),
            "tmvar": paths_config["tmvar_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source),
        }
        self.output_dir = (
            paths_config["annotations_merged_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler

    def merge_files(self):
        """
        Merge files from all input directories and write the combined output to the output directory.
        """
        file_names = self.file_handler.list_files(self.input_dirs["disease"])
        for file_name in file_names:
            if not file_name.endswith(".xml"):
                continue
            logger.info(f"Processing file: {file_name}")
            documents = [
                self._parse_bioc_file(
                    self.file_handler.get_file_path(
                        self.input_dirs[normalizer], file_name
                    )
                )
                for normalizer in self.input_dirs
            ]
            merged_document = self._merge_documents(documents)
            self._write_merged_file(file_name, merged_document)
            logger.info(f"Merged file written: {file_name}")

    def _parse_bioc_file(self, file_path):
        """
        Parse a BioC XML file and return its root element.

        :param file_path: Path to the BioC XML file.
        :return: Root element of the parsed XML tree.
        """
        logger.info(f"Parsing file: {file_path}")
        tree = self.file_handler.parse_xml_file(file_path)
        return tree.getroot()

    def _merge_documents(self, documents):
        """
        Merge multiple BioC documents into one with sequential annotation IDs.

        :param documents: List of BioC document XML elements.
        :return: Merged BioC document XML element.
        """
        merged_root = ET.Element("collection")

        # Step 1: Process first document
        normalizer_name = list(self.input_dirs.keys())[0]
        annotation_id = 0
        annotation_id = self._process_annotations(
            documents[0], normalizer_name, annotation_id, True
        )
        merged_root.extend(
            documents[0]
        )  # Start with the structure of the first document

        # Step 2: Merge remaining documents
        for doc_idx, document in enumerate(documents[1:], start=1):
            normalizer_name = list(self.input_dirs.keys())[doc_idx]
            logger.info(f"Merging document from normalizer {normalizer_name}...")
            annotation_id = self._process_annotations(
                document, normalizer_name, annotation_id, False, merged_root
            )

        logger.info("Merging completed.")
        return merged_root

    def _process_annotations(
        self, document, normalizer_name, annotation_id, is_first_doc, merged_root=None
    ):
        """
        Process annotations in a document, filtering and renumbering as needed.

        :param document: XML document to process
        :param normalizer_name: Current normalizer type
        :param annotation_id: Starting annotation ID
        :param is_first_doc: Boolean indicating if this is the first document
        :param merged_root: The merged document root (used for additional documents)
        """
        for passage_idx, passage in enumerate(document.findall(".//passage")):
            if not is_first_doc:
                merged_passage = merged_root.find("document").findall("passage")[
                    passage_idx
                ]
            else:
                merged_passage = passage

            annotations_to_remove = []
            for annotation in passage.findall("annotation"):
                annotation_type = (
                    annotation.find("infon[@key='type']").text
                    if annotation.find("infon[@key='type']") is not None
                    else None
                )

                if self._should_keep_annotation(normalizer_name, annotation_type):
                    annotation.set("id", str(annotation_id))
                    annotation_id += 1
                    if not is_first_doc:
                        merged_passage.append(annotation)
                else:
                    annotations_to_remove.append(annotation)

            # Remove unwanted annotations from the first document
            if is_first_doc:
                for annotation in annotations_to_remove:
                    passage.remove(annotation)

        return annotation_id  # Ensure annotation_id is carried over correctly

    def _should_keep_annotation(self, normalizer_name, annotation_type):
        """
        Determine if an annotation should be kept based on the normalizer type.

        :param normalizer_name: Current normalizer
        :param annotation_type: Type of the annotation
        :return: Boolean indicating whether to keep the annotation
        """
        if normalizer_name == "disease" and annotation_type == "Disease":
            return True
        elif normalizer_name == "chemical" and annotation_type == "Chemical":
            return True
        elif normalizer_name == "cellline" and annotation_type == "CellLine":
            return True
        elif normalizer_name == "tmvar" and annotation_type not in {
            "Chemical",
            "Disease",
            "CellLine",
        }:
            return True
        return False

    def _write_merged_file(self, file_name, merged_document):
        """
        Write the merged BioC document to the output directory.

        :param file_name: Name of the output file.
        :param merged_document: Merged BioC document XML element.
        """
        output_path = self.file_handler.get_file_path(self.output_dir, file_name)
        logger.info(f"Writing merged file to: {output_path}")
        self.file_handler.write_file_as_bioc(output_path, merged_document)


def main():
    """
    Main function to run the BioC file merger.
    """

    logger.info("Execution Started for Processing pipeline")

    parser = argparse.ArgumentParser(
        description="Ingest articles",
        epilog="Example: python3 -m src.data_enrichment.post_processor.py --workflow_id 123abc456def",
    )

    parser.add_argument(
        "--workflow_id",
        "-wid",
        type=str,
        help="Workflow ID of JIT pipeline run",
    )

    parser.add_argument(
        "--source",
        "-src",
        type=str,
        help="Article source (e.g., pmc, ct, rfd etc.)",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    logger.info(
        f"Execution Started for BioC Merger pipeline for workflow_id: {workflow_id}"
    )

    if not args.source:
        logger.error("No source provided. Please provide a valid source.")
        return
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)

    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    merger = BioCFileMerger(
        workflow_id=workflow_id,
        source=source,
        paths_config=paths,
        file_handler=file_handler,
    )
    merger.merge_files()

    logger.info("BioC Merger pipeline completed successfully.")


# Example usage
if __name__ == "__main__":
    main()
