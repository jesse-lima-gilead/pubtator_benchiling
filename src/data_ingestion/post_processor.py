import os
import xml.etree.ElementTree as ET
from pathlib import Path


class BioCFileMerger:
    def __init__(self, input_dirs, output_dir):
        """
        Initialize the merger with input directories for normalizers and an output directory.

        :param input_dirs: A dictionary where keys are normalizer names and values are directory paths.
        :param output_dir: Path to the output directory.
        """
        self.input_dirs = input_dirs
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def merge_files(self):
        """
        Merge files from all input directories and write the combined output to the output directory.
        """
        file_names = self._get_common_file_names()
        print(f"Found common files: {file_names}")
        for file_name in file_names:
            print(f"Processing file: {file_name}")
            documents = [
                self._parse_bioc_file(self.input_dirs[normalizer] / file_name)
                for normalizer in self.input_dirs
            ]
            merged_document = self._merge_documents(documents)
            self._write_merged_file(file_name, merged_document)
            print(f"Merged file written: {file_name}")

    def _get_common_file_names(self):
        """
        Get the common file names across all input directories.

        :return: A set of common file names.
        """
        file_sets = [
            set(os.listdir(directory)) for directory in self.input_dirs.values()
        ]
        common_files = set.intersection(*file_sets)
        print(f"Common files across directories: {common_files}")
        return common_files

    def _parse_bioc_file(self, file_path):
        """
        Parse a BioC XML file and return its root element.

        :param file_path: Path to the BioC XML file.
        :return: Root element of the parsed XML tree.
        """
        print(f"Parsing file: {file_path}")
        tree = ET.parse(file_path)
        return tree.getroot()

    def _merge_documents(self, documents):
        """
        Merge multiple BioC documents into one.

        :param documents: List of BioC document XML elements.
        :return: Merged BioC document XML element.
        """
        merged_root = ET.Element("collection")
        merged_root.extend(
            documents[0]
        )  # Start with the structure of the first document

        annotation_id = 0
        for doc_idx, document in enumerate(documents):
            if doc_idx == 0:
                continue

            normalizer_name = list(self.input_dirs.keys())[doc_idx]
            print(f"Merging document from normalizer {normalizer_name}...")

            for passage_idx, passage in enumerate(document.findall(".//passage")):
                print(f"Processing passage {passage_idx}...")
                merged_passage = merged_root.find("document").findall("passage")[
                    passage_idx
                ]

                for annotation in passage.findall("annotation"):
                    annotation_type = (
                        annotation.find("infon[@key='type']").text
                        if annotation.find("infon[@key='type']") is not None
                        else None
                    )

                    # Filter annotations based on the normalizer
                    if normalizer_name == "disease" and annotation_type == "Disease":
                        annotation.set("id", str(annotation_id))
                        annotation_id += 1
                        merged_passage.append(annotation)
                    elif (
                        normalizer_name == "chemical" and annotation_type == "Chemical"
                    ):
                        annotation.set("id", str(annotation_id))
                        annotation_id += 1
                        merged_passage.append(annotation)
                    elif (
                        normalizer_name == "cellline" and annotation_type == "CellLine"
                    ):
                        annotation.set("id", str(annotation_id))
                        annotation_id += 1
                        merged_passage.append(annotation)
                    elif normalizer_name == "tmvar" and annotation_type not in {
                        "Chemical",
                        "Disease",
                        "CellLine",
                    }:
                        annotation.set("id", str(annotation_id))
                        annotation_id += 1
                        merged_passage.append(annotation)

        print("Merging completed.")
        return merged_root

    def _write_merged_file(self, file_name, merged_document):
        """
        Write the merged BioC document to the output directory.

        :param file_name: Name of the output file.
        :param merged_document: Merged BioC document XML element.
        """
        output_path = self.output_dir / file_name
        print(f"Writing merged file to: {output_path}")
        tree = ET.ElementTree(merged_document)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)


# Example usage
if __name__ == "__main__":
    input_dirs = {
        "disease": Path("./data/ner_processed/taggerone_disease_annotated/"),
        "chemical": Path("./data/ner_processed/nlmchem_annotated"),
        "cellline": Path("./data/ner_processed/taggerone_cellLine_annotated"),
        "tmvar": Path("./data/ner_processed/tmvar_annotated"),
    }
    output_dir = "./data/ner_processed/annotations_merged"

    merger = BioCFileMerger(input_dirs, output_dir)
    merger.merge_files()
