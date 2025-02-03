import os
import csv
import xml.etree.ElementTree as ET
from pathlib import Path


class BioCFileValidator:
    def __init__(self, input_dirs, output_dir, csv_output_path):
        """
        Initialize the validator with input directories, output directory, and path for the CSV report.

        :param input_dirs: A dictionary where keys are normalizer names and values are directory paths.
        :param output_dir: Path to the output directory containing merged files.
        :param csv_output_path: Path to the CSV output file.
        """
        self.input_dirs = input_dirs
        self.output_dir = Path(output_dir)
        self.csv_output_path = csv_output_path

    def validate_files(self):
        """
        Validate merged files by comparing annotation counts with the source input files.
        Generate a CSV report.
        """
        with open(
            self.csv_output_path, mode="w", newline="", encoding="utf-8"
        ) as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(
                [
                    "file_name",
                    "passage_id",
                    "chemical_count_input_file",
                    "chemical_count_merged_file",
                    "disease_count_input_file",
                    "disease_count_merged_file",
                    "cellLine_count_input_file",
                    "cellLine_count_merged_file",
                    "tmvar_count_input_file",
                    "tmvar_count_merged_file",
                    "total_count_input_file",
                    "total_count_merged_file",
                    "total_count_matches",
                ]
            )

            for merged_file in self.output_dir.glob("*.xml"):
                file_name = merged_file.name
                print(f"Validating file: {file_name}")

                # Parse merged file
                merged_tree = ET.parse(merged_file)
                merged_root = merged_tree.getroot()

                # Get input file trees
                input_trees = {
                    normalizer: ET.parse(self.input_dirs[normalizer] / file_name)
                    for normalizer in self.input_dirs
                }

                for passage_idx, merged_passage in enumerate(
                    merged_root.findall(".//passage")
                ):
                    # Count annotations in merged file
                    chemical_count_merged = self._count_annotations(
                        merged_passage, "Chemical"
                    )
                    disease_count_merged = self._count_annotations(
                        merged_passage, "Disease"
                    )
                    cellLine_count_merged = self._count_annotations(
                        merged_passage, "CellLine"
                    )
                    tmvar_count_merged = self._count_annotations(
                        merged_passage,
                        "TMVar",
                        exclude_types={"Chemical", "Disease", "CellLine"},
                    )
                    total_count_merged = (
                        chemical_count_merged
                        + disease_count_merged
                        + cellLine_count_merged
                        + tmvar_count_merged
                    )

                    # Count annotations in input files
                    chemical_count_input = self._count_annotations_in_inputs(
                        input_trees["chemical"], passage_idx, "Chemical"
                    )
                    disease_count_input = self._count_annotations_in_inputs(
                        input_trees["disease"], passage_idx, "Disease"
                    )
                    cellLine_count_input = self._count_annotations_in_inputs(
                        input_trees["cellline"], passage_idx, "CellLine"
                    )
                    tmvar_count_input = self._count_annotations_in_inputs(
                        input_trees["tmvar"],
                        passage_idx,
                        "TMVar",
                        exclude_types={"Chemical", "Disease", "CellLine"},
                    )
                    total_count_input = (
                        chemical_count_input
                        + disease_count_input
                        + cellLine_count_input
                        + tmvar_count_input
                    )

                    # Compare counts
                    total_count_matches = total_count_input == total_count_merged

                    # Write row to CSV
                    writer.writerow(
                        [
                            file_name,
                            passage_idx + 1,
                            chemical_count_input,
                            chemical_count_merged,
                            disease_count_input,
                            disease_count_merged,
                            cellLine_count_input,
                            cellLine_count_merged,
                            tmvar_count_input,
                            tmvar_count_merged,
                            total_count_input,
                            total_count_merged,
                            total_count_matches,
                        ]
                    )

    def _count_annotations(self, passage, annotation_type, exclude_types=None):
        """
        Count annotations of a specific type in a passage, optionally excluding some types.

        :param passage: The passage element.
        :param annotation_type: The annotation type to count.
        :param exclude_types: A set of types to exclude (only used for TMVar).
        :return: Count of annotations.
        """
        count = 0
        for annotation in passage.findall("annotation"):
            infon = annotation.find("infon[@key='type']")
            if infon is not None:
                annotation_text = infon.text
                if exclude_types and annotation_text in exclude_types:
                    continue
                if annotation_type == "TMVar" and exclude_types is not None:
                    count += 1  # Count everything not excluded
                elif annotation_text == annotation_type:
                    count += 1
        return count

    def _count_annotations_in_inputs(
        self, tree, passage_idx, annotation_type, exclude_types=None
    ):
        """
        Count annotations of a specific type in a passage from input files, optionally excluding some types.

        :param tree: The input file tree.
        :param passage_idx: The index of the passage.
        :param annotation_type: The annotation type to count.
        :param exclude_types: A set of types to exclude (only used for TMVar).
        :return: Count of annotations.
        """
        try:
            passage = tree.findall(".//passage")[passage_idx]
            return self._count_annotations(passage, annotation_type, exclude_types)
        except IndexError:
            return 0


# Example usage
if __name__ == "__main__":
    input_dirs = {
        "disease": Path("./data/ner_processed/taggerone_disease_annotated/"),
        "chemical": Path("./data/ner_processed/nlmchem_annotated/"),
        "cellline": Path("./data/ner_processed/taggerone_cellLine_annotated/"),
        "tmvar": Path("./data/ner_processed/tmvar_annotated/"),
    }
    output_dir = "./data/ner_processed/annotations_merged/"
    csv_output_path = "./data/validation_report.csv"

    validator = BioCFileValidator(input_dirs, output_dir, csv_output_path)
    validator.validate_files()
