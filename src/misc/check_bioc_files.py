# import json
#
# import bioc
#
#
# def compare_bioc_files(file_path1, file_path2):
#     """
#     Compare two BioC XML files to check if:
#     1. The number of passages in both files are equal.
#     2. The number of annotations within each passage are equal.
#
#     Args:
#         file_path1 (str): Path to the first BioC XML file.
#         file_path2 (str): Path to the second BioC XML file.
#
#     Returns:
#         dict: A dictionary with comparison results.
#     """
#     # Read the BioC files
#     with open(file_path1, 'r', encoding='utf-8') as f1, open(file_path2, 'r', encoding='utf-8') as f2:
#         bioc_doc1 = bioc.load(f1)
#         bioc_doc2 = bioc.load(f2)
#
#     # Extract passages from both documents
#     passages1 = [p for doc in bioc_doc1.documents for p in doc.passages]
#     passages2 = [p for doc in bioc_doc2.documents for p in doc.passages]
#
#     # Compare the number of passages
#     num_passages1 = len(passages1)
#     num_passages2 = len(passages2)
#     passages_equal = num_passages1 == num_passages2
#
#     # Prepare detailed comparison results for annotations
#     annotation_details = []
#     annotations_equal = True
#
#     if passages_equal:
#         for i, (p1, p2) in enumerate(zip(passages1, passages2)):
#             num_annotations1 = len(p1.annotations)
#             num_annotations2 = len(p2.annotations)
#             annotation_details.append({
#                 "passage_index": i,
#                 "num_annotations_file1": num_annotations1,
#                 "num_annotations_file2": num_annotations2,
#                 "annotations_equal": num_annotations1 == num_annotations2
#             })
#             if num_annotations1 != num_annotations2:
#                 annotations_equal = False
#
#     # Prepare the overall result
#     result = {
#         "num_passages_equal": passages_equal,
#         "num_passages_file1": num_passages1,
#         "num_passages_file2": num_passages2,
#         "annotations_equal": annotations_equal,
#         "annotation_details": annotation_details,
#     }
#
#     return result
#
#
# # Example Usage
# file_path1 = "../../data/golden_dataset/ner_processed/gnorm2_annotated/PMC_4244293.xml"
# file_path2 = "../../test_data/golden_dataset/ner_processed/gnorm2_annotated/PMC_4244293.xml"
#
#
# comparison_result = compare_bioc_files(file_path1, file_path2)
# print(json.dumps(comparison_result, indent=4))

import os
import csv
from bioc import loads  # Assuming BioC is properly installed


def compare_bioc_files(dir1, dir2, output_csv):
    # Prepare list of files common to both directories
    common_files = set(os.listdir(dir1)) & set(os.listdir(dir2))
    comparison_results = []
    # print(os.listdir(dir1))
    # print(os.listdir(dir2))

    for filename in common_files:
        file1_path = os.path.join(dir1, filename)
        file2_path = os.path.join(dir2, filename)

        with open(file1_path, 'r', encoding='utf-8') as f1, open(file2_path, 'r', encoding='utf-8') as f2:
            bioc1 = loads(f1.read())
            bioc2 = loads(f2.read())

        # Compare passages
        num_passages_file1 = len(bioc1.documents[0].passages)
        num_passages_file2 = len(bioc2.documents[0].passages)
        num_passages_equal = num_passages_file1 == num_passages_file2

        annotation_details = []
        annotations_equal = True
        total_annotations_file1 = 0
        total_annotations_file2 = 0
        total_annotation_mismatches = 0

        for i, (passage1, passage2) in enumerate(zip(bioc1.documents[0].passages, bioc2.documents[0].passages)):
            num_annotations_file1 = len(passage1.annotations)
            num_annotations_file2 = len(passage2.annotations)
            passage_annotations_equal = num_annotations_file1 == num_annotations_file2

            # Calculate annotation mismatches
            annotation_mismatch = abs(num_annotations_file1 - num_annotations_file2)
            total_annotation_mismatches += annotation_mismatch
            total_annotations_file1+= num_annotations_file1
            total_annotations_file2+= num_annotations_file2

            if not passage_annotations_equal:
                annotations_equal = False

            annotation_details.append({
                "passage_index": i,
                "num_annotations_file1": num_annotations_file1,
                "num_annotations_file2": num_annotations_file2,
                "annotations_equal": passage_annotations_equal
            })

        comparison_results.append({
            "filename": filename,
            "num_passages_equal": num_passages_equal,
            "num_passages_file1": num_passages_file1,
            "num_passages_file2": num_passages_file2,
            "annotations_equal": annotations_equal,
            "total_annotations_file1": total_annotations_file1,
            "total_annotations_file2": total_annotations_file2,
            "total_annotation_mismatches": total_annotation_mismatches,
            "annotation_details": annotation_details
        })

    # Write results to a CSV file
    with open(output_csv, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Filename",
            "Passages Equal",
            "Passages Count (File1)",
            "Passages Count (File2)",
            "Annotations Equal",
            "Annotations Count (File1)",
            "Annotations Count (File2)",
            "Total Annotation Mismatches",
            "Mismatch Details"
        ])

        for result in comparison_results:
            mismatch_details = "; ".join([
                f"Passage {detail['passage_index']}: {detail['num_annotations_file1']} vs {detail['num_annotations_file2']}"
                for detail in result["annotation_details"] if not detail["annotations_equal"]
            ])
            writer.writerow([
                result["filename"],
                result["num_passages_equal"],
                result["num_passages_file1"],
                result["num_passages_file2"],
                result["annotations_equal"],
                result["total_annotations_file1"],
                result["total_annotations_file2"],
                result["total_annotation_mismatches"],
                mismatch_details
            ])

    print(f"Comparison results saved to {output_csv}")

dir1 = "../../data/golden_dataset/ner_processed/gnorm2_annotated"
dir2 = "../../test_data/golden_dataset/ner_processed/gnorm2_annotated"
output = "../../test_data/golden_dataset/comparison_results.csv"
# Example usage
compare_bioc_files(dir1, dir2, output)
