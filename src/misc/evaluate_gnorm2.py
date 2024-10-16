import os
import re
from collections import Counter, defaultdict
import xml.etree.ElementTree as ET


def calculate_metrics(annotation_check):
    annotation_count = annotation_check["annotation_count"]
    incorrect_text = annotation_check["incorrect_text"]
    wrong_species_geneid = annotation_check["wrong_species_geneid"]
    partial_annotation = annotation_check["partial_annotation"]
    combined_case = annotation_check["combined_case"]
    missed_annotations = annotation_check["missed_annotations"]

    # Calculate Correct Annotations (CA)
    correct_annotations = annotation_count - (
        incorrect_text + wrong_species_geneid + partial_annotation + combined_case
    )

    # Calculate metrics
    annotation_accuracy = (correct_annotations / annotation_count) * 100
    error_rate = (
        (incorrect_text + wrong_species_geneid + partial_annotation + combined_case)
        / annotation_count
    ) * 100
    missed_annotation_rate = (
        (missed_annotations / (annotation_count + missed_annotations)) * 100
        if missed_annotations > 0
        else 0
    )
    precision = (
        (correct_annotations / (correct_annotations + missed_annotations)) * 100
        if correct_annotations + missed_annotations > 0
        else 0
    )
    recall = (
        (correct_annotations / (annotation_count + missed_annotations)) * 100
        if annotation_count + missed_annotations > 0
        else 0
    )

    return {
        "correct_annotations": correct_annotations,
        "annotation_accuracy": annotation_accuracy,
        "error_rate": error_rate,
        "missed_annotation_rate": missed_annotation_rate,
        "precision": precision,
        "recall": recall,
    }


def check_annotations(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Initialize the count of annotations
    annotation_count = 0
    incorrect_text = 0
    wrong_species_geneid = 0
    partial_annotation = 0
    combined_case = 0
    missed_annotations = 0

    # Iterate through the XML to find all <annotation> tags
    for annotation in root.findall(".//annotation"):
        annotation_count += 1

        text_tag = annotation.find("text")

        if text_tag is not None:
            text_content = text_tag.text

            # Check for special symbols
            has_hash = "#" in text_content
            has_percent = "%" in text_content
            has_dollar = "$" in text_content

            # Classify based on the symbols
            if has_hash and not (has_percent or has_dollar):
                incorrect_text += 1
            elif has_percent and not has_hash and not has_dollar:
                wrong_species_geneid += 1
            elif has_dollar and not has_hash and not has_percent:
                partial_annotation += 1
            elif any([has_hash, has_percent, has_dollar]):
                combined_case += 1

    # Now check <text> tags for missed annotations (words starting with '?')
    for text_tag in root.findall(".//text"):
        text_content = text_tag.text

        if text_content:
            # Use regex to find words starting with '?'
            words_with_question_mark = re.findall(r"\?\w+", text_content)
            missed_annotations += len(words_with_question_mark)

    return {
        "annotation_count": annotation_count,
        "incorrect_text": incorrect_text,
        "wrong_species_geneid": wrong_species_geneid,
        "partial_annotation": partial_annotation,
        "combined_case": combined_case,
        "missed_annotations": missed_annotations,
    }


def write_report(
    file_reports, bioformer_overall_accuracy, pubmedbert_overall_accuracy, output_dir
):
    # Initialize an empty report string
    combined_report = "========== Combined Annotations Report ==========\n\n"

    # Loop through each file report and append it to the combined report
    for report in file_reports:
        file_name = report["file_name"]
        model = report["model"]
        annotation_check = report["annotation_check"]
        accuracy_metrices = report["accuracy_metrices"]
        annotations_by_type = report["annotations_by_type"]

        # Generate the file-specific report
        file_report_path = f"{output_dir}/{file_name.split('.')[0]}_{model}_report.txt"
        with open(file_report_path, "w") as file:
            # Header
            file.write(f"========== Annotations Report for {file_name} ==========\n")
            file.write(f"Model: {model}\n")
            file.write("=========================================\n\n")

            # Annotation counts and metrics
            file.write(f"Annotation Summary:\n-------------------\n")
            file.write(
                f"Total Annotations (Count): {annotation_check['annotation_count']}\n"
            )
            file.write(f"Incorrect Text (#): {annotation_check['incorrect_text']}\n")
            file.write(
                f"Wrong Species/GeneID (%): {annotation_check['wrong_species_geneid']}\n"
            )
            file.write(
                f"Partial Annotation ($): {annotation_check['partial_annotation']}\n"
            )
            file.write(
                f"Combined Cases (Multiple Symbols): {annotation_check['combined_case']}\n"
            )
            file.write(
                f"Missed Annotations (?): {annotation_check['missed_annotations']}\n\n"
            )

            # Accuracy metrics
            file.write(f"========== Accuracy Metrics ==========\n")
            file.write(
                f"Correct Annotations (CA): {accuracy_metrices['correct_annotations']}\n"
            )
            file.write(
                f"Annotation Accuracy (AA): {accuracy_metrices['annotation_accuracy']:.2f}%\n"
            )
            file.write(f"Error Rate (ER): {accuracy_metrices['error_rate']:.2f}%\n")
            file.write(
                f"Missed Annotation Rate (MAR): {accuracy_metrices['missed_annotation_rate']:.2f}%\n"
            )
            file.write(f"Precision (P): {accuracy_metrices['precision']:.2f}%\n")
            file.write(f"Recall (R): {accuracy_metrices['recall']:.2f}%\n")
            file.write("=========================================\n\n")

            # Unique annotations and frequency (sorted in descending order)
            file.write("============= Unique Annotations ==============\n")
            for annotation_type, annotation_counter in sorted(
                annotations_by_type.items()
            ):
                file.write("\n=============")
                file.write(f"\nType: {annotation_type}\n")
                file.write("=============\n")
                for text, count in annotation_counter.most_common():
                    file.write(f"{text}: {count}\n")
                file.write("=============\n")
            file.write("\n=========================================\n\n")

    # Write the overall accuracy report for Bioformer
    bioformer_overall_report_path = (
        f"{output_dir}/bioformer_overall_accuracy_report.txt"
    )
    with open(bioformer_overall_report_path, "w") as overall_file:
        overall_file.write(
            "========== Overall Accuracy Report for Bioformer ==========\n\n"
        )
        overall_file.write(
            f"Total Correct Annotations: {bioformer_overall_accuracy['total_correct_annotations']}\n"
        )
        overall_file.write(
            f"Total Annotation Count: {bioformer_overall_accuracy['total_annotation_count']}\n"
        )
        overall_file.write(
            f"Overall Annotation Accuracy: {bioformer_overall_accuracy['overall_annotation_accuracy']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Error Rate: {bioformer_overall_accuracy['overall_error_rate']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Precision: {bioformer_overall_accuracy['overall_precision']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Recall: {bioformer_overall_accuracy['overall_recall']:.2f}%\n"
        )
        overall_file.write("=============================================\n")

    print(
        f"Overall Bioformer report successfully written to {bioformer_overall_report_path}"
    )

    # Write the overall accuracy report
    pubmedbert_overall_report_path = (
        f"{output_dir}/pubmedbert_overall_accuracy_report.txt"
    )
    with open(pubmedbert_overall_report_path, "w") as overall_file:
        overall_file.write(
            "========== Overall Accuracy Report for Pubmedbert ==========\n\n"
        )
        overall_file.write(
            f"Total Correct Annotations: {pubmedbert_overall_accuracy['total_correct_annotations']}\n"
        )
        overall_file.write(
            f"Total Annotation Count: {pubmedbert_overall_accuracy['total_annotation_count']}\n"
        )
        overall_file.write(
            f"Overall Annotation Accuracy: {pubmedbert_overall_accuracy['overall_annotation_accuracy']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Error Rate: {pubmedbert_overall_accuracy['overall_error_rate']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Precision: {pubmedbert_overall_accuracy['overall_precision']:.2f}%\n"
        )
        overall_file.write(
            f"Overall Recall: {pubmedbert_overall_accuracy['overall_recall']:.2f}%\n"
        )
        overall_file.write("=============================================\n")

    print(
        f"Overall Pubmedebert report successfully written to {pubmedbert_overall_report_path}"
    )


# Function to clean and extract text from <text> tag
def clean_text(text):
    # Remove special characters (#, $, %)
    cleaned_text = re.sub(r"[#\$%]", "", text).strip()
    return cleaned_text


# Function to parse BioC XML and count unique cleaned annotations
def get_annotations_by_type(file_path):
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Initialize a defaultdict to store counters for each type
    annotations_by_type = defaultdict(Counter)

    # Find all <annotation> elements
    for annotation in root.findall(".//annotation"):
        # Get the <infon key="type"> element to categorize the annotation
        infon_type = annotation.find(".//infon[@key='type']")
        text_element = annotation.find("text")

        if infon_type is not None and text_element is not None:
            # Extract type and clean the text
            annotation_type = infon_type.text.strip()
            original_text = text_element.text.strip()
            cleaned_text = clean_text(original_text)

            # Only add non-empty cleaned text to the corresponding type counter
            if cleaned_text:
                annotations_by_type[annotation_type][cleaned_text] += 1

    return annotations_by_type


def process_files(file_reports, output_dir):
    bioformer_total_correct_annotations = 0
    bioformer_total_annotation_count = 0
    bioformer_total_incorrect_text = 0
    bioformer_total_missed_annotations = 0

    pubmedbert_total_correct_annotations = 0
    pubmedbert_total_annotation_count = 0
    pubmedbert_total_incorrect_text = 0
    pubmedbert_total_missed_annotations = 0

    # Iterate through all file reports
    for report in file_reports:
        annotation_check = report["annotation_check"]
        accuracy_metrices = report["accuracy_metrices"]
        model = report["model"]

        if model == "bioformer":
            bioformer_total_correct_annotations += accuracy_metrices[
                "correct_annotations"
            ]
            bioformer_total_annotation_count += annotation_check["annotation_count"]
            bioformer_total_incorrect_text += annotation_check["incorrect_text"]
            bioformer_total_missed_annotations += annotation_check["missed_annotations"]
        elif model == "pubmedbert":
            pubmedbert_total_correct_annotations += accuracy_metrices[
                "correct_annotations"
            ]
            pubmedbert_total_annotation_count += annotation_check["annotation_count"]
            pubmedbert_total_incorrect_text += annotation_check["incorrect_text"]
            pubmedbert_total_missed_annotations += annotation_check[
                "missed_annotations"
            ]

    # Calculate overall accuracy metrics for Bioformer
    bioformer_overall_annotation_accuracy = (
        (bioformer_total_correct_annotations / bioformer_total_annotation_count) * 100
        if bioformer_total_annotation_count > 0
        else 0
    )
    bioformer_overall_error_rate = (
        (
            (bioformer_total_incorrect_text + bioformer_total_missed_annotations)
            / bioformer_total_annotation_count
        )
        * 100
        if bioformer_total_annotation_count > 0
        else 0
    )
    bioformer_overall_precision = (
        (
            bioformer_total_correct_annotations
            / (bioformer_total_correct_annotations + bioformer_total_incorrect_text)
        )
        * 100
        if (bioformer_total_correct_annotations + bioformer_total_incorrect_text) > 0
        else 0
    )
    bioformer_overall_recall = (
        (bioformer_total_correct_annotations / bioformer_total_annotation_count) * 100
        if bioformer_total_annotation_count > 0
        else 0
    )

    # Calculate overall accuracy metrics for PubMedBERT
    pubmedbert_overall_annotation_accuracy = (
        (pubmedbert_total_correct_annotations / pubmedbert_total_annotation_count) * 100
        if pubmedbert_total_annotation_count > 0
        else 0
    )
    pubmedbert_overall_error_rate = (
        (
            (pubmedbert_total_incorrect_text + pubmedbert_total_missed_annotations)
            / pubmedbert_total_annotation_count
        )
        * 100
        if pubmedbert_total_annotation_count > 0
        else 0
    )
    pubmedbert_overall_precision = (
        (
            pubmedbert_total_correct_annotations
            / (pubmedbert_total_correct_annotations + pubmedbert_total_incorrect_text)
        )
        * 100
        if (pubmedbert_total_correct_annotations + pubmedbert_total_incorrect_text) > 0
        else 0
    )
    pubmedbert_overall_recall = (
        (pubmedbert_total_correct_annotations / pubmedbert_total_annotation_count) * 100
        if pubmedbert_total_annotation_count > 0
        else 0
    )

    # Overall accuracy report dictionary for bioformer
    bioformer_overall_accuracy = {
        "total_correct_annotations": bioformer_total_correct_annotations,
        "total_annotation_count": bioformer_total_annotation_count,
        "overall_annotation_accuracy": bioformer_overall_annotation_accuracy,
        "overall_error_rate": bioformer_overall_error_rate,
        "overall_precision": bioformer_overall_precision,
        "overall_recall": bioformer_overall_recall,
    }

    # Overall accuracy report dictionary for pubmedbert
    pubmedbert_overall_accuracy = {
        "total_correct_annotations": pubmedbert_total_correct_annotations,
        "total_annotation_count": pubmedbert_total_annotation_count,
        "overall_annotation_accuracy": pubmedbert_overall_annotation_accuracy,
        "overall_error_rate": pubmedbert_overall_error_rate,
        "overall_precision": pubmedbert_overall_precision,
        "overall_recall": pubmedbert_overall_recall,
    }

    # Generate and write the report for each file and overall
    write_report(
        file_reports,
        bioformer_overall_accuracy,
        pubmedbert_overall_accuracy,
        output_dir,
    )


if __name__ == "__main__":
    model_list = ["bioformer", "pubmedbert"]
    validation_dir_path = "../../data/validation/gnorm2_annotated"
    file_reports = []

    # Create Reports per File
    for model in model_list:
        for file_name in os.listdir(f"{validation_dir_path}/{model}_annotated"):
            if file_name.endswith(".xml"):
                file_path = f"{validation_dir_path}/{model}_annotated/{file_name}"
                print(file_path)

                annotation_check = check_annotations(file_path)

                accuracy_metrices = calculate_metrics(annotation_check)

                annotations_by_type = get_annotations_by_type(file_path)

                file_reports.append(
                    {
                        "file_name": file_name,
                        "model": model,
                        "annotation_check": annotation_check,
                        "accuracy_metrices": accuracy_metrices,
                        "annotations_by_type": annotations_by_type,
                    }
                )

                # print(f"Annotations report for file {file_name}: \n"
                #       f"annotation_count: {annotation_check["annotation_count"]}\n"
                #       f"incorrect_text: {annotation_check["incorrect_text"]}\n"
                #       f"wrong_species_geneid: {annotation_check["wrong_species_geneid"]}\n"
                #       f"partial_annotation: {annotation_check["partial_annotation"]}\n"
                #       f"combined_case: {annotation_check["combined_case"]}\n"
                #       f"missed_annotations: {annotation_check["missed_annotations"]}\n\n"
                #       f"Accuracy Report for model {model} on the file:\n"
                #       f"Correct Annotations (CA): {accuracy_metrices["correct_annotations"]}\n"
                #       f"Annotation Accuracy (AA): {accuracy_metrices["annotation_accuracy"]:.2f}%\n"
                #       f"Error Rate (ER): {accuracy_metrices["error_rate"]:.2f}%\n"
                #       f"Missed Annotation Rate (MAR): {accuracy_metrices["missed_annotation_rate"]:.2f}%\n"
                #       f"Precision (P): {accuracy_metrices["precision"]:.2f}%\n"
                #       f"Recall (R): {accuracy_metrices["recall"]:.2f}%\n\n"
                #       )

    output_dir = "../../data/validation/results/"
    process_files(file_reports, output_dir)
