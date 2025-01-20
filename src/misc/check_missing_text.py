import os

import os
import xml.etree.ElementTree as ET


def count_empty_text_tags_in_xml(xml_content):
    """
    Counts the number of empty <text></text> tags in the given XML content.
    """
    # Parse the XML content
    tree = ET.ElementTree(ET.fromstring(xml_content))

    # Find all <text> tags and check if they are empty
    count = 0
    total_count = 0
    for text_element in tree.findall(".//text"):
        total_count += 1
        if not text_element.text or text_element.text.strip() == "":
            count += 1
    return count, total_count


def count_empty_text_tags_in_dir(directory):
    """
    Counts the occurrences of empty <text></text> tags in all XML files in a directory.
    """
    overall_text_tag_total_count = 0
    overall_empty_text_tag_count = 0
    for filename in os.listdir(directory):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                xml_content = file.read()
                (
                    empty_text_tag_count,
                    total_text_tags_count,
                ) = count_empty_text_tags_in_xml(xml_content)
                print(
                    f"{filename}: Total {total_text_tags_count} <text> tags and {empty_text_tag_count} empty <text></text> tags"
                    f"\tPercent Empty tags ({empty_text_tag_count / total_text_tags_count * 100:.2f}%)"
                )
                overall_empty_text_tag_count += empty_text_tag_count
                overall_text_tag_total_count += total_text_tags_count

    print(
        f"\nTotal Text tags {overall_text_tag_total_count}"
        f"\nTotal empty <text></text> tags in all files: {overall_empty_text_tag_count}"
        f"\nPercent Empty tags ({overall_empty_text_tag_count / overall_text_tag_total_count * 100:.2f}%)"
    )


if __name__ == "__main__":
    aioner_annotated_path = (
        "../../data/enhanced_golden_dataset/ner_processed/aioner_annotated/"
        # "/Users/ishaanbhatnagar/Downloads/data/golden_dataset/ner_processed/aioner_annotated/"
        # "/Users/ishaanbhatnagar/Downloads/37_extra_aioner2/"
    )
    gnorm2_annotated_path = (
        "../../data/enhanced_golden_dataset/ner_processed/gnorm2_annotated/"
        # "/Users/ishaanbhatnagar/Downloads/data/golden_dataset/ner_processed/gnorm2_annotated/"
    )
    count_empty_text_tags_in_dir(aioner_annotated_path)
