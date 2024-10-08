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
    for text_element in tree.findall(".//text"):
        if not text_element.text or text_element.text.strip() == "":
            count += 1
    return count


def count_empty_text_tags_in_dir(directory):
    """
    Counts the occurrences of empty <text></text> tags in all XML files in a directory.
    """
    total_count = 0
    for filename in os.listdir(directory):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                xml_content = file.read()
                count = count_empty_text_tags_in_xml(xml_content)
                print(f"{filename}: {count} empty <text></text> tags")
                total_count += count

    print(f"Total empty <text></text> tags in all files: {total_count}")


if __name__ == "__main__":
    aioner_annotated_path = (
        "../../data/gilead_pubtator_results/aioner_annotated/bioformer_annotated/"
    )
    gnorm2_annotated_path = (
        "../../data/gilead_pubtator_results/gnorm2_annotated/bioformer_annotated/"
    )
    count_empty_text_tags_in_dir(gnorm2_annotated_path)
