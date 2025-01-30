import os
import xml.etree.ElementTree as ET


def extract_unique_combinations(directory: str):
    unique_combinations = set()

    for filename in os.listdir(directory):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Iterate over all <annotation> elements
            for annotation in root.findall(".//annotation"):
                type_tag = annotation.find('infon[@key="type"]')
                if type_tag is None or type_tag.text is None:
                    continue  # Skip if type is missing or empty

                type_value = type_tag.text.strip()

                # Skip unwanted types
                if type_value in {"Disease", "CellLine", "Chemical"}:
                    continue

                # Find the first <infon> tag that is not "type"
                random_identifier_key = None
                for infon in annotation.findall("infon"):
                    key_attr = infon.get("key", "")
                    if key_attr and key_attr != "type":  # Avoid picking the type field
                        random_identifier_key = (
                            key_attr  # Get the tag name (key attribute)
                        )
                        break  # Stop after finding the first valid key

                if random_identifier_key:
                    unique_combinations.add((type_value, random_identifier_key))

    return unique_combinations


if __name__ == "__main__":
    # Specify the directory containing XML files
    xml_directory_path = "../../data/ner_processed/gnorm2_annotated"

    # Extract unique combinations
    unique_pairs = extract_unique_combinations(xml_directory_path)

    # Print results
    for random_id, entity_type in sorted(unique_pairs):
        print(f"{random_id} {entity_type} ")

    print(unique_pairs)
