import xml.etree.ElementTree as ET


class PostProcessor:
    def __init__(self):
        self.file_path = None

    def set_file_path(self, file_path):
        self.file_path = file_path

    def process_file(self):
        # Parse the XML file
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        # Iterate through all <passage> tags
        for passage in root.findall(".//passage"):
            passage_text = passage.find("text").text
            if not passage_text:
                continue

            # Iterate through all <annotation> tags within each <passage>
            for annotation in passage.findall("annotation"):
                annotation_text_elem = annotation.find("text")

                # Check if the <text> tag is missing or empty
                if annotation_text_elem is None or not annotation_text_elem.text:
                    location = annotation.find("location")
                    if location is not None:
                        offset = int(location.get("offset", 0))
                        length = int(location.get("length", 0))

                        # Extract the corresponding text from the <passage> text using offset and length
                        extracted_text = passage_text[offset : offset + length]
                        print(extracted_text)

                        # Update or create the <text> tag inside <annotation>
                        if annotation_text_elem is None:
                            annotation_text_elem = ET.SubElement(annotation, "text")

                        annotation_text_elem.text = extracted_text

        # Overwrite the original file with the updated XML structure
        tree.write(self.file_path, encoding="utf-8", xml_declaration=True)
        print(f"File '{self.file_path}' has been updated successfully.")


if __name__ == "__main__":
    file_path = "../../test_data/aioner_annotated/pubmedbert_annotated/PMC_10483081.xml"
    post_processor = PostProcessor()
    post_processor.set_file_path(file_path)
    post_processor.process_file()
