import os
import xml.etree.ElementTree as ET
from xml.dom.minidom import parseString


class XMLFormatter:
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def prettify_xml(self, file_path):
        # Read the content of the XML file
        with open(file_path, "r", encoding="utf-8") as file:
            xml_content = file.read()

        # Parse the XML content and prettify it
        try:
            parsed_xml = parseString(xml_content)
            pretty_xml = parsed_xml.toprettyxml(indent="  ")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return

        # Write the prettified XML back to the original file
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(pretty_xml)

    def process_folder(self):
        # Loop through all files in the folder
        for filename in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, filename)

            # Only process XML files
            if filename.endswith(".xml"):
                print(f"Processing: {file_path}")
                self.prettify_xml(file_path)


# Example usage:
if __name__ == "__main__":
    folder_path = (
        # "../../data/gnorm2_annotated/bioformer_annotated"
        "../../data/gnorm2_annotated/pubmedbert_annotated"
    )
    formatter = XMLFormatter(folder_path)
    formatter.process_folder()
