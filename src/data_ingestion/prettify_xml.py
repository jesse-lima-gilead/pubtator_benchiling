import os
import xml.etree.ElementTree as ET
from xml.dom.minidom import parseString

from dotenv import set_key

from src.file_handler.base_handler import FileHandler
from src.utils.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class XMLFormatter:
    def __init__(self, folder_path: str, file_handler: FileHandler):
        self.file_handler = file_handler
        self.folder_path = folder_path

    def prettify_xml(self, file_path):
        # Read the content of the XML file
        xml_content = self.file_handler.read_file(file_path)

        # Parse the XML content and prettify it
        try:
            parsed_xml = parseString(xml_content)
            pretty_xml = parsed_xml.toprettyxml(indent="  ")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return

        # Write the prettified XML back to the original file
        self.file_handler.write_file(file_path, pretty_xml)

    def process_folder(self):
        # Loop through all files in the folder
        for filename in self.file_handler.list_files(self.folder_path):
            file_path = self.file_handler.get_file_path(self.folder_path, filename)

            # Only process XML files
            if filename.endswith(".xml"):
                # print(f"Processing: {file_path}")
                self.prettify_xml(file_path)


# Example usage:
if __name__ == "__main__":
    folder_path = (
        # "../../data/gnorm2_annotated/bioformer_annotated"
        "../../data/ner_processed/gnorm2_annotated"
    )
    formatter = XMLFormatter(folder_path)
    formatter.process_folder()
