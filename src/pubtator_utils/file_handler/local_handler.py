import json
import os
from typing import Union, Any
from xml.etree import ElementTree as ET
from src.pubtator_utils.file_handler.base_handler import FileHandler


class LocalFileHandler(FileHandler):
    """Handles file operations on the local filesystem."""

    def list_files(self, path: str) -> list[str]:
        """Lists all files in the given directory.

        Args:
            path (str): The directory path.

        Returns:
            list[str]: A list of file names.

        Raises:
            FileNotFoundError: If the directory does not exist.
            PermissionError: If access is denied.
        """
        try:
            return [
                f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))
            ]
        except FileNotFoundError:
            raise FileNotFoundError(f"Directory not found: {path}")
        except PermissionError:
            raise PermissionError(f"Permission denied: {path}")

    def get_file_path(self, base_path: str, file_name: str) -> str:
        """Constructs a full file path from a base directory and file name."""
        return os.path.join(base_path, file_name)

    def write_file(self, file_path: str, content: Union[str, bytes]) -> None:
        """Writes content to a local file.

        - If `content` is a string, writes using `"w"` mode with UTF-8 encoding.
        - If `content` is bytes, writes using `"wb"` mode.

        Args:
            file_path (str): The local file path to write to.
            content (str or bytes): The content to be written.

        Raises:
            OSError: If an error occurs while writing the file.
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            mode = "w" if isinstance(content, str) else "wb"
            encoding = "utf-8" if mode == "w" else None

            with open(file_path, mode, encoding=encoding) as f:
                f.write(content)
        except OSError as e:
            raise OSError(f"Error writing file {file_path}: {e}")

    def write_file_as_json(self, file_path: str, content: Any) -> None:
        """Writes content to a file in JSON format."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(content, json_file, indent=4, ensure_ascii=False)
        except TypeError as e:
            raise TypeError(f"Content is not JSON serializable: {e}")
        except OSError as e:
            raise OSError(f"Error writing JSON file {file_path}: {e}")

    def read_file(self, file_path: str, as_binary: bool = False) -> Union[str, bytes]:
        """Reads a file from the local filesystem.

        Args:
            file_path (str): The local file path to read from.
            as_binary (bool, optional): If True, reads the file as bytes. Defaults to False.

        Returns:
            Union[str, bytes]: The content of the file.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If access is denied.
            OSError: If an error occurs while reading the file.
        """
        mode = "rb" if as_binary else "r"
        encoding = None if as_binary else "utf-8"

        try:
            with open(file_path, mode, encoding=encoding) as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except PermissionError:
            raise PermissionError(f"Permission denied: {file_path}")
        except OSError as e:
            raise OSError(f"Error reading file {file_path}: {e}")

    def parse_xml_file(self, file_path: str) -> ET.ElementTree:
        """Parses an XML file and returns an ElementTree object."""
        try:
            return ET.parse(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"XML file not found: {file_path}")
        except ET.ParseError as e:
            raise ET.ParseError(f"Error parsing XML file {file_path}: {e}")

    def read_json_file(self, file_path: str) -> dict:
        """Reads a JSON file and returns its content as a dictionary.

        Args:
            file_path (str): The local file path to read from.

        Returns:
            dict: The JSON content as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If access is denied.
            json.JSONDecodeError: If the file is not valid JSON.
            OSError: If an error occurs while reading the file.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as json_file:
                return json.load(json_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        except PermissionError:
            raise PermissionError(f"Permission denied: {file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Error decoding JSON file {file_path}: {e}", doc=e.doc, pos=e.pos
            )
        except OSError as e:
            raise OSError(f"Error reading JSON file {file_path}: {e}")

    def write_file_as_bioc(self, file_path: str, bioc_document: ET.Element):
        """Writes a BioC XML document to a file.

        Args:
            file_path (str): The file path where the BioC XML document should be saved.
            merged_document (ET.Element): The root element of the BioC XML document.

        Raises:
            OSError: If an error occurs while writing the file.
        """
        try:
            # Ensure the output directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Create an ElementTree object and write the XML file
            tree = ET.ElementTree(bioc_document)
            tree.write(file_path, encoding="utf-8", xml_declaration=True)
        except OSError as e:
            raise OSError(f"Error writing BioC file {file_path}: {e}")
