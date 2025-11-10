import json
import os
import shutil
import bioc
import pandas as pd
import csv
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

    def exists(self, path: str) -> bool:
        """Checks whether a file or directory exists.

        Args:
            path (str): File or directory path.

        Returns:
            bool: True if exists, False otherwise.
        """
        return os.path.exists(path)

    def copy_file_local_to_s3(self, local_path, s3_path):
        pass

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

    def read_file_bytes(self, file_path):
        """Reads a file and returns its content as bytes."""
        return self.read_file(file_path, as_binary=True)

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

    def write_file_as_bioc(self, file_path: str, bioc_document):
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

            if isinstance(bioc_document, bioc.BioCCollection):
                raw_xml = bioc.dumps(bioc_document)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(raw_xml)
            elif isinstance(bioc_document, ET.Element):
                # Create an ElementTree object and write the XML file
                tree = ET.ElementTree(bioc_document)
                tree.write(file_path, encoding="utf-8", xml_declaration=True)
            else:
                raise TypeError(
                    f"Unsupported type for bioc_document: {type(bioc_document)}"
                )
        except OSError as e:
            raise OSError(f"Error writing BioC file {file_path}: {e}")

    def copy_file(self, src_path: str, dest_path: str, dest_bucket=None) -> None:
        """Copies a file from src_path to dest_path.

        Args:
            src_path (str): The source file path.
            dest_path (str): The destination file path.
            dest_bucket (Not Applicable for local): The destination bucket. Defaults to None.

        Raises:
            FileNotFoundError: If the source file does not exist.
            FileExistsError: If the destination exists and overwrite is False.
            PermissionError: If access is denied.
            OSError: If an error occurs during copying.
        """
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")
        if os.path.exists(dest_path):
            raise FileExistsError(f"Destination file already exists: {dest_path}")
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(src_path, dest_path)
        except PermissionError:
            raise PermissionError(f"Permission denied: {src_path} or {dest_path}")
        except OSError as e:
            raise OSError(f"Error copying file from {src_path} to {dest_path}: {e}")

    def move_file(self, src_path: str, dest_path: str) -> None:
        """Moves a file from src_path to dest_path.

        Args:
            src_path (str): The source file path.
            dest_path (str): The destination file path.

        Raises:
            FileNotFoundError: If the source file does not exist.
            FileExistsError: If the destination exists and overwrite is False.
            PermissionError: If access is denied.
            OSError: If an error occurs during moving.
        """
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")
        # if os.path.exists(dest_path):
        #     raise FileExistsError(f"Destination file already exists: {dest_path}")
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(src_path, dest_path)
        except PermissionError:
            raise PermissionError(f"Permission denied: {src_path} or {dest_path}")
        except OSError as e:
            raise OSError(f"Error moving file from {src_path} to {dest_path}: {e}")

    def delete_file(self, file_path: str) -> None:
        """Deletes a file at the given path.

        Args:
            file_path (str): The file path to delete.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If access is denied.
            OSError: If an error occurs while deleting the file.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            os.remove(file_path)
        except PermissionError:
            raise PermissionError(f"Permission denied: {file_path}")
        except OSError as e:
            raise OSError(f"Error deleting file {file_path}: {e}")

    def read_csv_file(
        self, file_path: str, as_pandas: bool = False, encoding: str = "utf-8"
    ):
        """Reads a CSV file from the local filesystem.

        By default (as_pandas=False) this returns a list of dictionaries (one per row) using
        Python's csv.DictReader which preserves the header as dict keys. If as_pandas=True,
        this method returns a pandas.DataFrame (pandas must be installed).

        Args:
            file_path (str): Path to the CSV file.
            as_pandas (bool): If True, use pandas.read_csv and return a DataFrame. Defaults to False.
            encoding (str): File encoding to use when reading the CSV. Defaults to 'utf-8'.

        Returns:
            list[dict] or pandas.DataFrame: Parsed CSV data.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            PermissionError: If access is denied.
            ImportError: If as_pandas=True but pandas is not installed.
            csv.Error: If there is an error parsing the CSV when using the builtin csv module.
            OSError: For other I/O related errors.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        if as_pandas:
            try:
                return pd.read_csv(file_path)
            except Exception as e:
                # Let pandas raise its own exceptions (e.g., ParserError, EmptyDataError)
                raise OSError(f"Error reading CSV with pandas: {e}")
        else:
            try:
                with open(file_path, "r", encoding=encoding, newline="") as csvfile:
                    reader = csv.DictReader(csvfile)
                    return [row for row in reader]
            except FileNotFoundError:
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            except PermissionError:
                raise PermissionError(f"Permission denied: {file_path}")
            except csv.Error as e:
                raise csv.Error(f"Error parsing CSV file {file_path}: {e}")
            except OSError as e:
                raise OSError(f"Error reading CSV file {file_path}: {e}")
