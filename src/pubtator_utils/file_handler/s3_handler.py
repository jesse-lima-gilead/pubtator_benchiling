import json
from io import BytesIO
from bioc import BioCCollection
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.s3_io_util import S3IOUtil
import bioc


class S3FileHandler(FileHandler):
    """Handles file operations on AWS S3 using S3IOUtil."""

    def __init__(self, platform_type: str):
        self.s3_util = S3IOUtil(platform_type)

    def list_files(self, path):
        """List all file names in the specified S3 path."""
        try:
            full_file_names = self.s3_util.list_files(path)
            return [file_name.split("/")[-1] for file_name in full_file_names]
        except ClientError as e:
            raise Exception(f"Error listing files in {path}: {e}")

    def get_file_path(self, base_path, file_name):
        """Constructs the full file path for an S3 object."""
        return f"{base_path}/{file_name}"

    def write_file(self, file_path, content):
        """Writes raw content to an S3 file."""
        try:
            self.s3_util.upload_file(file_path=file_path, content=content)
        except ClientError as e:
            raise Exception(f"Error writing file {file_path}: {e}")

    def write_file_as_json(self, file_path, content):
        """Writes a dictionary as a JSON file in S3."""
        try:
            json_content = json.dumps(content)
            self.s3_util.upload_file(file_path=file_path, content=json_content)
        except (ClientError, TypeError, ValueError) as e:
            raise Exception(f"Error writing JSON file {file_path}: {e}")

    def parse_xml_file(self, file_path):
        """Parses an XML file from S3 and returns an ElementTree."""
        try:
            xml_content = self.s3_util.download_file(file_path)
            if not xml_content:
                raise ValueError("Empty XML content or failed download.")

            tree = ET.ElementTree(ET.fromstring(xml_content))
            return tree
        except (ClientError, ValueError, ET.ParseError) as e:
            raise Exception(f"Error reading XML file: {e}")

    def read_file(self, file_path):  # check
        """Reads the content of a file from S3."""
        try:
            return self.s3_util.download_file(file_path)
        except ClientError as e:
            raise Exception(f"Error reading file {file_path}: {e}")

    def read_json_file(self, file_path):  # check
        """Reads a JSON file from S3 and returns a dictionary."""
        try:
            content = self.read_file(file_path)
            return json.loads(content) if content else None
        except json.JSONDecodeError as e:
            raise Exception(f"Error decoding JSON: {e}")

    def write_file_as_bioc(self, file_path, bioc_document):
        """Writes a BioCCollection or an XML ElementTree as a BioC file in S3."""
        try:
            with BytesIO() as bio_buffer:
                if isinstance(bioc_document, BioCCollection):
                    xml_str = bioc.dumps(bioc_document)  # returns a str (BioC XML)
                    content = xml_str.encode("utf-8")
                elif isinstance(bioc_document, ET.Element):
                    tree = ET.ElementTree(bioc_document)
                    tree.write(bio_buffer, encoding="utf-8", xml_declaration=True)
                    bio_buffer.seek(0)
                    content = bio_buffer.getvalue()
                else:
                    raise ValueError("Unsupported document type for BioC writing.")

                self.upload_file(file_path=file_path, content=content)
        except (ClientError, ValueError, ET.ParseError) as e:
            raise Exception(f"Error writing BioC file: {e}")

    def write_file_as_bioc(self, file_path, bioc_document):
        """Writes a BioCCollection or an XML ElementTree as a BioC file in S3."""
        try:
            with BytesIO() as bio_buffer:
                if isinstance(bioc_document, BioCCollection):
                    xml_str = bioc.dumps(bioc_document)  # returns a str (BioC XML)
                    content = xml_str.encode("utf-8")
                elif isinstance(bioc_document, ET.Element):
                    tree = ET.ElementTree(bioc_document)
                    tree.write(bio_buffer, encoding="utf-8", xml_declaration=True)
                    bio_buffer.seek(0)
                    content = bio_buffer.getvalue()
                else:
                    raise ValueError("Unsupported document type for BioC writing.")

                self.s3_util.upload_file(file_path=file_path, content=content)
        except (ClientError, ValueError, ET.ParseError) as e:
            raise Exception(f"Error writing BioC file: {e}")

    def copy_file(self, source_key, dest_key, dest_bucket_name):
        """Copies S3 file from one location to another."""
        try:
            self.s3_util.copy_file(
                source_key=source_key,
                dest_bucket_name=dest_bucket_name,
                dest_key=dest_key,
            )
        except ClientError as e:
            raise Exception(f"Error Copy file {dest_bucket_name}:{dest_key}: {e}")

    def move_file(self, source_key, dest_key):
        """Move a file within the S3 bucket."""
        try:
            self.s3_util.move_file(source_key=source_key, dest_key=dest_key)
        except ClientError as e:
            raise Exception(f"Error Moving file {source_key}:{dest_key}: {e}")

    def delete_file(self, file_path):
        """Delete a file from the S3 bucket."""
        try:
            self.s3_util.delete_file(object_name=file_path)
        except ClientError as e:
            raise Exception(f"Error Deleting file {file_path}: {e}")

    def read_csv_file(self, file_path, as_pandas, encoding):
        pass
