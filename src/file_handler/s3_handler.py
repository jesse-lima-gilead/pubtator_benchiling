import os
from .base_handler import FileHandler
from src.utils.s3_io_util import S3IOUtil


class S3FileHandler(FileHandler):
    """Handles file operations on AWS S3 using S3IOUtil."""

    def __init__(self):
        self.s3_util = S3IOUtil()

    def list_files(self, path):
        return self.s3_util.list_files(path)

    def get_file_path(self, base_path, file_name):
        return f"{base_path}/{file_name}"

    def write_file(self, file_path, content):
        self.s3_util.upload_file(content, file_path)
