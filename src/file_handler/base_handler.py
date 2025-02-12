from abc import ABC, abstractmethod


class FileHandler(ABC):
    """Abstract class to handle file operations for different storage types."""

    @abstractmethod
    def list_files(self, path):
        pass

    @abstractmethod
    def get_file_path(self, base_path, file_name):
        pass

    @abstractmethod
    def write_file(self, file_path, content):
        pass

    @abstractmethod
    def write_file_as_json(self, file_path, content):
        pass

    @abstractmethod
    def parse_xml_file(self, file_path):
        pass

    @abstractmethod
    def read_file(self, file_path):
        pass

    @abstractmethod
    def read_json_file(self, file_path):
        pass

    @abstractmethod
    def write_file_as_bioc(self, file_path, bioc_document):
        pass
