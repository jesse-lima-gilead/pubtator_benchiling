from pathlib import Path
from typing import Union

from src.data_ingestion.ingest_internal.pandoc_processor import PandocProcessor
from src.data_ingestion.ingest_internal.docx_ingestor import DocxProcessor
from src.data_ingestion.ingest_internal.pptx_ingestor import PptxProcessor


class InternalIngestorFactory:
    """
    Factory to return the appropriate processor based on file extension.
    """

    @staticmethod
    def get_processor(file_type, pandoc_executable: str = "pandoc"):
        """
        Return a processor instance for the given file.

        Args:
            file_type: type of file (docx, pdf, ...).
            pandoc_executable: Optional path to the pandoc binary.

        Raises:
            ValueError: if file extension is not supported.
        """
        base = PandocProcessor(pandoc_executable)

        if file_type in ("docx", "doc"):
            return DocxProcessor(base)
        if file_type in ("ppt", "pptx"):
            return PptxProcessor()

        raise ValueError(f"Unsupported file extension: {file_type}")
