from pathlib import Path
from typing import Any, Dict, Union
from src.data_ingestion.ingest_internal.extract_metadata.docx_metadata_extractor import (
    DocxMetadataExtractor,
)
from src.data_ingestion.ingest_internal.extract_metadata.slides_metadata_extractor import (
    SlidesMetadataExtractor,
)


class MetadataExtractorFactory:
    """
    Factory to return the appropriate processor based on file extension.
    """

    @staticmethod
    def get_extractor(
        file_path: Union[str, Path], pandoc_executable: str = "pandoc"
    ) -> Union[DocxProcessor, PptProcessor]:  # noqa: F821
        """
        Return a processor instance for the given file.

        Args:
            file_path: Path to the input file (used to detect extension).
            pandoc_executable: Optional path to the pandoc binary.

        Raises:
            ValueError: if file extension is not supported.
        """
        ext = Path(file_path).suffix.lower()

        if ext == ".docx":
            return DocxMetadataExtractor
        if ext in (".ppt", ".pptx"):
            return SlidesMetadataExtractor

        raise ValueError(f"Unsupported file extension: {ext}")
