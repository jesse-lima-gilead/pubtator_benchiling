import subprocess
from pathlib import Path
from typing import Union, Optional

from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PandocProcessor:
    """
    Generic Pandoc processor for converting documents to desired formats.
    """

    def __init__(self, pandoc_executable: str = "pandoc"):
        self.pandoc_executable = pandoc_executable

    def convert(
        self,
        input_path,
        output_path,
        input_format: str,
        output_format: str,
        template_path=None,
        extract_media_dir: Optional[str | Path] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        """
        Convert a document using pandoc.

        Args:
            input_path: Path to the source file (docx, pptx, etc.).
            output_path: Path to the target file (e.g., markdown, plain text).
            input_format: Pandoc input format (e.g., 'plain', 'markdown').
            output_format: Pandoc output format (e.g., 'plain', 'markdown').
            template_path: Optional path to a custom template.
            extract_media_dir: Directory to extract embedded media (e.g., images).
            extra_args: Additional CLI arguments for pandoc.
        """
        try:
            cmd = [self.pandoc_executable, str(input_path)]
            if template_path:
                cmd += ["--template", str(template_path)]
            if input_format:
                cmd += ["-f", input_format]
            if output_format:
                cmd += ["-t", output_format]
            if extract_media_dir:
                cmd += ["--extract-media", str(extract_media_dir)]
            if extra_args:
                cmd += extra_args

            cmd += ["-o", str(output_path)]

            subprocess.run(cmd, check=True)
            logger.info(f"Converted {input_path} → {output_path}")
        except Exception as e:
            logger.error(f"Failed Pandoc Conversion for {input_path}: {e}")
