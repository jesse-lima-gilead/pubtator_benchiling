import subprocess
import os
from pathlib import Path
from typing import Union, Optional, List

from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.file_handler.local_handler import LocalFileHandler

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class PandocProcessor:
    """
    Generic Pandoc processor for converting documents to desired formats.
    """

    def __init__(self, pandoc_executable: str = "pandoc", file_handler=None):
        self.pandoc_executable = pandoc_executable
        
        # Use provided file handler or try to get from config
        if file_handler is not None:
            self.file_handler = file_handler
        else:
            try:
                from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
                from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
                
                # Initialize the config loader
                config_loader = YAMLConfigLoader()
                
                # Retrieve paths config
                paths = config_loader.get_config("paths")
                storage_type = paths["storage"]["type"] if paths else "local"

                # Get file handler instance from factory
                self.file_handler = FileHandlerFactory.get_handler(storage_type)
            except Exception as e:
                logger.warning(f"Could not load config, using LocalFileHandler: {e}")
                self.file_handler = LocalFileHandler()

    def convert(
        self,
        input_path,
        output_path,
        input_format: str,
        output_format: str,
        failed_ingestion_path: str,
        template_path=None,
        extract_media_dir: Optional[Union[str, Path]] = None,
        extra_args: Optional[List[str]] = None,
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
            failed_file_path = (
                f"{Path(failed_ingestion_path)}/{os.path.basename(input_path)}"
            )
            self.file_handler.move_file(str(input_path), failed_file_path)
            logger.info(f"Moved Failed File {input_path} to {failed_ingestion_path}")
            logger.error(f"Failed Pandoc Conversion for {input_path}: {e}")
