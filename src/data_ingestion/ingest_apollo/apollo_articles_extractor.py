from src.data_ingestion.ingestion_utils.s3_extractor import extract_from_s3_apollo
import re
from pathlib import PurePosixPath
from typing import Optional

from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_apollo_articles(
    apollo_path: str,
    file_handler: FileHandler,
    apollo_source_config: dict,
    source: str,
    file_type: str = "all",
):
    source_type = apollo_source_config["type"]

    if source_type == "s3":
        s3_src_path = apollo_source_config["s3_src_path"]
        # call the S3 extractor
        extracted_files_to_uuid_map = extract_from_s3_apollo(
            apollo_path, file_handler, source, source_type, s3_src_path, file_type
        )
        return extracted_files_to_uuid_map
    elif source_type == "API":
        pass
    else:
        raise ValueError(f"Unsupported Source type: {source}")


def make_safe_filename(filename: str, max_len: Optional[int] = None) -> str:
    """
    Produce a safe filename by replacing every non-alphanumeric character with '_'.
    Keeps extension intact. Collapses repeated underscores and strips leading/trailing '_'.
    Optionally truncates the stem to max_len characters (applied before adding extension).
    """
    p = PurePosixPath(filename)
    stem = p.stem
    ext = p.suffix  # keep as-is including leading dot

    # replace any char that is not A-Za-z0-9 with underscore
    safe_stem = re.sub(r"[^A-Za-z0-9]", "_", stem)

    # collapse consecutive underscores and strip leading/trailing underscores
    safe_stem = re.sub(r"_+", "_", safe_stem).strip("_")

    if max_len and len(safe_stem) > max_len:
        safe_stem = safe_stem[:max_len]

    # if stem became empty (e.g., filename was only symbols), fallback to "file"
    if not safe_stem:
        safe_stem = "file"

    return f"{safe_stem}{ext}"
