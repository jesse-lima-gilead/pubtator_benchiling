import re
import unicodedata
import uuid
from pathlib import PurePosixPath
import hashlib
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingest_rfd.rfd_articles_preprocessor import (
    generate_safe_filename,
)

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def extract_from_s3(
    path: str, file_handler: FileHandler, source: str, storage_type: str = "s3"
):
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")

    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    s3_paths = paths_config["storage"][storage_type]
    # Source S3 data path
    src_data_path = s3_paths["ingestion_path"].replace("{source}", source)

    src_files = s3_file_handler.list_files(src_data_path)

    for cur_src_file in src_files:
        # path of the source s3 key
        cur_s3_full_path = s3_file_handler.get_file_path(src_data_path, cur_src_file)
        # path where the files are going to be written to in the ingestion directory of HPC
        cur_staging_path = file_handler.get_file_path(path, cur_src_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_s3_full_path, cur_staging_path)

        logger.info(
            f"File downloaded from S3: {cur_s3_full_path} to local: {cur_staging_path}"
        )

    ingested_articles_cnt = len(src_files)

    return ingested_articles_cnt


def stable_hash(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()


def extract_from_s3_eln(
    path: str,
    file_handler: FileHandler,
    source: str,
    storage_type: str = "s3",
    s3_src_path: str = "notebook",
    file_type: str = "json",
):
    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    src_files = s3_file_handler.s3_util.list_files(s3_src_path)  # to get full path

    files_to_grsar_id_map = {}

    # download to local
    for cur_s3_full_path in src_files:
        file_extension = cur_s3_full_path.split(".")[-1]
        # to consider only the file_type we want to extract
        if file_type != "all" and file_type != file_extension:
            continue
        document_grsar_id = stable_hash(cur_s3_full_path)
        cur_s3_file = f"{document_grsar_id}.{file_extension}"
        cur_staging_path = file_handler.get_file_path(path, cur_s3_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_s3_full_path, cur_staging_path)

        # map which has filename to uuid which will be utilised in extract_metadata
        files_to_grsar_id_map[cur_s3_full_path] = document_grsar_id
        logger.info(
            f"File downloaded from S3: {cur_s3_full_path} to local: {cur_staging_path}"
        )

    ingested_articles_cnt = len(files_to_grsar_id_map)
    logger.info(f"Files downloaded from S3: {ingested_articles_cnt}")

    return files_to_grsar_id_map


def extract_from_s3_rfd(
    path: str,
    file_handler: FileHandler,
    source: str,
    storage_type: str = "s3",
    s3_src_path: str = "RFD",
):
    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    src_files = s3_file_handler.s3_util.list_files(s3_src_path)  # to get full path

    # download to local
    for cur_s3_full_path in src_files:
        # path where the files are going to be written to in the ingestion directory of HPC
        cur_s3_file = cur_s3_full_path.split("/")[-1]
        cur_staging_path = file_handler.get_file_path(path, cur_s3_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_s3_full_path, cur_staging_path)
        logger.info(
            f"File downloaded from S3: {cur_s3_full_path} to local: {cur_staging_path}"
        )

    safe_file_name_cnt = generate_safe_filename(path)
    logger.info(
        f"Safe file names generated for {safe_file_name_cnt} articles successfully!"
    )


def extract_from_s3_apollo(
    path: str,
    file_handler: FileHandler,
    source: str,
    storage_type: str = "s3",
    s3_src_path: str = "Apollo",
    file_type: str = "all",
):
    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    src_files = s3_file_handler.s3_util.list_files(s3_src_path)  # to get full path

    # Filter out unwanted files
    filtered_files = []
    for file_path in src_files:
        s_clean = clean_path_str(file_path)
        p = PurePosixPath(s_clean)
        filename = p.name
        extension = p.suffix.lower()
        # to consider only the file_type we want to extract
        if file_type != "all" and file_type != extension.removeprefix("."):
            continue
        is_temp = (
            filename.startswith("~$")
            or any(pref in s_clean for pref in TEMP_PREFIXES)
            or extension in TEMP_EXTS
        )
        if is_temp:
            continue
        filtered_files.append(file_path)

    files_to_uuid_map = {}

    for cur_s3_full_path in filtered_files:
        # path where the files are going to be written to in the ingestion directory of HPC
        file_extension = cur_s3_full_path.split("/")[-1].split(".")[-1]
        file_uuid = str(uuid.uuid4())
        cur_src_file = f"{file_uuid}.{file_extension}"
        cur_staging_path = file_handler.get_file_path(path, cur_src_file)
        # Download to local HPC path
        s3_file_handler.s3_util.download_file(cur_s3_full_path, cur_staging_path)

        # map which has filename to uuid which will be utilised in extract_metadata
        files_to_uuid_map[cur_s3_full_path] = file_uuid
        logger.info(
            f"File downloaded from S3: {cur_s3_full_path} to local: {cur_staging_path}"
        )

    ingested_articles_cnt = len(filtered_files)
    logger.info(f"Files downloaded from S3: {ingested_articles_cnt}")

    return files_to_uuid_map


TEMP_PREFIXES = ("~$", ".DS_Store", "Thumbs.db")
TEMP_EXTS = {".tmp", ".db", ".lnk"}


# ---------------------
# Cleaning helpers
# ---------------------


def clean_path_str(s: str) -> str:
    """
    Normalize unicode and remove invisible / nuisance characters that often break regexes.
    Also normalise common single-quote/apostrophe characters to hyphen so ID forms like
    "GS'9598" or "GS’9598" become "GS-9598" and are detected as IDs (not dates).
    """
    if not isinstance(s, str):
        return s

    s = unicodedata.normalize("NFKC", s)

    # remove invisible nuisance characters
    for ch in ("\u00AD", "\u200B", "\u200C", "\uFEFF", "\u00A0"):
        s = s.replace(ch, "")

    # various dashes -> hyphen
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", s)

    # normalize curly quotes and straight apostrophes to hyphen (so GS'9598 -> GS-9598)
    s = re.sub(r"[’‘`']", "-", s)

    # collapse repeated whitespace around slashes and hyphens to single space where helpful
    s = re.sub(r"\s*[/\\]\s*", "/", s)
    s = re.sub(r"\s*-\s*", "-", s)

    return s
