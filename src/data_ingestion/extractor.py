import argparse
import traceback
from src.data_ingestion.ingest_apollo.apollo_articles_extractor import (
    extract_apollo_articles,
)
from src.data_ingestion.ingest_apollo.extract_metadata import (
    apollo_articles_metadata_extractor,
)
from src.data_ingestion.ingest_clinical_trials.ct_articles_extractor import (
    extract_ct_articles,
)
from src.data_ingestion.ingest_eln.eln_articles_extractor import extract_eln_articles
from src.data_ingestion.ingest_preprints_rxivs.preprint_articles_extractor import (
    extract_preprints_articles,
)
from src.data_ingestion.ingest_rfd.rfd_articles_extractor import extract_rfd_articles
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest articles",
        epilog="Example: python3 -m src.data_ingestion.extractor --source apollo --timestamp 20250919153659 --file_type xlsx",
    )
    parser.add_argument(
        "--timestamp",
        "-ts",
        type=str,
        required=True,
        help="Timestamp of the extraction run",
    )
    parser.add_argument(
        "--source",
        "-src",
        type=str,
        required=True,
        choices=["pmc", "ct", "preprint", "rfd", "apollo", "eln", "safe_eln"],
        help="Article source (allowed values: pmc, ct, preprint, rfd, apollo, safe_eln)",
    )
    parser.add_argument(
        "--file_type",
        "-ft",
        type=str,
        choices=["all", "docx", "pptx", "xlsx", "json"],
        default="all",
        help="Which file type to process, specially applicable in apollo, eln, safe_eln (default: all)",
    )
    return parser.parse_args()


def setup_environment(source: str, timestamp: str, file_type: str):
    config_loader = YAMLConfigLoader()
    paths_config = config_loader.get_config("paths")

    storage_type = paths_config["storage"]["type"]
    file_handler = FileHandlerFactory.get_handler(storage_type)
    paths = paths_config["storage"][storage_type]

    extraction_path = (
        paths["extraction_path"]
        .replace("{source}", source)
        .replace("{timestamp}", timestamp)
    )

    apollo_uuid_map_path = (
        paths["apollo_uuid_map_path"]
        .replace("{source}", source)
        .replace("{file_type}", file_type)
        .replace("{timestamp}", timestamp)
    )

    grsar_id_map_path = (
        paths["grsar_id_map_path"]
        .replace("{source}", source)
        .replace("{file_type}", file_type)
        .replace("{timestamp}", timestamp)
    )

    source_config = paths_config["ingestion_source"][source]

    return (
        paths_config,
        paths,
        file_handler,
        extraction_path,
        source_config,
        apollo_uuid_map_path,
        grsar_id_map_path,
    )


def run_extraction(
    source,
    file_handler,
    source_config,
    extraction_path,
    file_type=None,
    apollo_uuid_map_path=None,
    grsar_id_map_path=None,
):
    if source == "ct":
        extracted_articles_count = extract_ct_articles(
            ct_path=extraction_path,
            file_handler=file_handler,
            ct_source_config=source_config,
            source=source,
        )
        logger.info(f"{extracted_articles_count} CT Articles Extracted Successfully!")
    elif source == "preprint":
        extracted_articles_count = extract_preprints_articles(
            preprints_path=extraction_path,
            file_handler=file_handler,
            preprints_source_config=source_config,
            source=source,
        )
        logger.info(
            f"{extracted_articles_count} Preprint Articles Extracted Successfully!"
        )
    elif source == "rfd":
        extracted_articles_count = extract_rfd_articles(
            rfd_path=extraction_path,
            file_handler=file_handler,
            rfd_source_config=source_config,
            source=source,
        )
        logger.info(f"{extracted_articles_count} RFD Articles Extracted Successfully!")
    elif source == "apollo":
        extracted_files_to_uuid_map = extract_apollo_articles(
            apollo_path=extraction_path,
            file_handler=file_handler,
            apollo_source_config=source_config,
            source=source,
            file_type=file_type,
        )
        logger.info(
            f"{len(extracted_files_to_uuid_map)} Apollo Articles Extracted Successfully!"
        )
        # for time being to capture the uuid map generated for apollo
        logger.info(f"{extracted_files_to_uuid_map}")
        file_handler.write_file_as_json(
            apollo_uuid_map_path, extracted_files_to_uuid_map
        )

        apollo_articles_metadata_extractor(
            apollo_source_config=source_config,
            extracted_files_to_uuid_map=extracted_files_to_uuid_map,
            source=source,
        )
        logger.info(f"Generated Metadata files for Apollo Articles Successfully!")
    elif source == "eln":
        extracted_files_to_grsar_id_map = extract_eln_articles(
            eln_path=extraction_path,
            file_handler=file_handler,
            eln_source_config=source_config,
            source=source,
            file_type=file_type,
        )
        # for time being to capture the grsar_id map generated for eln
        # ToDO add rds implementation
        logger.info(f"{extracted_files_to_grsar_id_map}")
        file_handler.write_file_as_json(
            grsar_id_map_path, extracted_files_to_grsar_id_map
        )
        logger.info(
            f"{len(extracted_files_to_grsar_id_map)} ELN Articles Extracted Successfully!"
        )
    elif source == "safe_eln":
        extracted_files_to_grsar_id_map = extract_eln_articles(
            eln_path=extraction_path,
            file_handler=file_handler,
            eln_source_config=source_config,
            source=source,
            file_type=file_type,
        )
        # for time being to capture the grsar_id map generated for eln
        # ToDO add rds implementation
        logger.info(f"{extracted_files_to_grsar_id_map}")
        file_handler.write_file_as_json(
            grsar_id_map_path, extracted_files_to_grsar_id_map
        )
        logger.info(
            f"{len(extracted_files_to_grsar_id_map)} ELN Articles Extracted Successfully!"
        )
    else:
        raise ValueError(f"Unsupported source: {source}")


def main():
    logger.info("Execution Started")

    args = parse_args()
    timestamp, source, file_type = args.timestamp, args.source, args.file_type
    logger.info(f"{source} registered as SOURCE for processing")

    try:
        (
            paths_config,
            paths,
            file_handler,
            extraction_path,
            source_config,
            apollo_uuid_map_path,
            grsar_id_map_path,
        ) = setup_environment(source, timestamp, file_type)

        logger.info(f"Starting Extraction of {source}")
        run_extraction(
            source,
            file_handler,
            source_config,
            extraction_path,
            file_type,
            apollo_uuid_map_path,
            grsar_id_map_path,
        )
        logger.info("Execution Completed! Articles Extracted Successfully!")
    except Exception as e:
        full_trace = traceback.format_exc()  # full traceback string
        logger.error(f"Extraction for source: {source} failed due to {e}")
        logger.error(full_trace)
        raise


if __name__ == "__main__":
    main()
