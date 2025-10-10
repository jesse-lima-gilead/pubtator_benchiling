import argparse
from src.data_ingestion.ingest_apollo.apollo_articles_extractor import (
    extract_apollo_articles,
    apollo_generate_safe_filename,
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
        epilog="Example: python3 -m src.data_ingestion.extractor --source pmc --timestamp 20250919153659",
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
        choices=["pmc", "ct", "preprint", "rfd", "apollo", "eln"],
        help="Article source (allowed values: pmc, ct, preprint, rfd, apollo)",
    )
    return parser.parse_args()


def setup_environment(source: str, timestamp: str):
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

    source_config = paths_config["ingestion_source"][source]

    return paths_config, paths, file_handler, extraction_path, source_config


def run_extraction(
    source,
    file_handler,
    source_config,
    extraction_path,
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
        extracted_articles_count = extract_apollo_articles(
            apollo_path=extraction_path,
            file_handler=file_handler,
            apollo_source_config=source_config,
            source=source,
        )
        logger.info(
            f"{extracted_articles_count} Apollo Articles Extracted Successfully!"
        )

        apollo_generate_safe_filename(
            apollo_path=extraction_path,
            file_handler=file_handler,
            source=source,
        )
        logger.info(f"Generated Safe file names for Apollo Articles Successfully!")

        apollo_articles_metadata_extractor(
            apollo_source_config=source_config,
            source=source,
        )
        logger.info(f"Generated Metadata files for Apollo Articles Successfully!")
    elif source == "eln":
        extracted_articles_count = extract_eln_articles(
            eln_path=extraction_path,
            file_handler=file_handler,
            eln_source_config=source_config,
            source=source,
        )
        logger.info(f"{extracted_articles_count} ELN Articles Extracted Successfully!")
    else:
        raise ValueError(f"Unsupported source: {source}")


def main():
    logger.info("Execution Started")

    args = parse_args()
    timestamp, source = args.timestamp, args.source
    logger.info(f"{source} registered as SOURCE for processing")

    (
        paths_config,
        paths,
        file_handler,
        extraction_path,
        source_config,
    ) = setup_environment(source, timestamp)

    logger.info(f"Starting Extraction of {source}")
    run_extraction(source, file_handler, source_config, extraction_path)


if __name__ == "__main__":
    main()
