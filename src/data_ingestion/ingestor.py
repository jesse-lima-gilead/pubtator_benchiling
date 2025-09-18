import argparse

from src.data_ingestion.ingest_clinical_trials.articles_ingestor import CTIngestor
from src.data_ingestion.ingest_preprints_rxivs.articles_ingestor import (
    PrePrintsIngestor,
)
from src.data_ingestion.ingest_pubmed.articles_ingestor import PMCIngestor
from src.data_ingestion.ingest_rfd.articles_ingestor import RFDIngestor
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger = SingletonLogger().get_logger()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest articles",
        epilog="Example: python3 -m src.data_ingestion.ingestor --workflow_id workflow123 --source pmc",
    )
    parser.add_argument(
        "--workflow_id",
        "-wid",
        type=str,
        required=True,
        help="Workflow ID of JIT pipeline run",
    )
    parser.add_argument(
        "--source",
        "-src",
        type=str,
        required=True,
        choices=["pmc", "ct", "preprint", "rfd", "apollo"],
        help="Article source (allowed values: pmc, ct, preprint, rfd, apollo)",
    )
    return parser.parse_args()


def setup_environment(write_to_s3=True):
    config_loader = YAMLConfigLoader()
    paths_config = config_loader.get_config("paths")

    storage_type = paths_config["storage"]["type"]
    file_handler = FileHandlerFactory.get_handler(storage_type)
    paths = paths_config["storage"][storage_type]

    s3_paths, s3_file_handler = {}, None
    if write_to_s3:
        s3_paths = paths_config["storage"]["s3"]
        s3_file_handler = FileHandlerFactory.get_handler("s3")

    return paths_config, paths, file_handler, s3_paths, s3_file_handler


def run_ingestion(
    workflow_id,
    source,
    paths_config,
    paths,
    file_handler,
    write_to_s3,
    s3_paths,
    s3_file_handler,
):
    if source == "pmc":
        # Fetch article IDs
        article_ids_file_path = (
            paths["jit_ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        try:
            with open(article_ids_file_path, "r") as f:
                article_ids = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Failed to read article IDs: {e}")
            return

        if not article_ids:
            logger.error("No article IDs found in the provided file.")
            return

        logger.info(f"Article IDs to ingest: {article_ids}")

        pmc_ingestor = PMCIngestor(
            workflow_id=workflow_id,
            source=source,
            file_handler=file_handler,
            paths_config=paths,
            write_to_s3=write_to_s3,
            s3_paths_config=s3_paths,
            s3_file_handler=s3_file_handler,
        )
        pmc_ingestor.run(article_ids=article_ids, metadata_storage_type="file")

    elif source == "ct":
        ct_source_config = paths_config["ingestion_source"][source]
        ct_ingestor = CTIngestor(
            workflow_id=workflow_id,
            source=source,
            file_handler=file_handler,
            paths_config=paths,
            ct_source_config=ct_source_config,
            write_to_s3=write_to_s3,
            s3_paths_config=s3_paths,
            s3_file_handler=s3_file_handler,
        )
        ct_ingestor.run()

    elif source == "preprint":
        preprints_source_config = paths_config["ingestion_source"][source]
        preprints_ingestor = PrePrintsIngestor(
            workflow_id=workflow_id,
            source=source,
            file_handler=file_handler,
            paths_config=paths,
            preprints_source_config=preprints_source_config,
            write_to_s3=write_to_s3,
            s3_paths_config=s3_paths,
            s3_file_handler=s3_file_handler,
        )
        preprints_ingestor.run()

    elif source == "rfd":
        rfd_source_config = paths_config["ingestion_source"][source]
        rfd_ingestor = RFDIngestor(
            workflow_id=workflow_id,
            source=source,
            file_handler=file_handler,
            paths_config=paths,
            rfd_source_config=rfd_source_config,
            write_to_s3=write_to_s3,
            s3_paths_config=s3_paths,
            s3_file_handler=s3_file_handler,
        )
        rfd_ingestor.run()

    else:
        raise ValueError(f"Unsupported source: {source}")


def main():
    logger.info("Execution Started")

    args = parse_args()
    workflow_id, source = args.workflow_id, args.source
    logger.info(f"{workflow_id} Workflow ID registered for processing")
    logger.info(f"{source} registered as SOURCE for processing")

    write_to_s3 = True
    paths_config, paths, file_handler, s3_paths, s3_file_handler = setup_environment(
        write_to_s3
    )

    run_ingestion(
        workflow_id,
        source,
        paths_config,
        paths,
        file_handler,
        write_to_s3,
        s3_paths,
        s3_file_handler,
    )

    logger.info("Execution Completed! Articles Ingested!")


if __name__ == "__main__":
    main()
