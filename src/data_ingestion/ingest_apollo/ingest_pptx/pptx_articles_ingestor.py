import argparse

from pptx import Presentation
from lxml.etree import XMLSyntaxError
from src.data_ingestion.ingest_apollo.ingest_pptx.apollo_pptx_to_bioc_converter import (
    pptx_to_bioc_converter,
)
from src.data_ingestion.ingest_apollo.fetch_metadata import (
    metadata_extractor,
)
from src.data_ingestion.ingest_apollo.ingest_pptx.pptx_table_processor import (
    extract_pptx_tables,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class apolloPPTXIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        apollo_source_config: dict[str, str],
        write_to_s3: bool,
        source: str = "apollo",
        summarization_pipe=None,
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.apollo_path = (
            paths_config["ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.ingestion_interim_path = (
            paths_config["ingestion_interim_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.bioc_path = (
            paths_config["bioc_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.article_metadata_path = (
            paths_config["metadata_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.summary_path = (
            paths_config["summary_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.embeddings_path = (
            paths_config["embeddings_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.failed_ingestion_path = (
            paths_config["failed_ingestion_path"]
            .replace("{workflow_id}", workflow_id)
            .replace("{source}", source)
        )
        self.file_handler = file_handler
        self.apollo_source_config = apollo_source_config
        self.source = source
        self.summarization_pipe = summarization_pipe

        # Pop known keys (consumes them from kwargs)
        self.write_to_s3 = write_to_s3
        self.s3_file_handler: Optional[FileHandler] = kwargs.pop(
            "s3_file_handler", None
        )
        self.s3_paths_config: Dict[str, str] = kwargs.pop("s3_paths_config", {}) or {}

        # Build S3 paths only if enabled
        if self.write_to_s3:
            self.s3_apollo_path = self.s3_paths_config.get(
                "ingestion_path", ""
            ).replace("{source}", source)
            self.s3_interim_path = self.s3_paths_config.get(
                "ingestion_interim_path", ""
            ).replace("{source}", source)
            self.s3_bioc_path = self.s3_paths_config.get("bioc_path", "").replace(
                "{source}", source
            )
            self.s3_article_metadata_path = self.s3_paths_config.get(
                "metadata_path", ""
            ).replace("{source}", source)
            self.s3_summary_path = self.s3_paths_config.get("summary_path", "").replace(
                "{source}", source
            )
            self.s3_embeddings_path = self.s3_paths_config.get(
                "embeddings_path", ""
            ).replace("{source}", source)
            self.s3_failed_ingestion_path = self.s3_paths_config.get(
                "failed_ingestion_path", ""
            ).replace("{source}", source)
        else:
            self.s3_apollo_path = (
                self.s3_bioc_path
            ) = (
                self.s3_article_metadata_path
            ) = (
                self.s3_summary_path
            ) = (
                self.s3_interim_path
            ) = self.embeddings_path = self.s3_failed_ingestion_path = None

    def process_pptx_file(self, apollo_file_path: str):
        """
        Try to open PPTX. On failure, move to failed_ingestion_path and return False.
        On success, return True.
        """
        try:
            prs = Presentation(apollo_file_path)
            logger.info(f"Successfully opened PPTX: {apollo_file_path}")
            return True
        except XMLSyntaxError as e:
            logger.warning(f"XMLSyntaxError while reading {apollo_file_path}: {e}")
        except Exception as e:
            logger.warning(f"Error while reading {apollo_file_path}: {e}")

        # Move failed file to failed_ingestion_path
        file_name = apollo_file_path.split("/")[-1]
        dest_path = self.file_handler.get_file_path(
            self.failed_ingestion_path, file_name
        )

        self.file_handler.move_file(apollo_file_path, dest_path)
        logger.info(f"Moved {apollo_file_path} to failed ingestion folder: {dest_path}")

        ##Upload to S3 made common across Apollo Docs
        # if self.write_to_s3:
        #     s3_dest_path = self.s3_file_handler.get_file_path(
        #         self.s3_failed_ingestion_path, file_name
        #     )
        #     self.s3_file_handler.copy_file_local_to_s3(dest_path, s3_dest_path)
        #     logger.info(
        #         f"Uploaded {dest_path} to S3 failed ingestion folder: {s3_dest_path}"
        #     )
        return False

    def pptx_processor(self, file: str):
        logger.info(f"Started PPTX Processing for {file}")

        if file.endswith(".pptx"):
            apollo_file_path = self.file_handler.get_file_path(self.apollo_path, file)

            # --- PRE-CHECK: open PPTX safely ---
            if not self.process_pptx_file(apollo_file_path=apollo_file_path):
                # failed to open, already moved to failed_ingestion_path
                return

            logger.info(f"Started Metadata extraction for {file}")
            # fetch_metadata
            metadata_fields = metadata_extractor(
                file=file,
                article_metadata_path=self.article_metadata_path,
                local_file_handler=self.file_handler,
                s3_article_metadata_path=self.s3_article_metadata_path,
                s3_file_handler=self.s3_file_handler,
            )

            logger.info(f"Started PPTX to BioC conversion for {file}")
            # pptx to bioc conversion
            pptx_to_bioc_converter(
                file_handler=self.file_handler,
                internal_doc_name=file,
                internal_docs_path=self.apollo_path,
                bioc_path=self.bioc_path,
                metadata_fields=metadata_fields,
                write_to_s3=self.write_to_s3,
                s3_bioc_path=self.s3_bioc_path,
                s3_file_handler=self.s3_file_handler,
            )

            logger.info(f"Started Table extraction for {file}")
            # extract table
            extract_pptx_tables(
                file_handler=self.file_handler,
                pptx_path=apollo_file_path,
                interim_dir=self.ingestion_interim_path,
                embeddings_dir=self.embeddings_path,
                bioc_metadata_fields=metadata_fields,
                write_to_s3=self.write_to_s3,
                s3_embeddings_path=self.s3_embeddings_path,
                s3_interim_path=self.s3_interim_path,
                s3_file_handler=self.s3_file_handler,
            )
            logger.info(f"Completed PPTX Processing for {file}")

            ##Upload to S3 made common across Apollo Docs
            # if self.write_to_s3:
            #     s3_dest_path = self.s3_file_handler.get_file_path(
            #         self.s3_apollo_path, file
            #     )
            #     self.s3_file_handler.copy_file_local_to_s3(
            #         apollo_file_path, s3_dest_path
            #     )
            #     logger.info(
            #         f"Uploaded {apollo_file_path} to S3 ingestion folder: {s3_dest_path}"
            #     )
        else:
            logger.error(f"{file} is not a PPTX file.")

    # Runs the combined process
    def run(self, file_name: str):
        self.pptx_processor(file=file_name)


def main():
    """
    Main function to run the apollo Ingestor with improved command-line interface.
    """
    logger.info("Execution Started")

    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    write_to_s3 = False
    s3_paths = {}
    s3_file_handler = None
    if write_to_s3:
        # Get S3 Paths and file handler for writing to S3
        storage_type = "s3"
        s3_paths = paths_config["storage"][storage_type]
        s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    parser = argparse.ArgumentParser(
        description="Ingest apollo articles",
        epilog="Example: python3 -m src.data_ingestion.ingest_apollo.ingest_pptx.articles_ingestor --workflow_id workflow123 --source apollo",
    )

    parser.add_argument(
        "--workflow_id",
        "-wid",
        type=str,
        help="Workflow ID of JIT pipeline run",
    )

    parser.add_argument(
        "--source",
        "-src",
        type=str,
        help="Article source (e.g., pmc, ct, apollo etc.)",
        default="apollo",
    )

    args = parser.parse_args()

    if not args.workflow_id:
        logger.error("No workflow_id provided.")
        return
    else:
        workflow_id = args.workflow_id
        logger.info(f"{workflow_id} Workflow Id registered for processing")

    if not args.source:
        logger.error("No source provided. Defaulting to 'ct'.")
        source = "ct"
    else:
        source = args.source
        logger.info(f"{source} registered as SOURCE for processing")

    apollo_source_config = paths_config["ingestion_source"][source]

    apollo_ingestor = apolloPPTXIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        apollo_source_config=apollo_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    apollo_ingestor.run()

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
