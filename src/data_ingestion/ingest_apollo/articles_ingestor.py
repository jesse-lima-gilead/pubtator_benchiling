from src.data_ingestion.ingest_apollo.apollo_articles_uploader import (
    upload_apollo_articles,
)
from src.data_ingestion.ingest_apollo.ingest_pptx.pptx_articles_ingestor import (
    apolloPPTXIngestor,
)
from src.data_ingestion.ingest_apollo.ingest_docx.docx_articles_ingestor import (
    apolloDOCXIngestor,
)
from src.data_ingestion.ingest_apollo.ingest_xlsx.xlsx_articles_ingestor import (
    apolloXLSXIngestor,
)
from src.data_ingestion.ingest_apollo.ingest_pdf.pdf_articles_ingestor import (
    apolloPDFIngestor,
)

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from typing import Any, Dict, Optional
from pathlib import Path

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class APOLLOIngestor:
    def __init__(
        self,
        workflow_id: str,
        file_handler: FileHandler,
        paths_config: dict[str, str],
        apollo_source_config: dict[str, str],
        write_to_s3: bool,
        file_type : str = "all",
        source: str = "apollo",
        **kwargs: Any,  # optional extras (e.g. s3 settings)
    ):
        self.source = source
        self.file_type = file_type
        self.workflow_id = workflow_id
        self.paths_config = paths_config
        self.file_handler = file_handler
        self.apollo_source_config = apollo_source_config

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
            ) = (
                self.s3_embeddings_path
            )= (
                self.s3_failed_ingestion_path
            )  = None

    def pdf_md_conversion_file_validation(self,):
        """
        In nextFlow pipeline we are converting pdf file to md format.
        This functions uses it for its validation, incase if we miss to convert any file from pdf to markdown.
        It compares the PDF files and MD files, finds the delta and move the respective .pdf file to failed path
        """

        pdf_file_list = [file.replace(".pdf","") for file in self.file_handler.list_files(self.apollo_path) if file.endswith(".pdf")]
        md_file_list = [file.replace(".md","") for file in self.file_handler.list_files(self.apollo_path) if file.endswith(".md")]
        failed_files = set(pdf_file_list) - set(md_file_list)

        Path(self.failed_ingestion_path).mkdir(parents=True, exist_ok=True)

        logger.info(f" Number PDF file : {len(pdf_file_list)}")
        logger.info(f" Number of MD file : {len(md_file_list)}")

        for file in failed_files:
            try :
                file=f"{file}.pdf"
                if self.file_handler.exists(f"{self.apollo_path}/{file}"):
                    self.file_handler.move_file(str(f"{self.apollo_path}/{file}"), f"{self.failed_ingestion_path}/{file}")
                    logger.info(f"Moved Failed File {self.apollo_path}/{file} to {self.failed_ingestion_path}/{file}")
            except Exception as e:
                logger.info(f"Skipped File {self.apollo_path}/{file} : \n Error occured : {e}")

        logger.info(f" PDF -> MD Failed File Conversion Count : {len(failed_files)}")


    # Runs the combined process
    def run(
        self,
    ):
        logger.info("Instantiating apolloDOCXIngestor...")
        apollo_docx_ingestor = apolloDOCXIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Instantiating apolloPPTXIngestor...")
        apollo_pptx_ingestor = apolloPPTXIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Instantiating apolloXLSXIngestor...")
        apollo_xlsx_ingestor = apolloXLSXIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Instantiating apolloPDFIngestor...")
        apollo_md_ingestor = apolloPDFIngestor(
            workflow_id=self.workflow_id,
            source=self.source,
            file_handler=self.file_handler,
            paths_config=self.paths_config,
            apollo_source_config=self.apollo_source_config,
            write_to_s3=self.write_to_s3,
            s3_paths_config=self.s3_paths_config,
            s3_file_handler=self.s3_file_handler,
        )
        # For PDF processing : Upstream conversion pipeline(nextFlow pipeline) generates Markdown (.md) as the intermediate format.
        # Processing is now standardized on the .md format, replacing the original .pdf source.
        self.file_type= "md" if self.file_type == "pdf" else self.file_type

        logger.info("Processing Apollo Articles...")
        allowed_file_type=self.apollo_source_config["allowed_file_type"]
        logger.info(f"Allowed file type: {allowed_file_type}")

        if self.file_type == "all" or self.file_type == "md":
            logger.info(f" File Type is : {self.file_type} so, calling the pdf_md_conversion_file_validation")
            self.pdf_md_conversion_file_validation()

        for file_name in self.file_handler.list_files(self.apollo_path):
            logger.info(f"Processing Apollo file {file_name}")
            file_extn = file_name.split(".")[-1]
            if self.file_type == "all":
                if file_extn in allowed_file_type:
                    logger.info(f"Calling apollo_{file_extn}_ingestor.")
                    eval(f"apollo_{file_extn}_ingestor").run(file_name=file_name)
                else:
                    logger.info(f"Skipping file because it is of file type: {file_extn}")
            else:
                if (self.file_type in allowed_file_type) and (file_name.endswith(f".{self.file_type}")):
                    logger.info(f"Calling apollo_{self.file_type}_ingestor.")
                    eval(f"apollo_{self.file_type}_ingestor").run(file_name=file_name)
                else :
                    logger.info(f"Skipping file because it is of file type: {file_extn}")

        logger.info("Uploading All the Processed files from Local to S3")
        upload_apollo_articles(
            apollo_path=self.apollo_path,
            s3_apollo_path=self.s3_apollo_path,
            bioc_path=self.bioc_path,
            s3_bioc_path=self.s3_bioc_path,
            interim_path=self.ingestion_interim_path,
            s3_interim_path=self.s3_interim_path,
            summary_path=self.summary_path,
            s3_summary_path=self.s3_summary_path,
            embeddings_path=self.embeddings_path,
            s3_embeddings_path=self.s3_embeddings_path,
            failed_ingestion_path=self.failed_ingestion_path,
            s3_failed_ingestion_path=self.s3_failed_ingestion_path,
            file_handler=self.file_handler,
            s3_file_handler=self.s3_file_handler,
        )
        logger.info("Completed Uploading All the files from Local to S3")
