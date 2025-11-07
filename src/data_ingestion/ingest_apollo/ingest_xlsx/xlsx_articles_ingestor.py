import argparse, re, os
import pandas as pd
from jedi.api import file_name
#from xlsx import Presentation
from lxml.etree import XMLSyntaxError

from src.data_ingestion.ingest_apollo.fetch_metadata import (
    metadata_extractor,
)
from src.data_ingestion.ingest_apollo.ingest_xlsx.xlsx_table_processor import (
    process_tables,
)
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.pandoc_processor import PandocProcessor
from pathlib import Path
from typing import Any, Dict, Optional, List

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class apolloXLSXIngestor:
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
            ) = self.s3_embeddings_path = self.s3_failed_ingestion_path = None

    def make_safe_filename(self,filename: str, max_len: Optional[int] = None) -> str:
        """
        Produce a safe filename by replacing every non-alphanumeric character with '_'.
        Keeps extension intact. Collapses repeated underscores and strips leading/trailing '_'.
        Optionally truncates the stem to max_len characters (applied before adding extension).
        """

        # replace any char that is not A-Za-z0-9 with underscore
        safe_stem = re.sub(r"[^A-Za-z0-9]", "_", filename)

        # collapse consecutive underscores and strip leading/trailing underscores
        safe_stem = re.sub(r"_+", "_", safe_stem).strip("_")

        if max_len and len(safe_stem) > max_len:
            safe_stem = safe_stem[:max_len]

        # if stem became empty (e.g., filename was only symbols), fallback to "file"
        if not safe_stem:
            safe_stem = "file"

        return f"{safe_stem}"


    def process_xlsx_file(self, apollo_file_path: str):
        """
        Try to open XLSX. On failure, move to failed_ingestion_path and return False.
        On success, return True.
        """
        try:
            #prs = Presentation(apollo_file_path)
            all_sheets_dict = pd.read_excel(apollo_file_path, sheet_name=None)
            logger.info(f"Successfully opened XLSX: {apollo_file_path}")
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

    def xlsx_to_html_conversion(self,
                            apollo_file_path: Path,
                            file: str,
                            sheet_name: str = None
         ):
        """
                Read xlsx file and first convert each sheet in csv then to html file.
        """
        try :
            all_sheets = pd.read_excel(apollo_file_path, sheet_name=sheet_name)
            for sheet_name, df in all_sheets.items():
                # re_sheet_name= re.sub(r"[^A-Za-z0-9]", "_", sheet_name).replace("__","_")
                safe_sheet_name = self.make_safe_filename(sheet_name)
                interim_dir = Path(f"{self.ingestion_interim_path}/{file.replace(".xlsx", "")}/{safe_sheet_name}")
                interim_dir.mkdir(parents=True, exist_ok=True)
                csv_filename = self.file_handler.get_file_path(interim_dir, f"{safe_sheet_name}.csv")
                # os.path.join(csv_dir, f"{sheet_name}.csv")
                df.to_csv(csv_filename, index=False)
                logger.info(f"  - Converted sheet '{sheet_name}' to {csv_filename}")
                self.convert_xlsx_to_html(
                    apollo_doc=f"{safe_sheet_name}.csv",
                    apollo_path=self.apollo_path,
                    apollo_interim_path=interim_dir,
                    failed_ingestion_path=self.failed_ingestion_path,
                    input_doc_type="csv",  # ["docx","doc"],
                    output_doc_type="html",
                )

            logger.info(f"Completed All xlsx sheet conversion to html for {apollo_file_path}")
        except Exception as e:
            logger.warning(f"Error occurred while converting xlsx to html for {apollo_file_path}: {e}")

    def convert_xlsx_to_html(self,
            apollo_doc: str,
            apollo_path: str,
            apollo_interim_path: str,
            failed_ingestion_path: str,
            input_doc_type: str = "csv",  # ["docx","doc"],
            output_doc_type: str = "html",
    ):
        pandoc_processor = PandocProcessor(pandoc_executable="pandoc")

        if apollo_doc.endswith(".csv") and not apollo_doc.startswith("~$"):
            logger.info(f"Started file format conversion for doc: {apollo_doc}")

            input_doc_path = Path(apollo_interim_path) / apollo_doc
            #input_doc_path = Path(apollo_interim_path)

            # Create an output directory for this docx
            apollo_dir_name = apollo_doc.replace(".csv", "")
            apollo_output_dir = Path(apollo_interim_path)
            apollo_output_dir.mkdir(parents=True, exist_ok=True)
            output_doc_path = Path(apollo_output_dir) / apollo_doc.replace(
                f".{input_doc_type}", f".{output_doc_type}"
            )
            media_dir = Path(apollo_output_dir)
            logger.info(f"Converting {input_doc_path} to {output_doc_path}")

            pandoc_processor.convert(
                input_path=input_doc_path,
                output_path=output_doc_path,
                input_format=input_doc_type,
                output_format=output_doc_type,
                failed_ingestion_path=failed_ingestion_path,
                extract_media_dir=media_dir,
            )
            logger.info(f"{apollo_doc} Converted to HTML Successfully!")
        else:
            logger.error(f"{apollo_doc} is not a DocX file to convert.")

    def extract_tables_from_xlsx_html(self,
            xlsx_interim_path: str,
            article_metadata : dict,
            xlsx_embeddings_path: str,
            xlsx_filename : str,
    ):
        table_details_block=[]
        sheet_idx=0
        for xlsx_html_dir in os.listdir(xlsx_interim_path):
            logger.info(f"Processing XLSX HTML file: {xlsx_html_dir}")
            xlsx_html_dir_path = Path(xlsx_interim_path) / xlsx_html_dir
            xlsx_html_file_path = xlsx_html_dir_path / (xlsx_html_dir + ".html")
            xlsx_html_file_name = xlsx_html_dir + ".html"
            if os.path.exists(xlsx_html_file_path):
                sheet_idx +=1
                logger.info(f"HTML file found: {xlsx_html_file_name}. Extracting Tables...")

                # Read the HTML content
                html_content = self.file_handler.read_file(xlsx_html_file_path)

                # Process tables in XLSX HTML
                html_with_flat_tables, table_details = process_tables(
                    html_str=html_content,
                    source_filename=xlsx_html_file_name,
                    sheet_idx=sheet_idx,
                    output_tables_path=xlsx_html_dir_path,
                    article_metadata=article_metadata,
                    xlsx_filename=xlsx_filename,
                    #table_state="remove",
                    )
                table_details_block = [*table_details_block,*table_details]

                # Write back modified HTML with flat table text
                #self.file_handler.write_file(xlsx_html_file_path, html_with_flat_tables)
                logger.info(
                    f"Extracted {len(table_details)} tables from {xlsx_html_file_name}"
                )

        # Make sure xlsx_embeddings_path exists
        Path(xlsx_embeddings_path).mkdir(parents=True, exist_ok=True)
        # Save table details as JSON
        logger.info(f"Saving extracted table details to JSON...")
        file_path = f"{xlsx_embeddings_path}/{xlsx_filename}_tables.json"
        self.file_handler.write_file_as_json(file_path=file_path, content=table_details_block)
        logger.info(f"Table details saved to {file_path}")

    def xlsx_processor(self, file: str):
        logger.info(f"Started XLSX Processing for {file}")

        if file.endswith(".xlsx"):
            apollo_file_path = self.file_handler.get_file_path(self.apollo_path, file)

            # --- PRE-CHECK: open XLSX safely ---
            if not self.process_xlsx_file(apollo_file_path=apollo_file_path):
                # failed to open, already moved to failed_ingestion_path
                return

            logger.info(f"Started Metadata extraction for {file}")
            # fetch_metadata
            article_metadata = metadata_extractor(
                file=file,
                article_metadata_path=self.article_metadata_path,
                local_file_handler=self.file_handler,
                s3_article_metadata_path=self.s3_article_metadata_path,
                s3_file_handler=self.s3_file_handler,
            )

            # # Read the .xlsx file
            # df = pd.read_excel(apollo_file_path)
            #
            # # Write to an .html file
            # apollo_html_path = self.file_handler.get_file_path(self.ingestion_interim_path, file.replace(".xlsx", ".html"))
            # df.to_html(apollo_html_path, index=False)

            self.xlsx_to_html_conversion(apollo_file_path=apollo_file_path, file=file, sheet_name=None)

            logger.info(f"Started Table extraction for {file}")
            # extract table
            interim_dir = f"{self.ingestion_interim_path}/{file.replace(".xlsx", "")}/"
            self.extract_tables_from_xlsx_html(
                xlsx_interim_path=interim_dir,
                article_metadata=article_metadata,
                xlsx_embeddings_path=self.embeddings_path,
                xlsx_filename=file.replace(".xlsx", ""),
            )
            logger.info(f"Tables Extracted from XLSX HTML Articles Successfully for {file}")

        else:
            logger.error(f"{file} is not a XLSX file.")


    # Runs the combined process
    def run(self, file_name: str):
        self.xlsx_processor(file=file_name)


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
        epilog="Example: python3 -m src.data_ingestion.ingest_apollo.ingest_xlsx.xlsx_articles_ingestor --workflow_id workflow1 --source apollo",
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

    apollo_ingestor = apolloXLSXIngestor(
        workflow_id=workflow_id,
        source=source,
        file_handler=file_handler,
        paths_config=paths,
        apollo_source_config=apollo_source_config,
        write_to_s3=write_to_s3,
        s3_paths_config=s3_paths,
        s3_file_handler=s3_file_handler,
    )

    apollo_ingestor.run(file_name="")

    logger.info("Execution Completed! Articles Ingested!")


# Calling the main method
if __name__ == "__main__":
    main()
