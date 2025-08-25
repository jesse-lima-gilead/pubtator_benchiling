from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
import pandas as pd
import bioc
from datetime import datetime
import xml.etree.ElementTree as ET

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def create_bioc_from_row(row: pd.Series, date_today: str):
    metadata_fields = [
        "nct_id",
        "title",
        "study_url",
        "acronym",
        "study_status",
        "study_results",
        "other_ids",
        "start_date",
        "primary_completion_date",
        "completion_date",
        "first_posted",
        "results_first_posted",
        "last_update_posted",
    ]

    passage_fields = [
        "brief_summary",
        "primary_outcome_measures",
        "secondary_outcome_measures",
        "other_outcome_measures",
    ]

    collection = bioc.BioCCollection()
    collection.source = "ClinicalTrials.gov"
    collection.date = date_today

    doc = bioc.BioCDocument()
    doc.id = str(row.get("nct_id", ""))

    for field in metadata_fields:
        value = row.get(field, None)
        if pd.notna(value):
            doc.infons[field] = str(value)

    for field in passage_fields:
        value = row.get(field, None)
        if pd.notna(value):
            passage = bioc.BioCPassage()
            passage.infons["type"] = field
            passage.offset = 0
            passage.text = str(value)
            doc.passages.append(passage)

    collection.documents.append(doc)
    return collection


def convert_ct_csv_to_bioc(
    ct_df,
    bioc_path,
    file_handler: FileHandler,
    s3_bioc_path,
    s3_file_handler: FileHandler,
):
    today_str = datetime.now().strftime("%Y-%m-%d")
    for _, row in ct_df.iterrows():
        nct_id = str(row.get("nct_id", "record"))
        collection = create_bioc_from_row(row, today_str)

        filename = f"{nct_id}.xml"

        file_path = file_handler.get_file_path(bioc_path, filename)
        file_handler.write_file_as_bioc(file_path, collection)
        logger.info(f"For article_id {nct_id}, Saving BioC XML to {file_path}")

        # Save to S3
        s3_file_path = s3_file_handler.get_file_path(s3_bioc_path, filename)
        s3_file_handler.write_file_as_bioc(s3_file_path, collection)
        logger.info(f"For article_id {nct_id}, Saving BioC XML to S3: {s3_file_path}")
