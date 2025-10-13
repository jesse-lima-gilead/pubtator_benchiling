import os
from pathlib import Path

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def metadata_extractor(
    file: str,
    article_metadata_path: str,
    local_file_handler: FileHandler,
    s3_article_metadata_path: str,
    s3_file_handler: FileHandler,
):
    metadata_file_name = f"{file.split('.')[0]}_metadata.json"
    local_metadata_full_path = local_file_handler.get_file_path(
        article_metadata_path, metadata_file_name
    )
    s3_metadata_path = s3_file_handler.get_file_path(
        s3_article_metadata_path, metadata_file_name
    )

    # read the metadata file from S3
    metadata_fields = s3_file_handler.read_json_file(s3_metadata_path)

    # write the metadata file to local
    local_file_handler.write_file_as_json(local_metadata_full_path, metadata_fields)
    logger.info(
        f"Copied metadata from S3: {s3_metadata_path} to local: {local_metadata_full_path}"
    )

    bioc_metadata_fields = {
        "created_date": metadata_fields.get("created_date", None),
        "team": metadata_fields.get("team", None),
        "sub_team": metadata_fields.get("sub_team", None),
        "sub_context": metadata_fields.get("sub_context", None),
        "full_path": metadata_fields.get("full_path", None),
        "experiment_id_all": metadata_fields.get("experiment_id_all", None),
        "compound_id_all": metadata_fields.get("compound_id_all", None),
        "species": metadata_fields.get("species", None),
        "assay_protocol": metadata_fields.get("assay_protocol", None),
        "assay_protocol_all": metadata_fields.get("assay_protocol_all", None),
        "meeting_report_type": metadata_fields.get("meeting_report_type", None),
        "meeting_report_subtype": metadata_fields.get("meeting_report_subtype", None),
        "meeting_report_type_all": metadata_fields.get("meeting_report_type_all", None),
        "author_owner": metadata_fields.get("author_owner", None),
        "article_type": metadata_fields.get("article_type", None),
        "title": metadata_fields.get("title", None),
    }

    # #testing for local
    # bioc_metadata_fields = {
    #     "created_date": "2023-01-01",
    #     "team": "HIV_Protease_Activator",
    #     "sub_team": "Biology",
    #     "sub_context": "Biology subteam meeting/2022/09222022/20220916",
    #     "full_path": "Apollo/HIV_Protease_Activator/Biology/Biology subteam meeting/2022/20221102/20221102 Spinfection Biology Sub team meeting.pptx",
    #     "experiment_id_all": ["GS-1299598","GS-1591557"],
    #     "compound_id_all": ["GS-1299598","GS-1591557"],
    #     "species": "PBMC",
    #     "assay_protocol": "titration",
    #     "assay_protocol_all": ["titration"],
    #     "meeting_report_type": "Biology subteam meeting",
    #     "meeting_report_subtype": "2022",
    #     "meeting_report_type_all": ["Biology subteam meeting"],
    #     "author_owner": "PBMC",
    #     "article_type": "Chemistry",
    #     "title": "Apollo/HIV_Protease_Activator/Biology/Biology subteam meeting/2022/20221102/20221102 Spinfection Biology Sub team meeting.pptx"
    # }

    # return the metadata retrieved for BioC file
    return bioc_metadata_fields


def get_article_metadata(article_name: str, article_metadata_path: str) -> dict:
    # Read the article metadata JSON file and return metadata
    try:
        article_metadata_file_path = (
            Path(article_metadata_path) / f"{article_name}_metadata.json"
        )
        if not os.path.exists(article_metadata_file_path):
            logger.warning(f"Metadata file not found: {article_metadata_file_path}")
            return {}
        with open(article_metadata_file_path, "r") as metadata_file:
            import json

            metadata_fields = json.load(metadata_file)
            article_metadata = {
                "created_date": metadata_fields.get("created_date", None),
                "team": metadata_fields.get("team", None),
                "sub_team": metadata_fields.get("sub_team", None),
                "sub_context": metadata_fields.get("sub_context", None),
                "full_path": metadata_fields.get("full_path", None),
                "experiment_id_all": metadata_fields.get("experiment_id_all", None),
                "compound_id_all": metadata_fields.get("compound_id_all", None),
                "species": metadata_fields.get("species", None),
                "assay_protocol": metadata_fields.get("assay_protocol", None),
                "assay_protocol_all": metadata_fields.get("assay_protocol_all", None),
                "meeting_report_type": metadata_fields.get("meeting_report_type", None),
                "meeting_report_subtype": metadata_fields.get(
                    "meeting_report_subtype", None
                ),
                "meeting_report_type_all": metadata_fields.get(
                    "meeting_report_type_all", None
                ),
                "author_owner": metadata_fields.get("author_owner", None),
                "article_type": metadata_fields.get("article_type", None),
                "title": metadata_fields.get("title", None),
            }
        return article_metadata
    except Exception as e:
        logger.error(
            f"Error reading article metadata from {article_metadata_path}: {e}"
        )
        return {}
