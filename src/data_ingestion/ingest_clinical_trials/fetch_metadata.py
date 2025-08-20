import pandas as pd
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


# Split date string into parts as-is
def split_date_parts(date_str):
    if not isinstance(date_str, str):
        return None
    parts = date_str.strip().split("-")
    keys = ["year", "month", "day"]
    return {k: v for k, v in zip(keys, parts)}


def articles_metadata_extractor(ct_df, metadata_path: str, file_handler: FileHandler):
    # Fields to include in JSON
    required_fields = [
        "nct_id",
        "title",
        "study_url",
        "acronym",
        "study_status",
        "study_results",
        "conditions",
        "interventions",
        "sponsor",
        "collaborators",
        "sex",
        "age",
        "phases",
        "enrollment",
        "funder_type",
        "study_type",
        "study_design",
        "other_ids",
        "start_date",
        "primary_completion_date",
        "completion_date",
        "first_posted",
        "results_first_posted",
        "last_update_posted",
        "locations",
        "study_documents",
    ]

    # Pipe-delimited fields
    pipe_fields = [
        "conditions",
        "interventions",
        "collaborators",
        "phases",
        "study_design",
        "other_ids",
        "locations",
        "study_documents",
    ]

    # Date fields
    date_fields = [
        "start_date",
        "primary_completion_date",
        "completion_date",
        "first_posted",
        "results_first_posted",
        "last_update_posted",
    ]

    # Process each row
    for _, row in ct_df.iterrows():
        nct_id = row.get("nct_id")
        if pd.isna(nct_id):
            continue

        output_data = {}
        for field in required_fields:
            value = row.get(field)
            if pd.isna(value):
                output_data[field] = None
            elif field in date_fields:
                output_data[field] = split_date_parts(value)
            elif field in pipe_fields:
                output_data[field] = [
                    v.strip() for v in str(value).split("|") if v.strip()
                ]
            else:
                output_data[field] = value

        filename = f"{nct_id}_metadata.json"
        file_path = file_handler.get_file_path(metadata_path, filename)

        file_handler.write_file_as_json(file_path, output_data)

        logger.info(f"For nct_id {nct_id}, Saving metadata to {file_path}")
