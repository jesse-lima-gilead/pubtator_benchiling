import pandas as pd
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def articles_summarizer(ct_df, summary_path, file_handler: FileHandler):
    for _, row in ct_df.iterrows():
        nct_id = row.get("nct_id")
        brief_summary = row.get("brief_summary")

        if pd.isna(nct_id) or pd.isna(brief_summary):
            continue

        filename = f"{nct_id}.txt"
        file_path = file_handler.get_file_path(summary_path, filename)

        file_handler.write_file(file_path, brief_summary)

        logger.info(f"For nct_id {nct_id}, Saving Summary to {file_path}")
