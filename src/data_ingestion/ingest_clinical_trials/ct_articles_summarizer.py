import pandas as pd
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def _shorten_summary_by_sentences(brief_summary: str, max_words: int = 100) -> str:
    """Return a shortened summary following the rules described."""
    if not brief_summary or not isinstance(brief_summary, str):
        return brief_summary

    words = brief_summary.split()
    if len(words) <= max_words:
        return brief_summary

    # Split by '.' and strip whitespace; ignore empty fragments
    lines = [s.strip() for s in brief_summary.split(".") if s.strip()]

    selected_lines = []
    current_word_count = 0

    for line in lines:
        line_words = line.split()
        lw = len(line_words)
        selected_lines.append(line)
        current_word_count += lw
        if current_word_count >= max_words:
            break
        # if current_word_count + lw <= max_words:
        #     selected_lines.append(line)
        #     current_word_count += lw
        # else:
        #     break

    # If we selected fewer than 2 lines but there are at least 2 lines available,
    # pick the first 2 lines as per your rule.
    if len(selected_lines) < 2 and len(lines) >= 2:
        selected_lines = lines[:2]

    logger.info(f"CT Summary shortened to {current_word_count}")

    return ". ".join(selected_lines).strip()


def articles_summarizer(
    ct_df,
    summary_path,
    file_handler: FileHandler,
    write_to_s3: bool,
    s3_summary_path,
    s3_file_handler: FileHandler,
):
    for _, row in ct_df.iterrows():
        nct_id = row.get("nct_id")
        brief_summary = _shorten_summary_by_sentences(row.get("brief_summary"))

        if pd.isna(nct_id) or pd.isna(brief_summary):
            continue

        filename = f"{nct_id}.txt"

        file_path = file_handler.get_file_path(summary_path, filename)
        file_handler.write_file(file_path, brief_summary)
        logger.info(f"For nct_id {nct_id}, Saving Summary to {file_path}")

        if write_to_s3:
            # Save to S3
            s3_file_path = s3_file_handler.get_file_path(s3_summary_path, filename)
            s3_file_handler.write_file(s3_file_path, brief_summary)
            logger.info(f"For nct_id {nct_id}, Saving Summary to S3: {s3_file_path}")
