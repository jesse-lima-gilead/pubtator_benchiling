import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import os
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader


# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()
model_path_type = config_loader.get_config("paths")["model"]["type"]
model_path_config = config_loader.get_config("paths")["model"][model_path_type][
    "summarization_model"
]


# ---- user-provided regex for dates ----
MONTHS = r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
RE_DATE_ISO_SEP = re.compile(
    r"\b(20\d{2}[-_/]\d{1,2}[-_/]\d{1,2})\b"
)  # 2024-06-11 or 2024_06_11
RE_DATE_US_SEP = re.compile(
    r"\b(\d{1,2}[-_/]\d{1,2}[-_/]\d{2,4})\b"
)  # 3_26_2024 or 03-26-2024
RE_DATE_COMPACT = re.compile(
    r"\b(20\d{6}|\d{6,8})\b"
)  # 20240430, 240513, 062222, 20230113
RE_MONTH_DAY_YEAR = re.compile(
    r"(" + MONTHS + r")\s*(\d{1,2})\d{0,12}(20\d{2})", re.IGNORECASE
)
RE_DAY_MONTH_YEAR = re.compile(
    r"\b(\d{1,2})\s*(" + MONTHS + r")\s*(20\d{2})", re.IGNORECASE
)
RE_MONTH_YEAR = re.compile(r"\b(" + MONTHS + r")[- _]?(20\d{2})\b", re.IGNORECASE)

# ---- month map ----
MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _preprocess_for_text_matching(text: str) -> str:
    """Normalize separators and put spaces between letters <-> digits to help month/day/year regexes."""
    t = re.sub(r"[_/:-]+", " ", text)  # separators -> space
    t = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", t)  # 05Nov -> 05 Nov
    t = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", t)  # Nov05 -> Nov 05
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_date(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (date_normalized, date_raw)
      - date_normalized: 'YYYY-MM-DD' (if day present) or 'YYYY-MM' (if only month-year)
      - date_raw: the raw substring matched
    """
    # 1. ISO-like with separators: 2024-06-11 or 2024_06_11
    if m := RE_DATE_ISO_SEP.search(text):
        date_raw = m.group(1)
        s = re.sub(r"[_/]", "-", date_raw)
        try:
            dt = datetime.strptime(s, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d"), date_raw
        except ValueError:
            pass

    # 2. US-style with separators: 03-26-2024 or 3_26_24
    if m := RE_DATE_US_SEP.search(text):
        date_raw = m.group(1)
        s = re.sub(r"[_/]", "-", date_raw)
        for fmt in ("%m-%d-%Y", "%m-%d-%y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d"), date_raw
            except ValueError:
                continue

    # 3. Compact numeric: 20240430, 240513, 062222, etc.
    if m := RE_DATE_COMPACT.search(text):
        date_raw = m.group(1)
        val = date_raw
        for fmt in ("%Y%m%d", "%y%m%d", "%m%d%Y", "%m%d%y"):
            try:
                dt = datetime.strptime(val, fmt)
                return dt.strftime("%Y-%m-%d"), date_raw
            except ValueError:
                continue

    # 4. Preprocess to handle glued tokens or underscores (e.g., 05Nov21, 20_Nov_2018, 19June2012)
    t = _preprocess_for_text_matching(text)

    # 5. Day Month Year like "20 Nov 2018" or two-digit year "05 Nov 21"
    m = re.search(r"\b(\d{1,2})\s*(" + MONTHS + r")\s*(\d{2,4})\b", t, re.IGNORECASE)
    if m:
        day_s, month_s, year_s = m.groups()
        day = int(day_s)
        month = MONTH_MAP[month_s.lower()[:3]]
        year = int(year_s)
        if year < 100:  # treat two-digit as 2000s by default
            year += 2000
        try:
            dt = datetime(year, month, day)
            return dt.strftime("%Y-%m-%d"), m.group(0)
        except ValueError:
            pass

    # 6. Month Day Year like "Nov 20 2018"
    m = re.search(r"\b(" + MONTHS + r")\s*(\d{1,2})\s*(\d{2,4})\b", t, re.IGNORECASE)
    if m:
        month_s, day_s, year_s = m.groups()
        day = int(day_s)
        month = MONTH_MAP[month_s.lower()[:3]]
        year = int(year_s)
        if year < 100:
            year += 2000
        try:
            dt = datetime(year, month, day)
            return dt.strftime("%Y-%m-%d"), m.group(0)
        except ValueError:
            pass

    # 7. Month Year only like "Nov 2018" or "Aug2024"
    if m := RE_MONTH_YEAR.search(t):
        month_s, year_s = m.groups()
        month = MONTH_MAP[month_s.lower()[:3]]
        year = int(year_s)
        return f"{year:04d}-{month:02d}", m.group(0)

    return None, None


# ---- metadata extractor using your date regexes; puts extracted fields directly into meta ----
def extract_filename_metadata(filename: str) -> dict:
    """
    Extract metadata fields and normalized date from filename.
    Adds found fields into meta dict, e.g. meta['date_normalized'] = ...
    Keeps GS code as 'GS_<digits>' in meta['gs_code'].
    """
    pattern = re.compile(
        r"""
        ^GS_(?P<gs_code>\d+)_                                   # GS code digits
        (?P<indication>[A-Za-z0-9]+)?_?                         # Indication (optional)
        (?P<mechanism>[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*)?_?        # Mechanism/target (optional)
        RFD                                                     # Marker
        (?:_(?P<extra>complete|number_\d+|FINAL))?              # Extra tags (optional)
        .*                                                      # anything else (dates etc.)
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    meta = {
        "article_id": filename.split(".")[0],
        "date_normalized": normalize_date(filename),
    }  # base name without extension
    match = pattern.search(filename)

    if match:
        groups = {k: v for k, v in match.groupdict().items() if v is not None}
        # keep GS_ prefix
        if "gs_code" in groups:
            meta["gs_code"] = f"GS_{groups.pop('gs_code')}"
        # add remaining groups directly
        for k, v in groups.items():
            meta[k] = v

    if "gs_code" not in meta:
        pattern = re.compile(r"(?<!\w)(GS[-_]\d+)(?=$|[_\s]|[^A-Za-z0-9])")
        match = pattern.search(filename)
        if match:
            meta["gs_code"] = match.group(1)

    # Always attempt to extract a date (even if main pattern failed)
    date_norm, date_raw = normalize_date(filename)
    if date_norm:
        meta["date_normalized"] = date_norm
        meta["date_raw"] = date_raw

    return meta


def extract_tables_metadata(tables_summary_path: str) -> dict:
    """
    Extract tables metadata from the tables summary JSON file.
    """
    try:
        with open(tables_summary_path, "r", encoding="utf-8") as f:
            tables_summary = f.read()
        import json

        tables_data = json.loads(tables_summary)
        num_tables = len(tables_data)

        tables_keywords = []
        for table in tables_data:
            tables_keywords.append(table["table_keywords"])
            # tables_list.append(
            #     {
            #         "table_id": table.get("table_id", ""),
            #         "table_name": table.get("table_name", ""),
            #         "table_keywords": table.get("table_keywords", []),
            #         "caption": table.get("caption", ""),
            #         "header_text": table.get("header_text", ""),
            #         "footer_text": table.get("footer_text", ""),
            #     }
            # )
        tables_keywords = list(set(tables_keywords))
        tables_metadata = {"num_tables": num_tables, "table_keywords": tables_keywords}
        return tables_metadata
    except Exception as e:
        logger.error(f"Error reading tables summary from {tables_summary_path}: {e}")
        return {}


def articles_metadata_extractor(
    rfd_path: str, metadata_path: str, file_handler: FileHandler
) -> int:
    """
    Extract metadata from RFD filenames and save to a JSON file.
    """
    metadata_extraction_cnt = 0
    for rfd_article in os.listdir(rfd_path):
        if rfd_article.endswith(".docx") and not rfd_article.startswith("~$"):
            rfd_article_name = rfd_article.replace(".docx", "")
            file_name_metadata = extract_filename_metadata(rfd_article_name)
        else:
            continue
        rfd_metadata = {**file_name_metadata}

        # Add article_type to rfd_metadata
        rfd_metadata["article_type"] = "RFD"

        if rfd_metadata:
            try:
                rfd_metadata_file_name = rfd_article_name + "_metadata.json"
                rfd_metadata_file_path = Path(metadata_path) / rfd_metadata_file_name
                file_handler.write_file_as_json(rfd_metadata_file_path, rfd_metadata)
                metadata_extraction_cnt += 1
                logger.info(
                    f"Metadata extracted and saved for {rfd_article_name} to {rfd_metadata_file_path}"
                )
            except Exception as e:
                logger.error(f"Error saving metadata for {rfd_article_name}: {e}")
        else:
            logger.warning(
                f"No metadata extracted for {rfd_article_name}, skipping save."
            )
    return metadata_extraction_cnt


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

            article_metadata = json.load(metadata_file)
        return article_metadata
    except Exception as e:
        logger.error(
            f"Error reading article metadata from {article_metadata_path}: {e}"
        )
        return {}


# ---- quick test on your samples ----
if __name__ == "__main__":
    files = [
        "GS_1156_PI_Stabilizer_RFD_20_Nov_2018_FINAL.docx",
        "GS_1720_HIV_INSTI_QW_RFD_05Nov21.docx",
        "GS_6734_Oral_Nuc_Cov_lipid_RFD.docx",
        "GS_4416_PD_L1_number_2_RFD_FINAL_13_Mar_2019.docx",
        "GS_9813_HCV_Nuc3_RFD_19June2012.docx",
        "GS_9901_PI3Kd_RFD_19Dec2013.docx",
    ]
    for f in files:
        print(extract_filename_metadata(f))
