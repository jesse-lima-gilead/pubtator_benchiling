import uuid
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import re
from typing import Optional, Tuple, List
import os

from src.data_ingestion.ingestion_utils.s3_uploader import upload_to_s3
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Reuse your existing imports/functions:
# expand_table_to_matrix
# generate_clean_and_context_flat
# extract_table_id_and_name  (optional)
# get_article_metadata
# DOMAIN_STOPWORDS


# small domain stoplist - extend as needed
DOMAIN_STOPWORDS = set(
    [
        "median",
        "mean",
        "range",
        "interquartile",
        "iqr",
        "p-value",
        "p",
        "n",
        "number",
        "unit",
        "units",
        "value",
        "values",
        "count",
        "percentage",
    ]
)

# simple abbreviation expansion example (extend with your domain mappings)
ABBREV_MAP = {
    "aki": "acute kidney injury",
    "mi": "myocardial infarction",
    # add more domain-specific ones
}
_re_clean_strip = re.compile(r"[^0-9A-Za-z\s\-\_\/\%\.\(\)\,]")
_re_multi_space = re.compile(r"\s+")
_re_control = re.compile(r"[\x00-\x1f\x7f-\x9f]+")
_re_keep_chars = re.compile(r"[^0-9A-Za-z\s\-\_\/\%\.\(\)\,]")  # allow / - _ % . ( ) ,


def is_numeric_like(s: str) -> bool:
    """Return True if s is purely numeric / range / percentage etc."""
    if not isinstance(s, str) or not s.strip():
        return False
    s = s.strip()
    # pure number or percentage or number ranges like '10-20', '10–20', '10 to 20'
    if re.fullmatch(r"^[\d\.,]+%?$", s):
        return True
    if re.search(r"^\d+[\-\u2013\u2014]\d+$", s):  # hyphen or dash ranges
        return True
    if re.search(r"^\d+\s*(to|-|\u2013|\u2014)\s*\d+$", s):
        return True
    return False


def expand_table_to_matrix(table_tag):
    """
    Return matrix (list of rows) where each cell is text, expanding rowspan/colspan.
    """
    rows = table_tag.find_all("tr")
    matrix = []  # list of lists representing finished rows
    # We'll maintain a "grid" as we go that tracks occupied cells
    grid = []  # list of lists
    for r_idx, tr in enumerate(rows):
        # ensure grid has row r_idx
        while len(grid) <= r_idx:
            grid.append([])
        c_idx = 0
        for cell in tr.find_all(["td", "th"]):
            # find the next available column
            while c_idx < len(grid[r_idx]) and grid[r_idx][c_idx] is not None:
                c_idx += 1
            text = " ".join(cell.stripped_strings)
            rowspan = int(cell.get("rowspan", 1))
            colspan = int(cell.get("colspan", 1))
            # ensure enough columns in existing grid rows for colspan
            max_col = c_idx + colspan
            for rr in range(len(grid)):
                while len(grid[rr]) < max_col:
                    grid[rr].append(None)
            # place value and mark spans
            for dr in range(rowspan):
                rr = r_idx + dr
                while len(grid) <= rr:
                    grid.append([None] * max_col)
                for dc in range(colspan):
                    cc = c_idx + dc
                    # For the top-left slot place the value, others mark as continuation (empty string)
                    grid[rr][cc] = text if (dr == 0 and dc == 0) else ""
            c_idx = max_col
    # normalize row lengths
    max_len = max(len(r) for r in grid) if grid else 0
    matrix = [
        [(cell if cell is not None else "") for cell in row]
        + [""] * (max_len - len(row))
        for row in grid
    ]
    return matrix


def extract_caption_header_footer_pmc(table_wrap):
    """
    Extract caption, header_text, footer_text from PMC <table-wrap>.
    """
    # ---- CAPTION ----
    caption_tag = table_wrap.find("caption")
    caption_text = " ".join(caption_tag.stripped_strings) if caption_tag else None

    # ---- HEADER TEXT ----
    # Standard PMC structure: header exists inside <thead>
    header_text = None
    thead = table_wrap.find("thead")
    if thead:
        header_row = thead.find("tr")
        if header_row:
            header_text = " ".join(
                [
                    " ".join(th.stripped_strings)
                    for th in header_row.find_all(["th", "td"])
                ]
            )

    # ---- FOOTER TEXT ----
    footer = table_wrap.find("table-wrap-foot")
    footer_text = " ".join(footer.stripped_strings) if footer else None

    return caption_text, header_text, footer_text


def normalize_cell_text(text: str, lowercase: bool = True) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _re_control.sub(" ", s)
    s = s.strip()
    # remove unwanted characters but keep medically useful punctuation
    s = _re_keep_chars.sub(" ", s)
    s = _re_multi_space.sub(" ", s).strip()
    if lowercase:
        s = s.lower()
    # expand simple abbreviations (optional)
    words = []
    for token in s.split():
        words.append(ABBREV_MAP.get(token, token))
    return " ".join(words)


def generate_clean_and_context_flat(
    caption: Optional[str],
    header: Optional[str],
    matrix: List[List[str]],
    *,
    lowercase_clean: bool = True,
    remove_numeric_cells_from_clean: bool = False,
    remove_tokens_with_digits_from_clean: bool = False,
    strip_special_chars_from_clean: bool = False,
    header_boost: int = 3,
    domain_stopwords: Optional[set] = None,
    ner_preserve_punctuation: bool = True,
) -> Tuple[str, str]:
    """
    Return (clean_flat, context_flat).

    Defaults preserve numbers and commonly useful punctuation in clean_flat.
    - lowercase_clean: lowercase clean text (default True).
    - remove_numeric_cells_from_clean: if True, skip pure-numeric cells from clean_flat (default False).
    - remove_tokens_with_digits_from_clean: if True, drop tokens containing digits (default False).
    - strip_special_chars_from_clean: if True, remove uncommon special chars (default False).
    - header_boost: repeat header tokens this many times in clean_flat to raise importance.
    - domain_stopwords: optional set of boilerplate tokens to skip in clean_flat.
    - ner_preserve_punctuation: whether context_flat preserves punctuation useful for NER/UI.
    """

    # context-safe caption/header (keep original punctuation/spacing for readability)
    cap_context = (caption or "").strip()
    hdr_context = (header or "").strip()

    # normalized caption/header for cleaning pipeline
    cap_norm = normalize_cell_text(caption or "", lowercase=lowercase_clean)
    hdr_norm = normalize_cell_text(header or "", lowercase=lowercase_clean)

    # helper to apply optional stricter cleaning for clean tokens
    def _make_clean_token(s: str) -> str:
        s = s.strip()
        if not s:
            return ""
        if strip_special_chars_from_clean:
            # remove characters outside permitted set (this keeps digits & common punctuation)
            s = _re_clean_strip.sub(" ", s)
        if remove_tokens_with_digits_from_clean:
            tokens = [t for t in s.split() if not re.search(r"\d", t)]
            s = " ".join(tokens)
        s = _re_multi_space.sub(" ", s).strip()
        if lowercase_clean:
            s = s.lower()
        return s

    hdr_clean = _make_clean_token(hdr_norm)
    cap_clean = _make_clean_token(cap_norm)

    # Build clean tokens (header/caption boosting)
    clean_tokens: List[str] = []
    if hdr_clean:
        clean_tokens += [hdr_clean] * max(1, int(header_boost))
    if cap_clean:
        clean_tokens += [cap_clean] * max(1, int(header_boost // 2))

    context_lines: List[str] = []

    def cell_for_context(raw_cell: str) -> str:
        if raw_cell is None:
            return ""
        s = str(raw_cell)
        s = _re_control.sub(" ", s).strip()
        s = _re_multi_space.sub(" ", s)
        return s

    # Iterate rows/cells
    for r_idx, row in enumerate(matrix, start=1):
        ctx_cells = []
        for c in row:
            c_raw = c or ""
            c_norm_full = normalize_cell_text(c_raw, lowercase=lowercase_clean)

            # 1) Skip numeric-only cells in clean (only if requested)
            if remove_numeric_cells_from_clean and is_numeric_like(c_norm_full):
                ctx_cells.append(cell_for_context(c_raw))
                continue

            # 2) Skip purely domain-stopword cells in clean (if domain_stopwords provided)
            if (
                domain_stopwords
                and c_norm_full
                and all(tok in domain_stopwords for tok in c_norm_full.split())
            ):
                ctx_cells.append(cell_for_context(c_raw))
                continue

            # 3) Create cleaned token for clean_flat, applying optional stripping
            c_clean = c_norm_full
            if strip_special_chars_from_clean:
                c_clean = _re_clean_strip.sub(" ", c_clean)
            if remove_tokens_with_digits_from_clean:
                tokens = [t for t in c_clean.split() if not re.search(r"\d", t)]
                c_clean = " ".join(tokens)
            c_clean = _re_multi_space.sub(" ", c_clean).strip()
            if lowercase_clean:
                c_clean = c_clean.lower()

            # Add to clean tokens if non-empty
            if c_clean:
                clean_tokens.append(c_clean)

            # Keep original-ish value in context
            ctx_cells.append(cell_for_context(c_raw))

        context_lines.append(" | ".join(ctx_cells))

    # Final assembly
    clean_flat = " ".join([t for t in clean_tokens if t]).strip()
    clean_flat = _re_multi_space.sub(" ", clean_flat)
    # Add caption as well
    clean_flat = cap_context + " " + clean_flat

    context_parts: List[str] = []
    if cap_context:
        context_parts.append(f"Caption: {cap_context}")
    if hdr_context:
        context_parts.append(f"Header: {hdr_context}")
    for i, line in enumerate(context_lines, start=1):
        context_parts.append(f"Row {i}: {line}")
    context_flat = "\n".join(context_parts)

    return clean_flat, context_flat


def process_pmc_tables(
    xml_str: str,
    article_id: str,
    article_metadata: dict,
    output_tables_path: str,
) -> list:
    """
    PMC version of process_tables() that:
    - Parses <table-wrap> blocks in PMC XML
    - Produces dictionaries identical to HTML process_tables()
    """
    soup = BeautifulSoup(xml_str, "lxml-xml")

    results = []
    # Ensure output directory exists
    output_tables_path = Path(output_tables_path) / article_id
    output_tables_path.mkdir(parents=True, exist_ok=True)

    # Iterate through all PMC table-wrap blocks
    for idx, table_wrap in enumerate(soup.find_all("table-wrap")):
        # Extract the actual <table> inside <table-wrap>
        table_tag = table_wrap.find("table")
        if table_tag is None:
            # Some PMC tables may be images; skip for now
            continue

        # Convert table to HTML string
        table_html = str(table_tag)

        # Convert table to matrix (reuse your same function)
        matrix = expand_table_to_matrix(table_tag)

        # Extract caption, header_text, footer_text in PMC style
        caption, header, footer = extract_caption_header_footer_pmc(table_wrap)

        # ---- COLUMN NAME EXTRACTION (Option A + C) ----
        column_names = None

        # Option A: <thead> exists → extract <th>
        thead = table_tag.find("thead")
        if thead:
            header_row = thead.find("tr")
            if header_row:
                column_names = [
                    " ".join(th.stripped_strings)
                    for th in header_row.find_all(["th", "td"])
                ]

        # Option C: No <thead> → infer from first matrix row
        if not column_names and len(matrix) > 0:
            inferred = [cell.strip() for cell in matrix[0]]
            # Ensure at least 1 non-empty header field
            if any(inferred):
                column_names = inferred

        # If still no good headers → fallback to numeric columns
        if not column_names:
            column_names = [str(i) for i in range(len(matrix[0]))]

        # ---- REMOVE HEADER ROW FROM MATRIX ----
        if header or thead:
            # Remove first row from matrix only IF it corresponded to header
            data_rows = matrix[1:]
        else:
            data_rows = matrix

        # article_table_id = <table-wrap id="...">
        article_table_id = table_wrap.get("id") or f"PMC_Table_{idx + 1}"

        # ---- DETERMINE TABLE NAME (Label → Caption → Fallback) ----
        label_tag = table_wrap.find("label")
        # print("LABEL TAG:", label_tag)
        if label_tag and label_tag.get_text(strip=True):
            table_name = " ".join(label_tag.stripped_strings)
        elif caption and caption.strip():
            table_name = caption.strip()
        else:
            table_name = f"PMC_Table_{idx + 1}"

        # Build merged_text in similar format to HTML version
        merged_text = f"{table_name}\n{caption or ''}\n{header or ''}\n{table_html}\n{footer or ''}"
        # logger.info(f"DATA ROWS: {data_rows}")
        logger.info(f"COLUMN NAMES: {column_names}")
        # ---- BUILD DATAFRAME WITH REAL COLUMN NAMES ----
        df = pd.DataFrame(data_rows)

        # ----- SAVE EXCEL FILE (same as HTML version) -----
        excel_file = output_tables_path / f"{article_table_id}.xlsx"
        df.to_excel(
            excel_file,
            index=False,
            header=False,
        )
        logger.info(f"Table extracted: {excel_file}")

        # Clean & context text
        clean_flat, context_flat = generate_clean_and_context_flat(
            caption=caption,
            header=header,
            matrix=matrix,
            lowercase_clean=True,
            remove_numeric_cells_from_clean=True,
            remove_tokens_with_digits_from_clean=False,
            strip_special_chars_from_clean=True,
            header_boost=3,
            domain_stopwords=DOMAIN_STOPWORDS,
            ner_preserve_punctuation=True,
        )

        # Build output dictionary identical to your HTML parser
        results.append(
            {
                "payload": {
                    "table_sequence": idx + 1,  # 1-based index
                    "table_id": str(uuid.uuid4()),
                    "chunk_processing_date": datetime.now().date().isoformat(),
                    "article_id": article_id,
                    "article_table_id": article_table_id,
                    "table_name": table_name,
                    **article_metadata,
                    "caption": caption,
                    "header_text": header,
                    "footer_text": footer,
                    "columns": column_names or list(df.columns.astype(str)),
                    "row_count": df.shape[0],
                    "column_count": df.shape[1],
                    "clean_flat_text": clean_flat,
                    "context_flat_text": context_flat,
                    "merged_text": merged_text,
                    "chunk_type": "table_chunk",
                    "processing_ts": datetime.now().isoformat(),
                }
            }
        )

    logger.info(f"Processed {len(results)} tables for article {article_id}")
    return results


def write_pmc_tables_json(
    article_id: str,
    tables_list: list,
    embeddings_output_dir: str,
    file_handler: FileHandler,
):
    file_name = f"{article_id}_tables.json"
    json_file = file_handler.get_file_path(embeddings_output_dir, file_name)
    file_handler.write_file_as_json(json_file, tables_list)
    logger.info(f"Written {file_name} table embeddings to {json_file}")

    # output_file = output_dir / f"pmc_{article_id}_tables.json"
    # with open(output_file, "w", encoding="utf-8") as f:
    #     json.dump(tables, f, indent=2, ensure_ascii=False)
    # return output_file


def upload_pmc_table_files(
    interim_path: str,
    s3_interim_path: str,
    embeddings_path: str,
    s3_embeddings_path: str,
    file_handler: FileHandler,
    s3_file_handler: FileHandler,
):
    file_upload_counter = 0

    if file_handler.exists(interim_path):
        # Upload the Interim HTML Files to S3
        logger.info(f"Uploading Apollo Interim Files to S3")
        apollo_interim_file_upload_counter = 0
        for apollo_dir in os.listdir(interim_path):
            logger.info(f"Processing apollo dir: {apollo_dir}")
            apollo_dir_path = Path(interim_path) / apollo_dir
            for apollo_interim_file in os.listdir(apollo_dir_path):
                apollo_interim_file_path = file_handler.get_file_path(
                    apollo_dir_path, apollo_interim_file
                )

                # Uploading the Tables XLSX, Article HTML, TOC Removed Passages and Table Extraction Summary
                if os.path.isfile(
                    apollo_interim_file_path
                ) and not apollo_interim_file.startswith("~$"):
                    s3_path = str(apollo_dir) + "/" + str(apollo_interim_file)
                    s3_file_path = s3_file_handler.get_file_path(
                        s3_interim_path, s3_path
                    )
                    logger.info(
                        f"Uploading file {apollo_interim_file_path} to S3 path {s3_file_path}"
                    )
                    upload_to_s3(
                        local_path=apollo_interim_file_path,
                        s3_path=s3_file_path,
                        s3_file_handler=s3_file_handler,
                    )
                    apollo_interim_file_upload_counter += 1

                # Uploading the images in the media folder for docx
                elif os.path.isdir(apollo_interim_file_path):
                    for image_file in file_handler.list_files(apollo_interim_file_path):
                        image_file_path = file_handler.get_file_path(
                            apollo_interim_file_path, image_file
                        )
                        if os.path.isfile(
                            image_file_path
                        ) and not image_file.startswith("~$"):
                            s3_path = (
                                str(apollo_dir)
                                + "/"
                                + str(apollo_interim_file)
                                + "/"
                                + str(image_file)
                            )
                            s3_file_path = s3_file_handler.get_file_path(
                                s3_interim_path, s3_path
                            )
                            logger.info(
                                f"Uploading file {image_file_path} to S3 path {s3_file_path}"
                            )
                            upload_to_s3(
                                local_path=image_file_path,
                                s3_path=s3_file_path,
                                s3_file_handler=s3_file_handler,
                            )
                            apollo_interim_file_upload_counter += 1
                        else:
                            logger.warning(f"Skipping file: {image_file} for S3 upload")
                else:
                    logger.warning(
                        f"Skipping file: {apollo_interim_file} for S3 upload"
                    )

        logger.info(
            f"Total Apollo Interim Files uploaded to S3: {apollo_interim_file_upload_counter}"
        )
        file_upload_counter += apollo_interim_file_upload_counter

    if file_handler.exists(embeddings_path):
        # Upload the Embeddings Files to S3
        logger.info(f"Uploading Apollo Embeddings Files to S3")
        apollo_embeddings_upload_counter = 0
        for apollo_embedding_file in file_handler.list_files(embeddings_path):
            if apollo_embedding_file.endswith(".json"):
                local_file_path = file_handler.get_file_path(
                    embeddings_path, apollo_embedding_file
                )
                s3_file_path = s3_file_handler.get_file_path(
                    s3_embeddings_path, apollo_embedding_file
                )
                logger.info(
                    f"Uploading file {apollo_embedding_file} to S3 path {s3_file_path}"
                )
                upload_to_s3(
                    local_path=local_file_path,
                    s3_path=s3_file_path,
                    s3_file_handler=s3_file_handler,
                )
                apollo_embeddings_upload_counter += 1
            else:
                logger.warning(f"Skipping file: {apollo_embedding_file} for S3 upload")
        logger.info(
            f"Total Apollo Embeddings Files uploaded to S3: {apollo_embeddings_upload_counter}"
        )
        file_upload_counter += apollo_embeddings_upload_counter

    return file_upload_counter


def extract_pmc_tables(
    file_handler: FileHandler,
    pmc_file_path: str,
    interim_dir: str,
    embeddings_dir: str,
    article_metadata_path: str,
):
    logger.info(f"Processing PMC file for Tables: {pmc_file_path}")
    xml_content = file_handler.read_file(pmc_file_path)
    article_id = pmc_file_path.split("/")[-1].split(".")[0]
    article_metadata = file_handler.read_json_file(article_metadata_path)
    tables = process_pmc_tables(xml_content, article_id, article_metadata, interim_dir)
    write_pmc_tables_json(article_id, tables, embeddings_dir, file_handler)
    logger.info(f"Completed Processing PMC file for Tables: {pmc_file_path}")
