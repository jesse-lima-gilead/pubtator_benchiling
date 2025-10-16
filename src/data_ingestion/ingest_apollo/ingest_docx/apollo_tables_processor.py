import uuid
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag
import pandas as pd
import re
from typing import Optional, Tuple, List
import os

from src.data_ingestion.ingest_apollo.fetch_metadata import get_article_metadata
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.data_ingestion.ingest_apollo.ingest_docx.table_keyword_extractor import (
    extract_table_keywords,
)

# Get the logger instance
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()
model_path_type = config_loader.get_config("paths")["model"]["type"]
model_path_config = config_loader.get_config("paths")["model"][model_path_type][
    "summarization_model"
]


def extract_table_id_and_name(
    header_text: Optional[str], caption_text: Optional[str], idx: int
) -> Tuple[str, str]:
    """
    Improved: identifies Table numbers like `Table 1`, `Table 2a`, `TABLE-3:`, `Tbl. 4`, etc.
    Falls back to Extracted_Table_<idx>.
    """
    table_id = f"Extracted_Table_{idx}"
    table_name = f"Extracted_Table_{idx}"
    candidate = (header_text or "") + " " + (caption_text or "")
    candidate = candidate.strip()
    if candidate:
        # look for patterns like "Table 1", "TABLE 12a", "Tbl. 3:", "Table-3"
        match = re.search(
            r"\b(?:Table|TABLE|Tbl|tbl)[\s\.\-]*([0-9]+[a-zA-Z]?)\b", candidate
        )
        if match:
            num = match.group(1)
            table_id = f"Table_{num}"
            table_name = candidate.strip()
        else:
            # if header/caption is short and looks like a title, use it as name
            if len(candidate.split()) <= 15:
                table_name = candidate.strip()
    return table_id, table_name


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


def find_caption_header_footer(table_tag, lookback_siblings=3, lookahead_siblings=3):
    # caption
    cap = table_tag.find("caption")
    caption_text = " ".join(cap.stripped_strings) if cap else None

    # preceding siblings as header candidates
    header_text = None
    footer_text = None

    # function to extract text from a sibling skipping whitespace
    def text_of(node):
        if node is None:
            return None
        if isinstance(node, NavigableString):
            return str(node).strip()
        if isinstance(node, Tag):
            return " ".join(node.stripped_strings).strip()
        return None

    # look back
    sib = table_tag.previous_sibling
    count = 0
    while sib and count < lookback_siblings:
        t = text_of(sib)
        if t:
            # prefer explicit "Table" label or short lines likely to be captions/headers
            if re.search(r"\b[Tt]able\b|\bTbl\b", t) or len(t.split()) < 40:
                header_text = t
                break
        sib = sib.previous_sibling
        count += 1

    # look forward for footers
    sib = table_tag.next_sibling
    count = 0
    while sib and count < lookahead_siblings:
        t = text_of(sib)
        if t:
            if re.match(r"^(Note:|Source:|N=|n=|\()", t) or len(t.split()) < 40:
                footer_text = t
                break
        sib = sib.next_sibling
        count += 1

    return caption_text, header_text, footer_text


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

_re_control = re.compile(r"[\x00-\x1f\x7f-\x9f]+")
_re_multi_space = re.compile(r"\s+")
_re_keep_chars = re.compile(r"[^0-9A-Za-z\s\-\_\/\%\.\(\)\,]")  # allow / - _ % . ( ) ,


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


_re_clean_strip = re.compile(r"[^0-9A-Za-z\s\-\_\/\%\.\(\)\,]")
_re_multi_space = re.compile(r"\s+")
_re_control = re.compile(r"[\x00-\x1f\x7f-\x9f]+")


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

    context_parts: List[str] = []
    if cap_context:
        context_parts.append(f"Caption: {cap_context}")
    if hdr_context:
        context_parts.append(f"Header: {hdr_context}")
    for i, line in enumerate(context_lines, start=1):
        context_parts.append(f"Row {i}: {line}")
    context_flat = "\n".join(context_parts)

    return clean_flat, context_flat


def extract_keywords_from_table(table_df: pd.DataFrame, flattened_table: str):
    table_keywords = extract_table_keywords(
        flat_text=flattened_table,
        table_df=table_df,
    )
    return table_keywords


def process_tables(
    html_str: str,
    source_filename: str,
    output_tables_path: str,
    article_metadata_path: str,
    table_state: str = "remove",
) -> Tuple[str, List[dict]]:
    """
    Process tables in the given HTML string:
    - Extract each table, convert to matrix, find caption/header/footer.
    - Save each table as an Excel file in output_tables_path.
    - Replace or remove tables in the HTML based on table_state.
    - Generate clean_flat and context_flat text representations for each table.
    - Extract keywords from each table.
    - Return modified HTML and list of table metadata dicts.
    :param html_str: HTML content as string
    :param source_filename: Source filename (used for article_id)
    :param output_tables_path: Output directory to save Excel files
    :param article_metadata_path: Metadata file path
    :param table_state: Replace with flattened text ("replace") or remove ("remove")
    :return:
    """
    source_filename = source_filename.replace(".html", "")
    soup = BeautifulSoup(html_str, "lxml")
    article_metadata = get_article_metadata(
        article_name=source_filename, article_metadata_path=article_metadata_path
    )

    result = []
    for idx, table in enumerate(soup.find_all("table")):
        if table.name is None:
            logger.warning(f"Skipping table at index {idx} due to missing tag name.")
            continue

        # Preserve Table HTML in str
        table_html = str(table)

        # Create the matrix representation of the table
        matrix = expand_table_to_matrix(table)

        # Find caption, header, footer
        caption, header, footer = find_caption_header_footer(table)

        # Create Merged Text with Table Header, Table HTML and Table Footer
        merged_text = f"{header or ''}\n{table_html}\n{footer or ''}"

        # Determine table_id and table_name
        article_table_id, table_name = extract_table_id_and_name(header, caption, idx)

        # Save to Excel
        df = pd.DataFrame(matrix)
        df.to_excel(
            Path(output_tables_path) / f"{article_table_id}.xlsx",
            index=False,
            header=False,
        )

        # Replace the table in HTML with flattened text
        clean_flat, context_flat = generate_clean_and_context_flat(
            caption=caption,
            header=header,
            matrix=matrix,
            lowercase_clean=True,
            remove_numeric_cells_from_clean=True,  # skip numeric cells from clean text
            remove_tokens_with_digits_from_clean=False,
            strip_special_chars_from_clean=True,
            header_boost=3,
            domain_stopwords=DOMAIN_STOPWORDS,
            ner_preserve_punctuation=True,
        )

        # Depending on table_state, either remove or replace in HTML
        if table_state == "remove":
            table.decompose()
        elif table_state == "replace":
            replacement = soup.new_tag("div")
            replacement["article_table_id"] = article_table_id or f"table_{idx}"
            replacement["table_name"] = (
                table_name or f"{source_filename or 'doc'}_table_{idx}"
            ).strip()
            pre = soup.new_tag("pre")  # keeps line breaks readable in browsers
            pre.append(NavigableString(context_flat))
            replacement.append(pre)
            table.replace_with(replacement)
        else:
            logger.warning(
                f"Unknown table_state '{table_state}'. Defaulting to 'remove'."
            )
            table.decompose()

        result.append(
            {
                "payload": {
                    "table_sequence": idx,
                    "table_id": str(uuid.uuid4()),
                    "chunk_processing_date": datetime.now().date().isoformat(),
                    "article_id": article_metadata.get("full_path", source_filename),
                    "article_table_id": article_table_id,  # e.g. "Table 1"
                    "table_name": table_name,  # e.g. "Table 1. Aspirational Profile..."
                    "table_keywords": extract_keywords_from_table(
                        table_df=df, flattened_table=clean_flat
                    ),
                    "caption": caption,
                    "header_text": header,
                    "footer_text": footer,
                    "columns": list(df.columns.astype(str)),
                    "row_count": df.shape[0],
                    "column_count": df.shape[1],
                    **article_metadata,
                    "clean_flat_text": clean_flat,
                    "context_flat_text": context_flat,
                    "merged_text": merged_text,
                    "chunk_type": "table_chunk",
                    "processing_ts": datetime.now().isoformat(),
                },
            }
        )
    return str(soup), result
