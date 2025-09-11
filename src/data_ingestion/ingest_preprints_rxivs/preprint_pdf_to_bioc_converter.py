import math
import re
from collections import defaultdict
from typing import List, Tuple, Dict, Any, Optional
import bioc
import pymupdf
from datetime import datetime

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

PAGE_NUM_RE_LIST = [
    re.compile(r"^\s*\d+\s*$"),
    re.compile(r"^\s*page\s*\d+\b", flags=re.I),
    re.compile(r"^\s*\d+\s*/\s*\d+\s*$"),
    re.compile(r"^\s*page\s*\d+\s+of\s+\d+", flags=re.I),
]


def normalize_header_text(
    s: str, replace_digits: bool = True, max_len: int = 140
) -> str:
    if not isinstance(s, str):
        s = str(s)
    s = s.strip().lower()
    if replace_digits:
        s = re.sub(r"\d+", "#NUM#", s)
    s = re.sub(r"[^\w\s#\/]", " ", s)
    s = " ".join(s.split())
    if len(s) > max_len:
        s = s[:max_len]
    return s


def is_page_number_text(s: str) -> bool:
    if not isinstance(s, str):
        return False
    st = s.strip()
    for cre in PAGE_NUM_RE_LIST:
        if cre.match(st):
            return True
    return False


def find_running_headers_footers(
    doc, top_frac=0.08, bottom_frac=0.08, min_pages=2, freq_thresh=0.5
):
    """
    Scan doc (PyMuPDF.Document) and return dict with:
      - top_candidates: set of normalized strings
      - bottom_candidates: set of normalized strings
      - examples for debugging
    It uses page.get_text('blocks') and only block tuples.
    """
    n = doc.page_count
    top_map = defaultdict(set)
    bottom_map = defaultdict(set)
    top_examples = {}
    bottom_examples = {}

    for p in range(n):
        page = doc.load_page(p)
        w, h = page.rect.width, page.rect.height
        top_y_limit = h * top_frac
        bottom_y_start = h * (1.0 - bottom_frac)

        blocks = page.get_text("blocks")
        for b in blocks:
            if not isinstance(b, (list, tuple)) or len(b) < 5:
                continue
            x0, y0, x1, y1 = float(b[0]), float(b[1]), float(b[2]), float(b[3])
            text = (b[4] or "").strip()
            if not text:
                continue
            # top
            if y1 <= top_y_limit:
                norm = normalize_header_text(text)
                top_map[norm].add(p)
                top_examples.setdefault(norm, text)
            # bottom
            if y0 >= bottom_y_start:
                norm = normalize_header_text(text)
                bottom_map[norm].add(p)
                bottom_examples.setdefault(norm, text)

    # decide threshold
    min_occ = max(min_pages, int(math.ceil(freq_thresh * n)))
    top_candidates = set()
    bottom_candidates = set()

    for norm, pageset in top_map.items():
        if len(pageset) >= min_occ or is_page_number_text(top_examples.get(norm, "")):
            top_candidates.add(norm)
    for norm, pageset in bottom_map.items():
        if len(pageset) >= min_occ or is_page_number_text(
            bottom_examples.get(norm, "")
        ):
            bottom_candidates.add(norm)

    report = {
        "n_pages": n,
        "top_frac": top_frac,
        "bottom_frac": bottom_frac,
        "min_pages": min_pages,
        "freq_thresh": freq_thresh,
        "min_occ_threshold": min_occ,
        "top_candidates": list(top_candidates)[:50],
        "bottom_candidates": list(bottom_candidates)[:50],
        "top_examples": {k: top_examples[k] for k in list(top_examples)[:50]},
        "bottom_examples": {k: bottom_examples[k] for k in list(bottom_examples)[:50]},
    }

    return report


# ----------------- geometry helpers -----------------
def area(rect: Tuple[float, float, float, float]) -> float:
    return max(0.0, rect[2] - rect[0]) * max(0.0, rect[3] - rect[1])


def intersection_area(
    a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]
) -> float:
    ix0 = max(a[0], b[0])
    iy0 = max(a[1], b[1])
    ix1 = min(a[2], b[2])
    iy1 = min(a[3], b[3])
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    return (ix1 - ix0) * (iy1 - iy0)


def overlap_fraction(
    text_bbox: Tuple[float, float, float, float],
    other_bbox: Tuple[float, float, float, float],
) -> float:
    a = area(text_bbox)
    if a <= 0:
        return 0.0
    return intersection_area(text_bbox, other_bbox) / a


# ----------------- header regex map (priority order) -----------------
# prefix handles optional numbering like "1.", "I)", etc.
PREFIX = r"^\s*(?:\d+|[ivxlcdm]+)?[\.\)]?\s*"
TRAIL = r"[\:\-\—\–\s]*"  # allow trailing punctuation/spaces after heading

# Build list of (canonical_name, compiled_regex, short_line_word_limit_or_None)
# short_line_word_limit is used to avoid matching very short tokens (fig/tab) inside long lines.
HEADING_PATTERNS = [
    ("abstract", re.compile(PREFIX + r"(?:abstract|summary)\b" + TRAIL, flags=re.I)),
    (
        "introduction",
        re.compile(PREFIX + r"(?:introduction|background)\b" + TRAIL, flags=re.I),
    ),
    (
        "materials_and_methods",
        re.compile(
            PREFIX
            + r"(?:materials\s*(?:and|&|/)\s*methods|materials\s+and\s+methods|experimental\s+procedures|methodology|methods?)\b"
            + TRAIL,
            flags=re.I,
        ),
    ),
    (
        "results_and_discussion",
        re.compile(
            PREFIX
            + r"(?:results(?:\s*(?:and|&)\s*discussion)?|results\s*and\s*discussion)\b"
            + TRAIL,
            flags=re.I,
        ),
    ),
    ("results", re.compile(PREFIX + r"(?:results?)\b" + TRAIL, flags=re.I)),
    (
        "discussion",
        re.compile(
            PREFIX + r"(?:discussion(?:\s*(?:and|&)\s*conclusions?)?)\b" + TRAIL,
            flags=re.I,
        ),
    ),
    (
        "conclusion",
        re.compile(
            PREFIX + r"(?:conclusion|conclusions|concluding\s+remarks)\b" + TRAIL,
            flags=re.I,
        ),
    ),
    # Figures/tables: no short-line guard (allow descriptive captions)
    (
        "figures",
        re.compile(
            PREFIX + r"(?:figure|fig|figs|fig\.)\s*(?:\d+[\w\(\)\-]*)?\b" + TRAIL,
            flags=re.I,
        ),
    ),
    (
        "tables",
        re.compile(
            PREFIX
            + r"(?:table|tables|tab|tables|tab\.)\s*(?:\d+[\w\(\)\-]*)?\b"
            + TRAIL,
            flags=re.I,
        ),
    ),
    (
        "acknowledgements",
        re.compile(
            PREFIX + r"(?:acknowledg?e?ments?|acknowledgments?)\b" + TRAIL, flags=re.I
        ),
    ),
    (
        "references",
        re.compile(
            PREFIX + r"(?:reference|references|bibliography)\b" + TRAIL, flags=re.I
        ),
    ),
    (
        "supplementary",
        re.compile(
            PREFIX
            + r"(?:supplementary(?:\s+information)?|supporting\s+information)\b"
            + TRAIL,
            flags=re.I,
        ),
    ),
    (
        "funding",
        re.compile(PREFIX + r"(?:funding|financial\s+support)\b" + TRAIL, flags=re.I),
    ),
    (
        "author_contributions",
        re.compile(
            PREFIX + r"(?:author\s+contributions?|contributors?)\b" + TRAIL, flags=re.I
        ),
    ),
    (
        "conflict_of_interest",
        re.compile(
            PREFIX + r"(?:conflict\s+of\s+interest|competing\s+interests?)\b" + TRAIL,
            flags=re.I,
        ),
    ),
]


# ----------------- header detection using regex -----------------
def detect_heading_and_strip_regex(block_text: str) -> Tuple[str, str]:
    """
    Check the first two non-empty lines with regexes in order.
    If a match is found, remove the matched prefix (only the matched span),
    then collapse remaining lines into a single space-separated body string.
    Returns (canonical_heading, body_text).
    """
    if not isinstance(block_text, str):
        block_text = str(block_text)

    raw_lines = block_text.splitlines()
    nonempty_lines = [ln for ln in raw_lines if ln.strip()]
    candidates = nonempty_lines[:2]

    matched_canon = None
    matched_pattern = None
    matched_candidate_line = None

    # try each candidate line (up to first two)
    for cand in candidates:
        cand_stripped = cand.strip()
        if not cand_stripped:
            continue
        for canon, cre in HEADING_PATTERNS:
            m = cre.match(cand)  # anchored match at start via regex
            if m:
                matched_canon = canon
                matched_pattern = cre
                matched_candidate_line = cand
                break
        if matched_canon:
            break

    heading = matched_canon if matched_canon else "body_text"

    # remove matched prefix span (only first occurrence) from the block lines
    if matched_pattern:
        removed = False
        new_lines: List[str] = []
        for ln in raw_lines:
            if not removed:
                # attempt to remove prefix using the compiled pattern (anchored)
                new_ln = matched_pattern.sub("", ln, count=1)
                if new_ln != ln:
                    # We removed the matched prefix. If remainder non-empty, keep it.
                    if new_ln.strip():
                        new_lines.append(new_ln)
                    removed = True
                    continue
            new_lines.append(ln)
        body_lines = new_lines
    else:
        body_lines = raw_lines

    # clean_text
    # collapse body lines to single-space separated paragraph
    parts: List[str] = []
    for ln in body_lines:
        s = ln.strip()
        if s:
            parts.append(" ".join(s.split()))
    body_text = " ".join(parts)

    return heading, body_text


def build_bioc_collection_lib(
    source: str, date_str: str, documents: list
) -> bioc.BioCCollection:
    coll = bioc.BioCCollection()
    coll.source = source
    coll.date = date_str
    coll.key = ""  # empty

    metadata_fields = ["preprint_id", "doi", "posted_date", "license", "title"]

    for d in documents:
        doc = bioc.BioCDocument()
        doc.id = d.get("id", "")
        # add infons
        for k, v in (d.get("infons") or {}).items():
            if k in metadata_fields:
                doc.infons[k] = str(v)
        # add passages (flattened)
        for heading, text in d.get("passages", []):
            passage = bioc.BioCPassage()
            passage.infons["type"] = heading if heading else "body_text"
            passage.offset = 0
            passage.text = text or ""
            doc.passages.append(passage)
        coll.documents.append(doc)
    return coll


_RE_XML_ILLEGAL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")


def clean_xml_text(s):
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    # remove illegal chars
    s = _RE_XML_ILLEGAL.sub("", s)
    # optionally normalize line endings:
    # s = s.replace('\r\n', '\n').replace('\r', '\n')
    return s


def make_document_from_blocks(
    doc_id: str,
    kept_blocks_per_page: List[List[Tuple[str, str]]],
    infons: Optional[Dict[str, str]] = None,
    min_words: int = 100,
) -> Dict[str, Any]:
    """
    Convert your per-page kept_blocks into a document dict consumed by build_bioc_collection.
    This version merges consecutive blocks until merged text has at least `min_words`
    (measured by len(text.split())). Each emitted passage uses the heading of the first
    block in the merged buffer. If a new block has a non-'body_text' heading, flush the
    current buffer first (even if short), then start a new buffer with that heading.

    Parameters:
      - doc_id: string id for the document
      - kept_blocks_per_page: list (per page) of lists of (heading, body_text) tuples
      - infons: optional dict of metadata infons
      - min_words: minimum words to accumulate before flushing (int, default 100)

    Returns:
      A dict: {"id": doc_id, "infons": {...}, "passages": [(heading, text), ...]}
      where text has been cleaned with clean_xml_text before being added.
    """

    # helpers
    def _word_count(s: str) -> int:
        if not s:
            return 0
        return len(s.split())

    # flatten blocks preserving page order
    flat_blocks: List[Tuple[str, str]] = []
    for page_blocks in kept_blocks_per_page:
        if not page_blocks:
            continue
        for heading, text in page_blocks:
            # normalize whitespace on individual block early (so counts are stable)
            txt = "" if text is None else " ".join(str(text).split())
            h = "" if heading is None else str(heading)
            flat_blocks.append((h, txt))

    passages: List[Tuple[str, str]] = []

    # buffer state
    buffer_heading: Optional[str] = None
    buffer_text_parts: List[str] = []
    buffer_word_count: int = 0

    def flush_buffer():
        nonlocal buffer_heading, buffer_text_parts, buffer_word_count
        if buffer_heading is None:
            # nothing to flush
            buffer_text_parts = []
            buffer_word_count = 0
            return
        merged_text = " ".join(buffer_text_parts).strip()
        # if "figure" in merged_text.lower():
        #     print("buffer_text_parts", buffer_text_parts)
        if merged_text:
            # clean XML unsafe characters and normalize whitespace
            merged_text_clean = clean_xml_text(merged_text)
            heading_clean = clean_xml_text(buffer_heading) or "body_text"
            passages.append((heading_clean, merged_text_clean))
        # reset buffer
        buffer_heading = None
        buffer_text_parts = []
        buffer_word_count = 0

    # iterate blocks
    for h, txt in flat_blocks:
        # if buffer empty: start it
        if buffer_heading is None:
            buffer_heading = h or "body_text"
            buffer_text_parts = [txt] if txt else []
            buffer_word_count = _word_count(" ".join(buffer_text_parts))
            # immediate flush if it already meets threshold
            if buffer_word_count >= min_words:
                flush_buffer()
            continue

        # if incoming heading is body_text -> append and maybe flush on threshold
        if h == "body_text":
            if txt:
                buffer_text_parts.append(txt)
                buffer_word_count += _word_count(txt)
            if buffer_word_count >= min_words:
                flush_buffer()
            # else continue accumulating
            continue

        # incoming heading is non-body_text:
        # flush current buffer regardless of size, then start a new buffer with this heading
        flush_buffer()
        buffer_heading = h or "body_text"
        buffer_text_parts = [txt] if txt else []
        buffer_word_count = _word_count(" ".join(buffer_text_parts))
        if buffer_word_count >= min_words:
            flush_buffer()
        # continue

    # end of all blocks: flush anything remaining
    flush_buffer()

    # build document dict
    doc = {"id": doc_id, "infons": infons, "passages": passages}
    return doc


# ----------------- core extractor (uses the new regex function) -----------------
def extract_pages_block_level_simple(pdf_path: str, table_thresh: float = 0.2):
    doc = pymupdf.open(pdf_path)

    header_footer_info = find_running_headers_footers(
        doc, top_frac=0.08, bottom_frac=0.08, min_pages=2, freq_thresh=0.5
    )

    n_pages = doc.page_count
    start, end = (0, n_pages)

    kept_blocks_per_page = []

    for pno in range(start, end):
        page = doc.load_page(pno)

        # collect overall table bboxes using page.find_tables()
        table_bboxes: List[Tuple[float, float, float, float]] = []
        try:
            tf = page.find_tables()
            if tf and tf.tables:
                for t in tf.tables:
                    if hasattr(t, "bbox"):
                        table_bboxes.append(tuple(t.bbox))
                    else:
                        cells = [tuple(c) for c in getattr(t, "cells", [])]
                        if cells:
                            x0 = min(c[0] for c in cells)
                            y0 = min(c[1] for c in cells)
                            x1 = max(c[2] for c in cells)
                            y1 = max(c[3] for c in cells)
                            table_bboxes.append((x0, y0, x1, y1))
        except Exception:
            table_bboxes = []

        # get blocks (assume tuple shape)
        blocks = page.get_text("blocks")
        kept_blocks: List[Tuple[str, str]] = []  # (heading, body_text)

        for b in blocks:
            # follow the agreed-upon shape; skip if not matching
            if not isinstance(b, (list, tuple)) or len(b) < 7:
                continue

            try:
                x0 = float(b[0])
                y0 = float(b[1])
                x1 = float(b[2])
                y1 = float(b[3])
            except Exception:
                continue
            text_raw = (b[4] or "") if len(b) >= 5 else ""
            try:
                block_type = int(b[6])
            except Exception:
                block_type = None

            bbox = (x0, y0, x1, y1)

            # drop image blocks or empty text
            if block_type == 1 or (
                isinstance(text_raw, str) and text_raw.strip() == ""
            ):
                logger.info(
                    f"image_or_empty, {(text_raw[:80] if text_raw else '<empty>')}"
                )
                continue

            # drop if overlaps a table bbox by >= table_thresh
            drop_for_table = False
            for tb in table_bboxes:
                if overlap_fraction(bbox, tb) >= table_thresh:
                    drop_for_table = True
                    break
            if drop_for_table:
                logger.info(f"table_overlap {text_raw[:50]}")
                continue

            norm_text = normalize_header_text(text_raw)
            page_h = page.rect.height
            if (
                bbox[3] <= page_h * 0.08
                and norm_text in header_footer_info["top_candidates"]
            ):
                # drop as header
                logger.info(f"Header Removed: {text_raw[:50]}")
                continue
            if bbox[1] >= page_h * (1.0 - 0.08) and (
                norm_text in header_footer_info["bottom_candidates"]
                or is_page_number_text(text_raw)
            ):
                # drop as footer/page-number
                logger.info(f"Footer Removed: {text_raw[:50]}")
                continue

            # Keep block: detect heading and normalize body
            heading, body_text = detect_heading_and_strip_regex(text_raw)

            # new: drop blocks with too few words (simple)
            MIN_BLOCK_WORDS = 10  # tune this: 15..30 typical ranges
            # after heading/body_text extraction:
            body_for_count = (body_text or "").strip()
            # count words (letters/digits/underscore sequences)
            words = re.findall(r"\b\w+\b", body_for_count)
            word_count = len(words)
            if word_count <= MIN_BLOCK_WORDS:
                logger.info(f"Dropped for Short text Block {word_count}")
                # skip (do not append to kept_blocks)
                continue

            kept_blocks.append((heading, body_text))

        kept_blocks_per_page.append(kept_blocks)

        logger.info(
            f"Page {pno + 1}/{n_pages}: kept {len(kept_blocks)} blocks, tables found: {len(table_bboxes)}"
        )

    doc.close()
    return kept_blocks_per_page


def convert_preprint_pdf_to_bioc(
    preprint_file_path: str,
    bioc_path: str,
    metadata_infons: dict,
    file_handler: FileHandler,
    write_to_s3: bool,
    s3_bioc_path: str,
    s3_file_handler: FileHandler,
):
    kept_blocks_per_page = extract_pages_block_level_simple(preprint_file_path)

    doc_id = preprint_file_path.split("/")[-1].split(".")[0]
    doc = make_document_from_blocks(
        doc_id, kept_blocks_per_page, infons=metadata_infons
    )

    source = "Preprints Bioarxiv"
    date_str = datetime.now().strftime("%Y-%m-%d")
    bioc_collection = build_bioc_collection_lib(
        source=source, date_str=date_str, documents=[doc]
    )

    # preprint_file_path is a full path
    bioc_file_name = f"{doc_id}.xml"
    bioc_full_path = file_handler.get_file_path(bioc_path, bioc_file_name)

    file_handler.write_file_as_bioc(bioc_full_path, bioc_collection)
    logger.info(f"For preprint_id {doc_id}, Saving BioC XML to {bioc_full_path}")

    if write_to_s3:
        # Save to S3
        s3_file_path = s3_file_handler.get_file_path(s3_bioc_path, bioc_file_name)
        s3_file_handler.write_file_as_bioc(s3_file_path, bioc_collection)
        logger.info(f"For preprint_id {doc_id}, Saving BioC XML to S3: {s3_file_path}")
