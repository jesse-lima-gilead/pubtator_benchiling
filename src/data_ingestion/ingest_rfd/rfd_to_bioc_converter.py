import os
from bs4 import BeautifulSoup, Tag, NavigableString
import bioc
from bioc import BioCCollection, BioCDocument, BioCPassage
import json
import re
from datetime import date
import html
import re
import unicodedata
import json
from typing import Optional
import bioc
from typing import List, Tuple, Optional

from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

from pathlib import Path

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# Initialize the config loader
config_loader = YAMLConfigLoader()

# Retrieve paths config
paths = config_loader.get_config("paths")
storage_type = paths["storage"]["type"]

# Get file handler instance from factory
file_handler = FileHandlerFactory.get_handler(storage_type)
# Retrieve paths from config
paths_config = paths["storage"][storage_type]

# ------------------ Converter Helpers -----------------------

DOT_CHARS = r"\.․‥…·⋯"


def _normalize_whitespace(s: str) -> str:
    """Normalize whitespace: convert newlines/tabs to single spaces and trim."""
    if s is None:
        return ""
    # convert different newline styles to space
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    # collapse runs of whitespace (spaces, tabs, etc.)
    s = re.sub(r"[ \t\r\n]+", " ", s)
    return s.strip()


def _text_for_passage(tag: Tag) -> str:
    txt = tag.get_text(separator="\n", strip=True)
    return _normalize_whitespace(txt)


def _is_pseudo_heading(tag: Tag) -> bool:
    """Heuristic: short uppercase lines or single strong/b child => pseudo-heading."""
    if not isinstance(tag, Tag):
        return False
    name = tag.name.lower()
    if name not in ("p", "div", "li"):
        return False
    text = tag.get_text(separator=" ", strip=True)
    if not text:
        return False
    words = text.split()
    # compute uppercase fraction only over letters to avoid punctuation bias
    alpha_chars = [c for c in text if c.isalpha()]
    upper_frac = (
        (sum(1 for c in alpha_chars if c.isupper()) / max(1, len(alpha_chars)))
        if alpha_chars
        else 0.0
    )
    if len(words) <= 12 and upper_frac >= 0.4 and len(text) <= 120:
        return True
    # OR single strong child (allow nested tags inside strong)
    children = [c for c in tag.children if isinstance(c, (Tag, NavigableString))]
    child_tags = [c for c in children if isinstance(c, Tag)]
    if len(child_tags) == 1 and child_tags[0].name.lower() in (
        "strong",
        "b",
        "u",
        "mark",
    ):
        return True
    return False


def _is_table_marker(tag: Tag) -> bool:
    """Detect divs that represent replaced tables/images via attributes."""
    if not isinstance(tag, Tag):
        return False
    attrs = ("image_id", "image_name", "data-id", "data-name")
    return any(tag.has_attr(a) for a in attrs)


def _get_default_section_title(extracted_title: str) -> str:
    """Get default section title based on content."""
    if not extracted_title:
        return "body_content"
    if "table" in extracted_title.lower():
        return "table"
    else:
        return "body_content"


def _extract_section_title(text: str) -> Optional[str]:
    """
    Return a title string if 'text' qualifies as a section title, preferring
    uppercase words/phrases when present. If nothing is acceptable, return None.

    Logic:
    - Reject multi-line text (>1 line).
    - Try to find longest contiguous sequence of ALL-UPPERCASE words (with at least one letter).
      If found and not absurdly long, return that sequence as the title.
    - Else fall back to original single-line + <150 words rule and return the original text.
    - If original text doesn't meet fallback rules, return None.
    """
    if not text:
        return None

    # must be single logical line to be a section title
    lines = text.strip().split("\n")
    if len(lines) > 1:
        return None

    words = text.strip().split()
    # reject overly long candidate
    if len(words) >= 150:
        return None

    # Find contiguous uppercase groups.
    uppercase_groups = []
    current_group = []

    for w in words:
        # strip surrounding punctuation (keep internal characters)
        cleaned = re.sub(r"^[^\w]+|[^\w]+$", "", w)
        if not cleaned:
            if current_group:
                uppercase_groups.append(" ".join(current_group))
                current_group = []
            continue

        has_alpha = any(c.isalpha() for c in cleaned)
        is_all_upper = cleaned.upper() == cleaned

        if has_alpha and is_all_upper:
            current_group.append(cleaned)
        else:
            if current_group:
                uppercase_groups.append(" ".join(current_group))
                current_group = []

    if current_group:
        uppercase_groups.append(" ".join(current_group))

    if uppercase_groups:
        candidate = max(uppercase_groups, key=lambda s: len(s))
        if len(candidate.strip()) >= 2:
            return candidate.strip()

    # Fallback: if the original text passes single-line & length checks, return it
    return text.strip() if (len(words) < 150) else None


# ------------------------- HTML To BioC Converter --------------------------


def html_to_bioc_collection(
    html_content: str,
    doc_id: str,
    source: str = "Internal Documents",
    debug_verify: bool = True,
) -> BioCCollection:
    """
    Convert html_content (string) to a bioc.BioCCollection (in-memory).

    Changes compared to earlier version:
    - All BioC passages have offset = 0 (per user request).
    - Passage texts have all newlines removed and whitespace normalized.
    - The concatenated doc.text is created without newlines between passages (joined by single space).
    - Critical robustness improvements: sanitization of script/style/hidden elements and verification checks.
    """
    soup = BeautifulSoup(html_content, "lxml")
    body = soup.body if soup.body else soup

    # --- Sanitization: remove scripts/styles and hidden elements ---
    for s in soup(["script", "style"]):
        s.decompose()
    for hidden in soup.find_all(attrs={"hidden": True}):
        hidden.decompose()
    for ah in soup.find_all(attrs={"aria-hidden": "true"}):
        ah.decompose()

    # We'll accumulate passages as (type, title, text, metadata)
    passages: List[Tuple[str, str, str, dict]] = []

    current_heading = "body_content"
    current_paras: List[str] = []

    def flush_section():
        nonlocal current_heading, current_paras
        if current_paras:
            text = "\n\n".join(current_paras).strip()
            if text:
                passages.append(("section", current_heading, text, {}))
            current_paras = []

    # Iterative traversal in reading order (stack)
    stack = [iter([body])]
    while stack:
        try:
            node = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue

        if isinstance(node, NavigableString):
            if not node.strip():
                continue
            current_paras.append(_normalize_whitespace(str(node)))
            continue

        if not isinstance(node, Tag):
            continue

        tagname = node.name.lower()

        # real heading -> flush and set heading
        if tagname in ("h1", "h2", "h3", "h4", "h5", "h6"):
            flush_section()
            extracted_title = _text_for_passage(node)
            title_candidate = _extract_section_title(extracted_title)
            if title_candidate:
                current_heading = title_candidate
            else:
                current_heading = _get_default_section_title(extracted_title)
                if extracted_title:
                    current_paras.append(extracted_title)
            continue

        # table/image replacement markers (special handling)
        if _is_table_marker(node):
            flush_section()
            table_text = _text_for_passage(node)
            prov = {}
            for attr in (
                "image_id",
                "image_name",
                "data-id",
                "data-name",
                "data-caption",
            ):
                if node.has_attr(attr):
                    prov[attr] = node[attr]
            if node.has_attr("data-cell-map"):
                try:
                    prov["cell_map"] = json.loads(node["data-cell-map"])
                except Exception:
                    prov["cell_map"] = node["data-cell-map"]
            title = (
                prov.get("image_name")
                or prov.get("data-name")
                or prov.get("image_id")
                or "table"
            )
            if table_text:
                passages.append(("table", title, table_text, prov))
            continue

        # lists: treat each li as a paragraph in order (top-level li only)
        if tagname in ("ol", "ul"):
            for li in node.find_all("li", recursive=False):
                li_text = _text_for_passage(li)
                if not li_text:
                    continue
                if _is_pseudo_heading(li):
                    flush_section()
                    title_candidate = _extract_section_title(li_text)
                    if title_candidate:
                        current_heading = title_candidate
                    else:
                        current_heading = _get_default_section_title(li_text)
                        current_paras.append(li_text)
                else:
                    current_paras.append(li_text)
            continue

        # p / div / li general handling
        if tagname in ("p", "div", "li"):
            text = _text_for_passage(node)
            if not text:
                continue
            if _is_pseudo_heading(node):
                flush_section()
                title_candidate = _extract_section_title(text)
                if title_candidate:
                    current_heading = title_candidate
                else:
                    current_heading = _get_default_section_title(text)
                    current_paras.append(text)
            else:
                current_paras.append(text)
            continue

        # tables (if any remain)
        if tagname == "table":
            flush_section()
            ttext = _text_for_passage(node)
            if ttext:
                passages.append(("table", "table", ttext, {}))
            continue

        # otherwise descend into children to preserve reading order
        if list(node.contents):
            stack.append(iter(list(node.contents)))
            continue

        # fallback: if tag has text and no children
        txt = _text_for_passage(node)
        if txt:
            current_paras.append(txt)

    # final flush
    flush_section()

    # Build canonical document text and compute offsets (we'll set passage offsets = 0)
    doc_text_parts: List[str] = []
    passage_records = []

    for ptype, title, text, meta in passages:
        piece = text.strip() if text else ""
        passage_records.append((ptype, title, piece, meta))
        if piece:
            doc_text_parts.append(piece)

    # Join passages with a single space to ensure NO newlines between passages
    doc_text = _normalize_whitespace(" ".join(doc_text_parts)) if doc_text_parts else ""

    # Build BioC objects
    coll = BioCCollection()
    coll.source = source
    coll.date = date.today().isoformat()

    doc = BioCDocument()
    doc.id = doc_id
    # include concatenated document text for downstream tools (no newlines between passages)
    try:
        doc.text = doc_text
    except Exception:
        pass

    doc.infons["original_format"] = "html"

    # Add passages with offsets and infons
    for ptype, title, piece, meta in passage_records:
        # Normalize newline -> space and collapse whitespace for each passage when storing in BioC
        p_text = _normalize_whitespace(piece)

        # skip empty passages (unless metadata/provenance exists)
        if not p_text and not meta:
            continue

        p = BioCPassage()
        # Per user request: set all passage offsets to 0
        p.offset = 0

        p.infons["type"] = ptype
        if ptype == "section" and title:
            p.infons["section_title"] = title
        elif title:
            p.infons["title"] = title
        if meta:
            try:
                p.infons["provenance"] = json.dumps(meta, ensure_ascii=False)
            except Exception:
                p.infons["provenance"] = str(meta)

        # store sanitized single-line text in the BioC passage
        p.text = p_text

        # --- Verification: ensure no newline characters remain in p.text ---
        if debug_verify:
            if "\n" in p.text or "\r" in p.text:
                raise AssertionError(
                    f"Newline found in passage text for title='{title}' (type={ptype})."
                )
            # collapse multiple spaces defensively
            if re.search(r"\s{2,}", p.text):
                p.text = re.sub(r"\s+", " ", p.text).strip()

        doc.add_passage(p)

    # Final verification: ensure doc.text has no newlines as well
    if debug_verify and doc_text:
        if "\n" in doc_text or "\r" in doc_text:
            raise AssertionError("Newlines found in document-level text (doc.text).")

    coll.add_document(doc)
    return coll


# ------------------ Remove TOC like passages in BioC XML ------------------

# dot-like chars (handle ellipsis and similar)
DOT_CHARS = r"\.\u2024\u2025\u2026\u00B7\u22EF"
_dotted_page_re = re.compile(rf"^.+[{DOT_CHARS}\s-]+\d+\s*$", re.DOTALL)

# Regex for front-matter headings/lists (case-insensitive).
# Matches variations like:
#  - "Table of Contents"
#  - "Contents"
#  - "List of Tables"
#  - "List of In-Text Tables"
#  - "List of Figures" / "List of In-Text Figures"
_front_matter_pattern = r"""
    \b(                                      # word boundary then one of:
        (table\s+of\s+contents)              | # "table of contents"
        (contents)                           | # "contents"
        (list\s+of\s+(in[-\s]?text\s+)?tables?) | # "list of tables" or "list of in-text tables"
        (list\s+of\s+(in[-\s]?text\s+)?figures?)| # "list of figures" or "list of in-text figures"
        (list\s+of\s+images?)                   # "list of images"
    )\b
"""
_FRONT_MATTER_RE = re.compile(_front_matter_pattern, flags=re.IGNORECASE | re.VERBOSE)


def _normalize_text_for_match(text):
    """Collapse whitespace and normalize for regex matching."""
    if text is None:
        return ""
    return " ".join(text.split()).strip()


def is_toc_like(passage: BioCPassage) -> bool:
    """Heuristic to determine if a passage is TOC-like using its section title and text"""
    if passage.infons.get("type") != "section":
        return False
    text = passage.text
    text_norm = _normalize_text_for_match(text)
    title = passage.infons.get("section_title", "")
    title_norm = _normalize_text_for_match(title)
    dot_count = sum(1 for c in text_norm if c in DOT_CHARS)
    if not title_norm:
        return False
    # Check for front-matter patterns on passage titles
    # and dotted lines with page numbers on a passage text, but only if it contains enough dots
    if (
        # All 3 conditions should meet for stricter check
        (
            _FRONT_MATTER_RE.match(title_norm)
            and dot_count >= 100
            and _dotted_page_re.match(text_norm)
        )
        or
        # If the title doesn't match, the dot_count threshold is set to a higher value
        (dot_count >= 250 and _dotted_page_re.match(text_norm))
    ):
        return True
    return False


def remove_toc_passages(bioc_collection: BioCCollection) -> BioCCollection:
    """
    Remove TOC-like passages from a BioC XML file.
    """
    try:
        for doc in bioc_collection.documents:
            original_count = len(doc.passages)
            removed_passages = [p for p in doc.passages if is_toc_like(p)]
            doc.passages = [p for p in doc.passages if not is_toc_like(p)]
            removed_count = original_count - len(doc.passages)
            logger.info(
                f"Removed {removed_count} TOC-like passages from document: {doc.id}"
            )
        return bioc_collection, removed_passages
    except Exception as e:
        logger.error(f"Error removing TOC passages in BioC Collection: {e}")


# ----------------- Merge Small Passages Iteratively in BioC XML while preserving section_title -----------------

_EXEC_SUMMARY_RE = re.compile(r"^\s*executive\s+summary\s*$", re.IGNORECASE)


def _is_executive_summary_title(title: str) -> bool:
    """Regex-based check for 'executive summary' section titles."""
    if not title:
        return False
    return bool(_EXEC_SUMMARY_RE.match(title.strip()))


def _get_section_title_from_passage(passage) -> str:
    return passage.infons.get("section_title", "") if passage.infons else ""


def _normalize_whitespace_single_line(s: Optional[str]) -> str:
    """Normalize whitespace into a single-line string (no newlines)."""
    if not s:
        return ""
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"[ \t\r\n]+", " ", s)
    return s.strip()


def _try_merge_provenance(a: Optional[str], b: Optional[str]) -> Optional[str]:
    """Try to sensibly merge two provenance infons (JSON-aware), return JSON string or joined string."""
    if not a and not b:
        return None
    if a and not b:
        return a
    if b and not a:
        return b
    try:
        va = json.loads(a)
    except Exception:
        va = a
    try:
        vb = json.loads(b)
    except Exception:
        vb = b

    if isinstance(va, list) and isinstance(vb, list):
        combined = va + vb
    elif isinstance(va, list):
        combined = va + [vb]
    elif isinstance(vb, list):
        combined = [va] + vb
    else:
        combined = [va, vb]

    try:
        return json.dumps(combined, ensure_ascii=False)
    except Exception:
        return " | ".join(
            [
                _normalize_whitespace_single_line(str(va)),
                _normalize_whitespace_single_line(str(vb)),
            ]
        )


def merge_small_passages_in_collection(
    collection: bioc.BioCCollection,
    threshold_words: int = 100,
    max_iterations: int = 5,
    prefer_merge_with_next: bool = True,
) -> bioc.BioCCollection:
    for doc in collection.documents:
        iteration = 0
        changed = True

        while changed and iteration < max_iterations:
            iteration += 1
            changed = False
            old_passages = list(doc.passages)
            n = len(old_passages)
            i = 0
            new_passages = []

            while i < n:
                cur = old_passages[i]
                cur_text = _normalize_whitespace_single_line(cur.text)
                cur_count = len(cur_text.split())

                if cur_count >= threshold_words:
                    p = bioc.BioCPassage()
                    p.offset = getattr(cur, "offset", 0)
                    p.text = cur_text
                    p.infons = dict(cur.infons) if cur.infons else {}
                    p.annotations = (
                        list(cur.annotations) if hasattr(cur, "annotations") else []
                    )
                    p.relations = (
                        list(cur.relations) if hasattr(cur, "relations") else []
                    )
                    new_passages.append(p)
                    i += 1
                    continue

                # Start a run of small passages
                run_texts = [cur_text]
                run_indices = [i]
                run_titles = [cur.infons.get("section_title", "") if cur.infons else ""]
                j = i + 1
                while j < n:
                    nxt = old_passages[j]
                    nxt_text = _normalize_whitespace_single_line(nxt.text)
                    nxt_count = len(nxt_text.split())
                    if nxt_count < threshold_words:
                        run_texts.append(nxt_text)
                        run_indices.append(j)
                        run_titles.append(
                            nxt.infons.get("section_title", "") if nxt.infons else ""
                        )
                        j += 1
                        continue
                    break
                run_merged_text = _normalize_whitespace_single_line(
                    " ".join([t for t in run_texts if t])
                )
                run_word_count = len(run_merged_text.split())
                merged_section_title = (
                    " | ".join([t for t in run_titles if t]).strip() or "body_content"
                )

                if run_word_count >= threshold_words:
                    base = old_passages[run_indices[-1]]
                    p = bioc.BioCPassage()
                    p.offset = getattr(base, "offset", 0)
                    p.text = run_merged_text
                    p.infons = dict(base.infons) if base.infons else {}
                    p.infons["section_title"] = merged_section_title
                    p.infons["type"] = p.infons.get("type", "section")
                    prov = None
                    for idx in run_indices:
                        prov = _try_merge_provenance(
                            prov,
                            old_passages[idx].infons.get("provenance")
                            if old_passages[idx].infons
                            else None,
                        )
                    if prov:
                        p.infons["provenance"] = prov
                    p.annotations = []
                    p.relations = []
                    for idx in run_indices:
                        if (
                            hasattr(old_passages[idx], "annotations")
                            and old_passages[idx].annotations
                        ):
                            p.annotations.extend(old_passages[idx].annotations)
                        if (
                            hasattr(old_passages[idx], "relations")
                            and old_passages[idx].relations
                        ):
                            p.relations.extend(old_passages[idx].relations)
                    new_passages.append(p)
                    i = j
                    changed = True
                    continue

                next_idx = j if j < n else None
                prev_exists = len(new_passages) > 0

                if (
                    prefer_merge_with_next
                    and next_idx is not None
                    and not _is_executive_summary_title(
                        _get_section_title_from_passage(old_passages[next_idx])
                    )
                ):
                    nxt = old_passages[next_idx]
                    nxt_text = _normalize_whitespace_single_line(nxt.text)
                    merged_text = _normalize_whitespace_single_line(
                        run_merged_text + " " + nxt_text
                    )
                    base = nxt
                    p = bioc.BioCPassage()
                    p.offset = getattr(base, "offset", 0)
                    p.text = merged_text
                    p.infons = dict(base.infons) if base.infons else {}
                    # Collect all section titles from run and next
                    all_titles = [t for t in run_titles if t]
                    next_title = _get_section_title_from_passage(nxt)
                    if next_title:
                        all_titles.append(next_title)
                    merged_all_titles = " | ".join(all_titles).strip() or "body_content"
                    p.infons["section_title"] = merged_all_titles
                    p.infons["type"] = p.infons.get("type", "section")
                    prov = None
                    for idx in run_indices:
                        prov = _try_merge_provenance(
                            prov,
                            old_passages[idx].infons.get("provenance")
                            if old_passages[idx].infons
                            else None,
                        )
                    prov = _try_merge_provenance(
                        prov, nxt.infons.get("provenance") if nxt.infons else None
                    )
                    if prov:
                        p.infons["provenance"] = prov
                    p.annotations = []
                    p.relations = []
                    for idx in run_indices:
                        if (
                            hasattr(old_passages[idx], "annotations")
                            and old_passages[idx].annotations
                        ):
                            p.annotations.extend(old_passages[idx].annotations)
                        if (
                            hasattr(old_passages[idx], "relations")
                            and old_passages[idx].relations
                        ):
                            p.relations.extend(old_passages[idx].relations)
                    if hasattr(nxt, "annotations") and nxt.annotations:
                        p.annotations.extend(nxt.annotations)
                    if hasattr(nxt, "relations") and nxt.relations:
                        p.relations.extend(nxt.relations)

                    new_passages.append(p)
                    i = next_idx + 1
                    changed = True
                    continue

                if prev_exists:
                    prev = new_passages.pop()
                    prev_text = _normalize_whitespace_single_line(prev.text)
                    merged_text = _normalize_whitespace_single_line(
                        prev_text + " " + run_merged_text
                    )
                    merged_infons = dict(prev.infons) if prev.infons else {}
                    # Collect all section titles from prev and run
                    prev_title = (
                        prev.infons.get("section_title", "") if prev.infons else ""
                    )
                    all_titles = [prev_title] + [t for t in run_titles if t]
                    merged_all_titles = (
                        " | ".join([t for t in all_titles if t]).strip()
                        or "body_content"
                    )
                    merged_infons["section_title"] = merged_all_titles
                    merged_infons["type"] = merged_infons.get("type", "section")
                    prov = _try_merge_provenance(
                        prev.infons.get("provenance") if prev.infons else None, None
                    )
                    for idx in run_indices:
                        prov = _try_merge_provenance(
                            prov,
                            old_passages[idx].infons.get("provenance")
                            if old_passages[idx].infons
                            else None,
                        )
                    if prov:
                        merged_infons["provenance"] = prov
                    merged_annotations = (
                        list(prev.annotations) if hasattr(prev, "annotations") else []
                    )
                    merged_relations = (
                        list(prev.relations) if hasattr(prev, "relations") else []
                    )
                    for idx in run_indices:
                        if (
                            hasattr(old_passages[idx], "annotations")
                            and old_passages[idx].annotations
                        ):
                            merged_annotations.extend(old_passages[idx].annotations)
                        if (
                            hasattr(old_passages[idx], "relations")
                            and old_passages[idx].relations
                        ):
                            merged_relations.extend(old_passages[idx].relations)
                    p = bioc.BioCPassage()
                    p.offset = getattr(prev, "offset", 0)
                    p.text = merged_text
                    p.infons = merged_infons
                    p.annotations = merged_annotations
                    p.relations = merged_relations
                    new_passages.append(p)
                    i = j
                    changed = True
                    continue

                p = bioc.BioCPassage()
                p.offset = getattr(old_passages[run_indices[-1]], "offset", 0)
                p.text = run_merged_text
                p.infons = (
                    dict(old_passages[run_indices[-1]].infons)
                    if old_passages[run_indices[-1]].infons
                    else {}
                )
                p.infons["section_title"] = merged_section_title
                p.infons["type"] = p.infons.get("type", "section")
                p.annotations = []
                p.relations = []
                for idx in run_indices:
                    if (
                        hasattr(old_passages[idx], "annotations")
                        and old_passages[idx].annotations
                    ):
                        p.annotations.extend(old_passages[idx].annotations)
                    if (
                        hasattr(old_passages[idx], "relations")
                        and old_passages[idx].relations
                    ):
                        p.relations.extend(old_passages[idx].relations)
                new_passages.append(p)
                i = j

            # End of while i < n
            doc.passages = new_passages

    return collection


# ----------------- BioC Collection Cleaners -----------------


def _repeat_unescape(s: str, max_rounds: int = 3) -> str:
    """Apply html.unescape repeatedly until string stabilizes or max_rounds reached."""
    prev = s
    for _ in range(max_rounds):
        cur = html.unescape(prev)
        if cur == prev:
            break
        prev = cur
    return prev


def _remove_control_chars(s: str) -> str:
    """Remove C0 control characters (keep common printable whitespace)."""
    # allow tabs/spaces (we'll normalize whitespace later), remove other C0 controls
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)


def _normalize_whitespace_single_line(s: Optional[str]) -> str:
    """Normalize whitespace to a single line (no newlines), collapse multiple spaces to one."""
    if not s:
        return ""
    # Replace NBSP and other non-breaking spaces with normal space
    s = s.replace("\u00A0", " ")
    s = s.replace("\u200B", "")  # zero-width space -> remove
    # convert \r\n,\r,\n to spaces
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    # collapse whitespace
    s = re.sub(r"[ \t\r\n]+", " ", s)
    return s.strip()


def _clean_string(s: Optional[str]) -> str:
    """
    Full cleaning pipeline for a single string:
    - ensure str, repeated html unescape (fixes &amp; &amp;#x2013; etc.)
    - unicode normalization (NFKC)
    - remove control characters
    - normalize whitespace to a single line
    """
    import html

    if s is None:
        return ""
    s = str(s)
    # Remove HTML specific tags/entities if any
    s = html.unescape(s)
    # run unescape repeated times (to handle double-encoded)
    s = _repeat_unescape(s, max_rounds=4)
    # unicode normalization
    s = unicodedata.normalize("NFKC", s)
    # remove control characters that sneak in from encodings
    s = _remove_control_chars(s)
    # final unescape (in case normalization reintroduced entities)
    s = html.unescape(s)
    # manually replace the &amp;
    s = s.replace("&amp;", "&")
    # normalize whitespace (single-line)
    s = _normalize_whitespace_single_line(s)
    return s


# ----------------- Bioc-level cleaners -----------------


def clean_passage(passage: bioc.BioCPassage, preserve_original: bool = True):
    """
    Clean a single BioCPassage in-place:
    - Clean passage.text
    - Clean passage.infons['section_title'] if present
    - Optionally store originals in infons 'orig_text' and 'orig_section_title'
    """
    # Clean text
    orig_text = getattr(passage, "text", "")
    cleaned_text = _clean_string(orig_text)
    if preserve_original and cleaned_text != orig_text:
        # store original if not already stored
        if "orig_text" not in passage.infons:
            passage.infons["orig_text"] = orig_text or ""
    passage.text = cleaned_text

    # Clean section_title in infons
    if passage.infons and "section_title" in passage.infons:
        orig_title = passage.infons.get("section_title")
        cleaned_title = _clean_string(orig_title)
        if preserve_original and cleaned_title != orig_title:
            if "orig_section_title" not in passage.infons:
                passage.infons["orig_section_title"] = orig_title or ""
        passage.infons["section_title"] = cleaned_title


def clean_all_infons(passage: bioc.BioCPassage, preserve_original: bool = True):
    """
    Optional helper: clean all infon values on a passage (not just section_title).
    Useful if infons were scraped from HTML and also contain entities.
    """
    if not passage.infons:
        return
    for k, v in list(passage.infons.items()):
        if v is None:
            continue
        # skip provenance if it's JSON we don't want to mangle
        if k == "provenance":
            continue
        cleaned = _clean_string(v)
        if (
            preserve_original
            and cleaned != v
            and f"orig_infon_{k}" not in passage.infons
        ):
            passage.infons[f"orig_infon_{k}"] = v
        passage.infons[k] = cleaned


def clean_bioc_collection(
    collection: bioc.BioCCollection,
    preserve_original: bool = True,
    clean_infons: bool = False,
) -> bioc.BioCCollection:
    """
    Clean all passages in a loaded BioC collection in-place.
    - preserve_original: store original text/title in infons before overwriting.
    - clean_infons: if True, also clean all infon values (except provenance).
    Returns the same collection object for convenience.
    """
    for doc in collection.documents:
        for passage in doc.passages:
            clean_passage(passage, preserve_original=preserve_original)
            if clean_infons:
                clean_all_infons(passage, preserve_original=preserve_original)
    return collection


# ----------------- Writing -----------------


def html_to_bioc_file(
    html_path: str, xml_path: str, file_handler, debug_verify: bool = True
) -> int:
    """
    Read html_path using your file_handler.read_file(path) -> str,
    write bioc xml to xml_path using file_handler.write_file(path, content).
    """
    html_content = file_handler.read_file(html_path)
    doc_id = html_path.split("/")[-1].rsplit(".", 1)[0]

    # Convert HTML to BioC collection
    coll = html_to_bioc_collection(
        html_content,
        doc_id=doc_id,
        source="Internal Documents",
        debug_verify=debug_verify,
    )

    # clean text/infons
    col_cleaned = clean_bioc_collection(
        collection=coll, preserve_original=False, clean_infons=True
    )

    # remove TOC passages
    col_with_toc_removed, removed_passages_for_toc = remove_toc_passages(col_cleaned)

    # Save the removed TOC passages if needed (for debugging)
    if removed_passages_for_toc:
        toc_debug_path = html_path.rsplit(".", 1)[0] + "_removed_toc_passages.json"
        toc_data = []
        for p in removed_passages_for_toc:
            toc_data.append(
                {
                    "section_title": p.infons.get("section_title", ""),
                    "text": p.text,
                    "infons": p.infons,
                }
            )
        file_handler.write_file(
            toc_debug_path, json.dumps(toc_data, ensure_ascii=False, indent=2)
        )
        logger.info(f"Saved removed TOC passages to {toc_debug_path}")

    # merge small passages
    col_with_merged_passages = merge_small_passages_in_collection(
        collection=col_with_toc_removed,
        threshold_words=100,
        max_iterations=5,
        prefer_merge_with_next=True,
    )

    # Make sure the output directory exists
    os.makedirs(os.path.dirname(xml_path), exist_ok=True)

    # ToDo: Use the file_handler to write the file instead of direct open/write

    # Attempt to dump with bioc.dump (common API)
    try:
        with open(xml_path, "w", encoding="utf-8") as outf:
            bioc.dump(col_with_merged_passages, outf)
        logger.info(f"Generated BioC XML {xml_path}")
        return 1
    except Exception:
        # Fallback: use the collection.to_xml() if available or manual fallback
        try:
            xml_str = col_with_merged_passages.to_xml()  # some bioc versions have this
            with open(xml_path, "w", encoding="utf-8") as outf:
                outf.write(xml_str)
            logger.info(f"Generated BioC XML {xml_path}")
            return 1
        except Exception:
            # As last resort, use bioc.BioCXMLWriter if available
            try:
                with open(xml_path, "w", encoding="utf-8") as outf:
                    writer = bioc.BioCXMLWriter(outf)
                    writer.write_collection(col_with_merged_passages)
                    writer.close()
                    logger.info(f"Generated BioC XML {xml_path}")
                    return 1
            except Exception as e:
                logger.warning(f"Failed to write BioC XML {xml_path}: {e}")
                return 0


def convert_rfd_html_to_bioc(
    rfd_interim_path: str,
    bioc_path: str,
):
    converted_articles_count = 0
    for rfd_html_dir in os.listdir(rfd_interim_path):
        rfd_html_dir_path = Path(rfd_interim_path) / rfd_html_dir
        rfd_html_file_path = rfd_html_dir_path / (rfd_html_dir + ".html")
        rfd_html_file_name = rfd_html_dir + ".html"
        rfd_bioc_xml_file_path = Path(bioc_path) / (rfd_html_dir + ".xml")
        if os.path.exists(rfd_html_file_path):
            logger.info(f"HTML file found: {rfd_html_file_name}")
            logger.info(f"Converting {rfd_html_file_name} -> BioC XML")
            conversion_status = html_to_bioc_file(
                html_path=str(rfd_html_file_path),
                xml_path=str(rfd_bioc_xml_file_path),
                file_handler=file_handler,
            )
            converted_articles_count += conversion_status
        else:
            logger.warning(
                f"HTML file not found: {rfd_html_file_name}, skipping conversion."
            )
    return converted_articles_count
