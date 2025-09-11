import pymupdf
import re
from datetime import datetime
import time
import random
import requests
from typing import Optional, Dict, Any

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

"""
Key functions provided:
- fetch_api_metadata(...) -> Optional[Dict]: fetches DOI metadata (CSL JSON or CrossRef)
with retries, backoff and Retry-After handling.
- extract_preprint_metadata_simple(...) -> Dict: scans a PDF with PyMuPDF, accumulates
doi/posted_date/license across blocks, attempts API fetch when a DOI is found,
writes the final collected metadata JSON and returns the result.
- preprints_articles_metadata_extractor(...) -> wrapper that exposes your original
integration point.
"""


def _normalize_doi_core(doi: str) -> str:
    """Return the core DOI (10.x/...) from either a DOI string or doi.org URL."""
    if not doi:
        return ""
    doi = doi.strip()
    if doi.lower().startswith("http"):
        parts = doi.split("/", 3)
        if len(parts) >= 4:
            return parts[3]
        return doi
    return doi


def fetch_api_metadata(
    doi: str,
    timeout: int = 10,
    max_retries: int = 4,
    backoff_factor: float = 0.8,
) -> Optional[Dict[str, Any]]:
    """
    Fetch metadata for a DOI using content-negotiation (CSL JSON) first, then CrossRef REST.
    Returns: {"source": "csl"|"crossref", "status_code": int, "data": <parsed json dict>} on success,
             None on failure.

    Retries on transient failures (5xx) and 429 (honors Retry-After). Uses exponential backoff + jitter.
    """

    doi_core = _normalize_doi_core(doi)
    # print(doi_core)
    if not doi_core:
        logger.error("fetch_api_metadata: empty DOI")
        return None

    # build polite User-Agent (include mailto if provided)
    ua = "metadata-fetcher/1.0"

    session = requests.Session()
    session.headers.update({"User-Agent": ua})

    def _sleep_for_attempt(attempt: int, retry_after: Optional[float] = None):
        if retry_after is not None and retry_after > 0:
            logger.info(f"Sleeping Retry-After {retry_after:.1f}s")
            time.sleep(retry_after)
            return
        # exponential backoff with jitter
        base = backoff_factor * (2**attempt)
        jitter = random.uniform(0, min(1.0, base * 0.5))
        sleep_time = base + jitter
        logger.info(f"Sleeping backoff {sleep_time:.2f}s (attempt {attempt})")
        time.sleep(sleep_time)

    # -- 1) Try content-negotiation CSL-JSON at doi.org --
    csl_url = f"https://doi.org/{doi_core}"
    csl_headers = {"Accept": "application/vnd.citationstyles.csl+json"}
    for attempt in range(max_retries):
        try:
            logger.info(f"CSL attempt {attempt+1}/{max_retries} -> {csl_url}")
            resp = session.get(csl_url, headers=csl_headers, timeout=timeout)
            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                    return data
                except ValueError:
                    # Non-JSON response - treat as failure and break to fallback
                    logger.info(
                        "CSL returned non-JSON payload; will fallback to CrossRef."
                    )
                    break
            elif status == 429:
                # Rate limited; honor Retry-After if present
                retry_after = None
                if "Retry-After" in resp.headers:
                    try:
                        retry_after = float(resp.headers["Retry-After"])
                    except Exception:
                        # could be HTTP-date; ignore for simplicity
                        retry_after = None
                _sleep_for_attempt(attempt, retry_after=retry_after)
                continue
            elif 500 <= status < 600:
                # transient server error -> retry
                _sleep_for_attempt(attempt)
                continue
            else:
                # 4xx other than 429: treat as non-retryable
                logger.info(
                    f"CSL request returned status {status}; not retrying this endpoint."
                )
                break
        except requests.RequestException as e:
            logger.info(f"CSL request exception: {e!r}")
            _sleep_for_attempt(attempt)
            continue

    # -- 2) Fallback: CrossRef REST API --
    crossref_url = f"https://api.crossref.org/works/{doi_core}"
    # CrossRef prefers a contact in UA; include mailto if available
    for attempt in range(max_retries):
        try:
            logger.info(f"CrossRef attempt {attempt+1}/{max_retries} -> {crossref_url}")
            resp = session.get(crossref_url, timeout=timeout)
            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                    # CrossRef wraps actual message under 'message'
                    message = data.get("message") if isinstance(data, dict) else data
                    return message
                except ValueError:
                    logger.info("CrossRef returned non-JSON payload.")
                    return None
            elif status == 429:
                retry_after = None
                if "Retry-After" in resp.headers:
                    try:
                        retry_after = float(resp.headers["Retry-After"])
                    except Exception:
                        retry_after = None
                _sleep_for_attempt(attempt, retry_after=retry_after)
                continue
            elif 500 <= status < 600:
                _sleep_for_attempt(attempt)
                continue
            else:
                logger.info(f"CrossRef returned status {status}; not retrying.")
                break
        except requests.RequestException as e:
            logger.info(f"CrossRef request exception: {e!r}")
            _sleep_for_attempt(attempt)
            continue

    logger.info("fetch_api_metadata: all attempts failed for both CSL and CrossRef.")
    return None


# --- helpers / regexes ---
_RE_CONTROL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")

DOI_URL_RE = re.compile(r"https?://doi\.org/(10\.\d{4,9}/\S+)", flags=re.I)
DOI_RE = re.compile(r"\bdoi[:\s]*?(10\.\d{4,9}/\S+)", flags=re.I)

# Dates like "April 14, 2020" or "14 April 2020" or ISO "2020-04-14"
DATE_RE_MONTHDAY = re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})\b")
DATE_RE_DAYMONTH = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b")
DATE_ISO_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
DATE_POSTED_RE = re.compile(
    r"\bthis version posted\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", flags=re.I
)

# Simple CC-BY / Creative Commons variants (capture a short surrounding context)
LICENSE_RE = re.compile(
    r"((?:CC(?:-| )?BY(?:[-\s]*\d(?:\.\d)?)?(?:\s+International)?(?:\s+license)?)|(?:Creative Commons(?: Attribution)?(?:\s+[^\n\r]{0,40})?))",
    flags=re.I,
)


# --- small utility functions ---
def _clean_block(s: str) -> str:
    """Remove illegal control characters commonly introduced by PDF parsing."""
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    # remove illegal control chars often found in PDF extraction
    s = _RE_CONTROL.sub("", s)
    return s.strip()


def _normalize_doi_from_match(s: str) -> str:
    """Return a normalized DOI URL like https://doi.org/10.x/... or None."""
    if not s:
        return None
    s = s.strip()
    m = DOI_URL_RE.search(s)
    if m:
        doi_core = m.group(1).rstrip(").,;:")
        return "https://doi.org/" + doi_core
    m2 = DOI_RE.search(s)
    if m2:
        doi_core = m2.group(1).rstrip(").,;:")
        return "https://doi.org/" + doi_core
    # fallback: look for 10.x/... anywhere
    m3 = re.search(r"(10\.\d{4,9}/\S+)", s)
    if m3:
        return "https://doi.org/" + m3.group(1).rstrip(").,;:")
    return None


def _try_parse_date(s: str, _depth: int = 0) -> Optional[str]:
    """
    Try several common date formats and return an ISO date (YYYY-MM-DD) or None.

    This version guards against runaway recursion by limiting recursive calls
    to a maximum depth of 5. If parsing requires more than 5 recursive attempts,
    it will return None (treating the string as non-date).
    """
    if not s:
        return None

    # recursion-depth guard: stop after 5 attempts
    if _depth >= 5:
        # avoid deep recursion and return None so caller moves on
        return None
    s = s.strip().rstrip(".,;:")
    # direct ISO
    m_iso = DATE_ISO_RE.search(s)
    if m_iso:
        return m_iso.group(1)
    # try common formats
    fmts = ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # try to extract a month-day-year substring and parse it
    m = DATE_RE_MONTHDAY.search(s)
    if m:
        # recurse but increase depth
        return _try_parse_date(m.group(1), _depth=_depth + 1)
    m2 = DATE_RE_DAYMONTH.search(s)
    if m2:
        # recurse but increase depth
        return _try_parse_date(m2.group(1), _depth=_depth + 1)
    return None


# --- main extraction function ---
def extract_from_block(block_text: str, collected: dict):
    """
    Extract DOI, posted_date and license from a single block of text.
    Returns a dict: {"doi":..., "posted_date":..., "license":...}
    """
    raw = _clean_block(block_text)
    result = {"doi": None, "posted_date": None, "license": None, "raw": raw}

    if not raw:
        return result

    if collected.get("doi") is None:
        # DOI: try url first, then doi: pattern, then general 10.x pattern
        doi = _normalize_doi_from_match(raw)
        if doi:
            result["doi"] = doi

    if collected.get("posted_date") is None:
        # Posted date: prefer the explicit 'this version posted' phrasing if present
        m_posted = DATE_POSTED_RE.search(raw)
        if m_posted:
            parsed = _try_parse_date(m_posted.group(1))
            if parsed:
                result["posted_date"] = parsed
        # otherwise try month-day, day-month, or ISO anywhere
        if not result["posted_date"]:
            m_mdy = DATE_RE_MONTHDAY.search(raw)
            if m_mdy:
                parsed = _try_parse_date(m_mdy.group(1))
                if parsed:
                    result["posted_date"] = parsed
        if not result["posted_date"]:
            m_iso = DATE_ISO_RE.search(raw)
            if m_iso:
                result["posted_date"] = m_iso.group(1)

    if collected.get("license") is None:
        # License: CC-BY or Creative Commons variants (first match)
        m_lic = LICENSE_RE.search(raw)
        if m_lic:
            lic = m_lic.group(1).strip()
            # normalize spacing and common forms
            lic_norm = re.sub(r"\s+", " ", lic).strip()
            # uppercase CC-BY forms to consistent representation
            lic_norm = re.sub(r"cc[\s-]*by", "CC-BY", lic_norm, flags=re.I)
            # ensure version like 4 -> 4.0
            lic_norm = re.sub(r"(CC-BY[\s-]*4)(?!\.)$", r"\1.0", lic_norm)
            result["license"] = lic_norm

    return result


def extract_preprint_metadata_simple(
    pdf_path: str,
    out_json: str,
    file_handler: FileHandler,
    write_to_s3: bool,
    s3_metadata_path: str,
    s3_file_handler: FileHandler,
    api_timeout: int = 8,
) -> Dict[str, Any]:
    """
    Scan PDF, collect DOI/posted_date/license from blocks, optionally call API.
    Behavior:
    - Accumulate fields across blocks (do not stop at the first DOI-only match).
    - If a DOI is found, attempt fetch_api_metadata(doi). The API result is not used to
    change the outward JSON schema; it is attempted for enrichment only (caller may
    choose to use API results separately).
    - Returns a small dict: {"preprint_id":..., "doi":..., "posted_date":..., "license":...}
    """
    preprint_id = pdf_path.split("/")[-1].split(".")[0]

    doc = pymupdf.open(pdf_path)
    n_pages = doc.page_count

    collected = {"doi": None, "posted_date": None, "license": None}

    def _col_update(field: str, value: Optional[str]):
        if not value:
            return
        if collected.get(field) is None:
            collected[field] = value

    # scan pages & blocks, accumulate fields (stop early only if all 3 found)
    for pidx in range(n_pages):
        page = doc.load_page(pidx)
        blocks = page.get_text("blocks")
        if not blocks:
            continue
        for b in blocks:
            if not isinstance(b, (list, tuple)) or len(b) < 5:
                continue
            raw = b[4] or ""
            raw_clean = _clean_block(raw)
            if not raw_clean:
                continue
            quick_lower = raw_clean.lower()
            if not (
                "doi" in quick_lower
                or "doi.org" in quick_lower
                or "posted" in quick_lower
                or "cc-by" in quick_lower
                or "creative commons" in quick_lower
                or "biorxiv" in quick_lower
                or "preprint" in quick_lower
            ):
                continue
            meta = extract_from_block(raw_clean, collected)
            _col_update("doi", meta.get("doi"))
            _col_update("posted_date", meta.get("posted_date"))
            _col_update("license", meta.get("license"))
            if collected["doi"] and collected["posted_date"] and collected["license"]:
                logger.info(f"Found all fields by page {pidx+1}; stopping scan.")
                break
        else:
            continue
        break

    doc.close()

    title = preprint_id
    # If we discovered a DOI, try API fetch; if API returns data, return api+collected; else return collected
    if collected.get("doi"):
        logger.info(
            f"DOI found; attempting fetch_api_metadata for {collected.get('Doi')}"
        )
        api_data = fetch_api_metadata(collected.get("doi"), timeout=api_timeout)
        if api_data:
            # combine api result with the collected PDF values and return that composite
            final_out = {"preprint_id": preprint_id, **api_data, **collected}
            # title = final_out["title"][0] if isinstance(final_out.get("title"), list) else final_out.get("title")
        else:
            logger.info("API returned no data; falling back to PDF-collected metadata.")
            final_out = {"preprint_id": preprint_id, **collected, "title": title}
    else:
        # no DOI found -> return collected only
        final_out = {"preprint_id": preprint_id, **collected, "title": title}

    try:
        file_handler.write_file_as_json(out_json, final_out)
        logger.info(f"For preprint_id {preprint_id}, Saving metadata to {out_json}")
        logger.info(f"Finished processing for {pdf_path}")
    except Exception as e:
        logger.error("Failed to write metadata JSON:", repr(e))

    if write_to_s3:
        s3_filename = pdf_path.split("/")[-1].replace(".pdf", "_metadata.json")
        s3_file_path = s3_file_handler.get_file_path(s3_metadata_path, s3_filename)
        s3_file_handler.write_file_as_json(s3_file_path, final_out)
        logger.info(
            f"For preprint_id {preprint_id}, Saving metadata to S3: {s3_file_path}"
        )

    # metadata_for_bioc = {"preprint_id": preprint_id, **collected, "title": title, "abstract": abstract}

    return final_out


def preprints_articles_metadata_extractor(
    preprint_file_path: str,
    article_metadata_path: str,
    file_handler: FileHandler,
    write_to_s3: bool,
    s3_metadata_path: str,
    s3_file_handler: FileHandler,
):
    # preprint_file_path is a full path
    metadata_json_file_name = preprint_file_path.split("/")[-1].replace(
        ".pdf", "_metadata.json"
    )
    metadata_path = file_handler.get_file_path(
        article_metadata_path, metadata_json_file_name
    )
    metadata_for_bioc = extract_preprint_metadata_simple(
        preprint_file_path,
        metadata_path,
        file_handler,
        write_to_s3,
        s3_metadata_path,
        s3_file_handler,
    )
    return metadata_for_bioc
