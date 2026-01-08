import re
import unicodedata
from datetime import datetime
from pathlib import PurePosixPath
from typing import List, Optional, Dict

from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.data_ingestion.ingestion_utils.document_data_insertion import insert_document_data

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

# ----- CONFIG / CATEGORY MAP -----
CATEGORY_MAP = {
    "Word": {".docx", ".doc"},
    "Excel": {".xlsx", ".xls"},
    "PowerPoint": {".pptx", ".ppt", ".pptm"},
    "GraphPad Prism": {".pzfx", ".prism"},
    "Archive": {".zip", ".gz", ".vortexgz"},
    "Scientific": {
        ".vortex",
        ".sdf",
        ".sd",
        ".sda",
        ".wsp",
        ".cdx",
        ".cdxml",
        ".cif",
        ".pdb",
        ".mdb",
    },
    "Text/Tabular": {".csv", ".dsv", ".rtf", ".txt"},
    "Image": {".png", ".jpg", ".jpeg", ".tif", ".tiff"},
    "Media": {".mp4", ".fcs", ".raw"},
    "PDF": {".pdf"},
    "Email": {".msg"},
    "Misc": {".asc"},
}

TEMP_PREFIXES = ("~$", ".DS_Store", "Thumbs.db")
TEMP_EXTS = {".tmp", ".db", ".lnk"}


# ---------------------
# Cleaning helpers
# ---------------------


def clean_path_str(s: str) -> str:
    """
    Normalize unicode and remove invisible / nuisance characters that often break regexes.
    Also normalise common single-quote/apostrophe characters to hyphen so ID forms like
    "GS'9598" or "GS’9598" become "GS-9598" and are detected as IDs (not dates).
    """
    if not isinstance(s, str):
        return s

    s = unicodedata.normalize("NFKC", s)

    # remove invisible nuisance characters
    for ch in ("\u00AD", "\u200B", "\u200C", "\uFEFF", "\u00A0"):
        s = s.replace(ch, "")

    # various dashes -> hyphen
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", s)

    # normalize curly quotes and straight apostrophes to hyphen (so GS'9598 -> GS-9598)
    s = re.sub(r"[’‘`']", "-", s)

    # collapse repeated whitespace around slashes and hyphens to single space where helpful
    s = re.sub(r"\s*[/\\]\s*", "/", s)
    s = re.sub(r"\s*-\s*", "-", s)

    return s


# ---------------------
# Regexes to find tokens
# ---------------------
MONTHS = (
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
)
RE_DATE_ISO_SEP = re.compile(r"\b(20\d{2}[-_/]\d{1,2}[-_/]\d{1,2})\b")
RE_DATE_US_SEP = re.compile(r"\b(\d{1,2}[-_/]\d{1,2}[-_/]\d{2,4})\b")
RE_DATE_COMPACT = re.compile(r"\b(20\d{6}|\d{6,8})\b")
RE_MONTH_DAY_YEAR = re.compile(
    r"(" + MONTHS + r")\s*(\d{1,2})\d{0,12}(20\d{2})", re.IGNORECASE
)
RE_DAY_MONTH_YEAR = re.compile(
    r"\b(\d{1,2})\s*(" + MONTHS + r")\s*(20\d{2})", re.IGNORECASE
)
RE_MONTH_YEAR = re.compile(r"\b(" + MONTHS + r")[- _]?(20\d{2})\b", re.IGNORECASE)
RE_YEAR_MONTH = re.compile(r"\b(20\d{2})[-_/ ]?(\d{1,2})\b")
RE_YEAR_ONLY = re.compile(r"\b([12]\d{3})\b")

RE_EXP = re.compile(
    r"\b(GS-\d{5,8}|G-\d{3,8}-\d+|PC-\d{3,8}(?:-\d{1,4})?)\b", re.IGNORECASE
)
RE_COMPOUND = re.compile(
    r"\b(GS-\d{5,8}|G-\d{3,8}|LEO[0-9A-Z]+|PC-\d{3,8})\b", re.IGNORECASE
)
SPECIES_KW = re.compile(
    r"\b(mouse|rat|pbmc|mice|human|specimen|MOUSE\d?)\b", re.IGNORECASE
)
MEETING_KW = re.compile(
    r"\b(CTM|PTM|MedChem|ChemMeet|Chemistry meetings|Meetings|RRC|PPI|SrStaff|Weekly|Brainstorm|V-RRC|PTM AC|CTM|CTM_DMPK)\b",
    re.IGNORECASE,
)
ASSAY_KW = re.compile(
    r"\b(HNMR|Western Blot|PKPD|PK summary|PK|DMPK|proteomics|QC|flowjo|flow|Specimen|3D_PSA|titration|Blot|NEOSphere|ELISA|Cryo|Cryo-EM|LCMS|AssayDevelopment|Assay|Degrader|MSD)\b",
    re.IGNORECASE,
)


# ---------------------
# Date parsing heuristics (unchanged)
# ---------------------


def try_parse_numeric_date(token: str) -> Optional[str]:
    s = re.sub(r"\D", "", token)
    L = len(s)

    if L == 8 and s.startswith("20"):
        try:
            return datetime.strptime(s, "%Y%m%d").date().isoformat()
        except ValueError:
            pass

    if L == 6:
        mm = int(s[:2])
        dd = int(s[2:4])
        yy = int(s[4:6])
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            year = 2000 + yy
            try:
                return datetime(year, mm, dd).date().isoformat()
            except ValueError:
                pass

        yy2 = int(s[:2])
        mm2 = int(s[2:4])
        dd2 = int(s[4:6])
        try:
            year = 2000 + yy2
            return datetime(year, mm2, dd2).date().isoformat()
        except Exception:
            pass

    if L in (7, 8):
        for m_len in (1, 2):
            try:
                month = int(s[:m_len])
                day = int(s[m_len : m_len + 2])
                year = int(s[m_len + 2 :])
                if year < 100:
                    year += 2000
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return datetime(year, month, day).date().isoformat()
            except Exception:
                pass

    return None


def parse_date_token(token: str) -> Optional[str]:
    t = token.strip().strip(".,_[]()")
    formats = (
        "%Y-%m-%d",
        "%Y_%m_%d",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%m_%d_%Y",
        "%m-%d-%y",
        "%m/%d/%y",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d-%B-%Y",
        "%d %B %Y",
        "%d %b %Y",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(t, fmt)
            return dt.date().isoformat()
        except Exception:
            pass

    return try_parse_numeric_date(t)


# ---------------------
# Helper: avoid numeric matches that are really IDs (e.g. "GS-1608852", "PC-007-2043", "ABC12345")
# ---------------------


def _looks_like_id_context(full_text: str, start: int, end: int) -> bool:
    """
    Return True when the numeric match at full_text[start:end] looks like it's part of an ID,
    e.g. preceded by short alpha prefix + hyphen (GS-1608852, PC-007-2043), or immediately adjacent
    to letters. This is intentionally conservative to avoid false positives.
    """
    L = len(full_text)

    # look back up to 5 chars
    lb_start = max(0, start - 5)
    left_context = full_text[lb_start:start]

    # common pattern: letters (1-4) then hyphen/underscore immediately before digits => ID
    if re.search(r"[A-Za-z]{1,4}[-_]\s*$", left_context):
        return True

    # if immediate previous char is a letter => probably alnum-id like "ABC12345"
    if start > 0 and full_text[start - 1].isalpha():
        return True

    # look right up to 5 chars
    right_context = full_text[end : end + 5]

    # if digits are followed by hyphen+letters/digits or immediate letters => ID-like
    if re.match(r"^[-_][A-Za-z0-9]{1,4}", right_context):
        return True
    if right_context and right_context[0].isalpha():
        return True

    return False


# ---------------------
# Reject implausible parsed years (tiny safety guard)
# ---------------------


def _iso_year_plausible_current(iso_date_str: str) -> bool:
    """
    Return True if iso_date_str (YYYY-MM-DD) has a year between 1900 and current year (inclusive).
    """
    if not iso_date_str or not isinstance(iso_date_str, str):
        return False
    try:
        year = int(iso_date_str[:4])
    except Exception:
        return False
    current_year = datetime.now().year
    return 1900 <= year <= current_year


# ---------------------
# extract_all_dates (unchanged)
# ---------------------


def extract_all_dates(full_path: str) -> List[Dict[str, Optional[str]]]:
    s = clean_path_str(full_path)
    tokens: List[str] = []

    # Use finditer so we can inspect match spans and skip tokens that are clearly part of IDs
    for rx in (RE_DATE_ISO_SEP, RE_DATE_US_SEP, RE_DATE_COMPACT):
        for m in rx.finditer(s):
            # matched group usually in group(1)
            if m.groups():
                token = m.group(1)
                try:
                    start, end = m.span(1)
                except Exception:
                    start, end = m.span(0)
            else:
                token = m.group(0)
                start, end = m.span(0)

            # skip if this numeric token looks like part of an ID (GS-..., PC-..., ABC123...)
            if _looks_like_id_context(s, start, end):
                continue

            if token:
                tokens.append(token)

    # month/day/year style matches and other month-name matches (these include letters — safe)
    for m in RE_MONTH_DAY_YEAR.findall(s):
        tokens.append(" ".join([m[0], m[1], m[2]]))
    for m in RE_DAY_MONTH_YEAR.findall(s):
        tokens.append(" ".join([m[0], m[1], m[2]]))
    for m in RE_MONTH_YEAR.findall(s):
        tokens.append(" ".join([m[0], m[1]]))
    for m in RE_YEAR_MONTH.findall(s):
        tokens.append(f"{m[0]}-{m[1]}")

    # uniquify while preserving order
    seen = set()
    uniq: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            uniq.append(t)

    parsed: List[Dict[str, Optional[str]]] = []
    for t in uniq:
        parsed.append({"raw": t, "iso": parse_date_token(t)})

    # Finally, avoid picking up bare long digit runs that are actually part of IDs.
    # Use finditer so we can inspect adjacency and skip ID-adjacent numeric runs.
    for m in re.finditer(r"\b\d{5,8}\b", s):
        token = m.group(0)
        start, end = m.span(0)
        # skip sequences that are adjacent to letters/hyphen patterns (IDs)
        if _looks_like_id_context(s, start, end):
            continue
        if not any(token == d["raw"] for d in parsed):
            parsed.append({"raw": token, "iso": try_parse_numeric_date(token)})

    return parsed


# -----------------------
# Try parse month-year into YYYY-MM-01
# -----------------------


def try_parse_month_year_token(raw_token: str) -> Optional[str]:
    m = RE_MONTH_YEAR.search(raw_token)
    if m:
        month_name, year = m.groups()
        dt = None
        try:
            dt = datetime.strptime(f"{month_name} {year}", "%B %Y")
        except Exception:
            try:
                dt = datetime.strptime(f"{month_name} {year}", "%b %Y")
            except Exception:
                dt = None
        if dt:
            return datetime(dt.year, dt.month, 1).date().isoformat()

    m2 = re.match(r"^(20\d{2})-(\d{1,2})$", raw_token)
    if m2:
        y = int(m2.group(1))
        mon = int(m2.group(2))
        try:
            return datetime(y, mon, 1).date().isoformat()
        except Exception:
            pass

    return None


# -----------------------
# Primary date chooser (unchanged behavior)
# -----------------------


def choose_primary_date_and_year(
    dates_list: List[Dict[str, Optional[str]]], cleaned_path: str
) -> Dict[str, Optional[str]]:
    out = {
        "primary_date": None,
        "primary_is_year_only": False,
        "year_only": None,
        "month_year_used": False,
    }

    dl = dates_list or extract_all_dates(cleaned_path)

    for d in dl:
        if d.get("iso") and _iso_year_plausible_current(d.get("iso")):
            out["primary_date"] = d["iso"]
            out["year_only"] = out["primary_date"][:4]
            return out

    for d in dl:
        my = try_parse_month_year_token(d["raw"])
        if my and _iso_year_plausible_current(my):
            out["primary_date"] = my
            out["primary_is_year_only"] = False
            out["month_year_used"] = True
            out["year_only"] = my[:4]
            return out

    parts = [p for p in cleaned_path.split("/") if p]
    for i, part in enumerate(parts):
        m = RE_YEAR_MONTH.search(part)
        if m:
            year, mon = m.groups()
            try:
                primary = datetime(int(year), int(mon), 1).date().isoformat()
                if _iso_year_plausible_current(str(primary)):
                    out["primary_date"] = primary
                    out["month_year_used"] = True
                    out["year_only"] = year
                    return out
            except Exception:
                pass

        my2 = try_parse_month_year_token(part)
        if my2 and _iso_year_plausible_current(my2):
            out["primary_date"] = my2
            out["month_year_used"] = True
            out["year_only"] = my2[:4]
            return out

    for token in re.findall(r"\d{6,8}", cleaned_path):
        iso = try_parse_numeric_date(token)
        if iso and _iso_year_plausible_current(iso):
            out["primary_date"] = iso
            out["year_only"] = iso[:4]
            return out

    m_year = RE_YEAR_ONLY.search(cleaned_path)
    if m_year:
        y = m_year.group(1)
        if _iso_year_plausible_current(f"{y}-01-01"):
            out["primary_date"] = f"{y}-01-01"
            out["primary_is_year_only"] = True
            out["year_only"] = y
            return out

    m2 = re.search(r"([12]\d{3})", cleaned_path)
    if m2:
        y = m2.group(1)
        if _iso_year_plausible_current(y):
            out["year_only"] = y

    return out


# ---------------------
# NEW: Filename date extraction
# ---------------------


def extract_filename_dates(filename: str) -> Dict[str, Optional[object]]:
    """
    Extract dates that appear in the filename (stem, i.e. without extension).
    Returns dict:
      {
        "filename_date_matches": [ { "raw": ..., "iso": ... }, ... ],
        "filename_primary_date_raw": raw_token_or_None,
        "filename_primary_date_iso": iso_or_None
      }

    Preference: choose the match that appears *closest to the end* of the filename stem.
    """
    stem = PurePosixPath(filename).stem  # filename without extension
    stem_clean = clean_path_str(stem)
    candidates = extract_all_dates(stem_clean)  # looks across the stem for tokens
    if not candidates:
        return {
            "filename_date_matches": None,
            "filename_primary_date_raw": None,
            "filename_primary_date_iso": None,
        }

    # choose the candidate whose raw token occurs last in the stem
    last_pos = -1
    chosen = None
    for c in candidates:
        raw = c["raw"]
        pos = stem_clean.lower().rfind(raw.lower())
        if pos > last_pos:
            last_pos = pos
            chosen = c

    return {
        "filename_date_matches": candidates,
        "filename_primary_date_raw": chosen["raw"] if chosen else None,
        "filename_primary_date_iso": chosen["iso"] if chosen else None,
    }


# ---------------------
# NEW: Safe filename generator
# ---------------------


def make_safe_filename(filename: str, max_len: Optional[int] = None) -> str:
    """
    Produce a safe filename by replacing every non-alphanumeric character with '_'.
    Keeps extension intact. Collapses repeated underscores and strips leading/trailing '_'.
    Optionally truncates the stem to max_len characters (applied before adding extension).
    """
    p = PurePosixPath(filename)
    stem = p.stem
    ext = p.suffix  # keep as-is including leading dot

    # replace any char that is not A-Za-z0-9 with underscore
    safe_stem = re.sub(r"[^A-Za-z0-9]", "_", stem)

    # collapse consecutive underscores and strip leading/trailing underscores
    safe_stem = re.sub(r"_+", "_", safe_stem).strip("_")

    if max_len and len(safe_stem) > max_len:
        safe_stem = safe_stem[:max_len]

    # if stem became empty (e.g., filename was only symbols), fallback to "file"
    if not safe_stem:
        safe_stem = "file"

    return f"{safe_stem}{ext}"


# ---------------------
# NEW: Meeting type / sub-meeting extraction from sub_context or entire path
# ---------------------
SUBMEET_RE = re.compile(
    r"\b(meet|PTM|CTM|MedChem|ChemMeet|RRC|PPI|SrStaff|Weekly|Brainstorm|V-RRC|CTM_DMPK|PTM AC|Chemistry meetings)\b",
    re.IGNORECASE,
)


def extract_meeting_types_from_path(s_clean: str) -> Dict[str, Optional[str]]:
    """
    Improved meeting-type extraction (see original docstring for details).
    """
    parts = [p for p in s_clean.split("/") if p]

    meeting_keyword_re = re.compile(
        r"\b(meet|meeting|subteam|ptm|ctm|medchem|chemmeet|rrc|ppi|srstaff|weekly|brainstorm|v-rrc|ptm ac|ctm_dmpk)\b",
        re.IGNORECASE,
    )

    def is_date_like(part: str) -> bool:
        if not part:
            return False
        part = part.strip()
        return bool(
            RE_YEAR_ONLY.search(part)
            or RE_DATE_COMPACT.search(part)
            or RE_DATE_US_SEP.search(part)
            or RE_DATE_ISO_SEP.search(part)
            or RE_MONTH_YEAR.search(part)
            or RE_YEAR_MONTH.search(part)
        )

    matches = []  # (index, part_text, matched_tokens_list_or_empty)
    for i, part in enumerate(parts):
        # strong token matches (abbrev like PTM/CTM) first
        sub_tokens = SUBMEET_RE.findall(part)
        if sub_tokens:
            matches.append((i, part, sub_tokens))
            continue

        # fallback: words containing 'meet' or other meeting keywords
        if meeting_keyword_re.search(part) or "meet" in part.lower():
            sub_tokens2 = SUBMEET_RE.findall(part)
            matches.append((i, part, sub_tokens2))
            continue

    # nothing matched
    if not matches:
        return {
            "meeting_type": None,
            "meeting_subtype": None,
            "meeting_matches_all": None,
        }

    # choose best match: prefer left-most match that contains a SUBMEET token, else left-most match
    chosen = None
    for m in matches:
        if m[2]:  # has SUBMEET_RE tokens
            chosen = m
            break
    if not chosen:
        chosen = matches[0]

    idx, part_text, toks = chosen

    def clean_meeting_text(text: str) -> str:
        t = text.strip()
        # remove trailing/leading punctuation
        t = re.sub(r"^[\W_]+|[\W_]+$", "", t)
        # remove compact numeric tokens (dates) embedded in the folder name
        t = re.sub(r"\b\d{6,8}\b", "", t)
        t = re.sub(r"\b[12]\d{3}\b", "", t)  # remove standalone years
        # collapse multiple spaces and hyphens to single space
        t = re.sub(r"[_\-\s]+", " ", t).strip()
        return t if t else None

    # Determine main meeting_type
    if toks:
        # If SUBMEET tokens available prefer that (normalize to first token)
        meeting_type = toks[0]
    else:
        meeting_type = clean_meeting_text(part_text) or "Meeting"

    # Now find a meeting_subtype:
    meeting_subtype = None

    # 1) Prefer the first date-like token after the meeting part (look up to next 4 parts)
    for j in range(idx + 1, min(len(parts), idx + 5)):
        nxt = parts[j]
        if is_date_like(nxt):
            meeting_subtype = nxt
            break

    # 2) If no date-like found, prefer the next non-trivial text part
    if meeting_subtype is None:
        for j in range(idx + 1, min(len(parts), idx + 5)):
            nxt = parts[j]
            if not nxt.strip():
                continue
            if re.match(
                r"^(thumbs|\.ds_store|downloads|archive)$", nxt.strip(), re.IGNORECASE
            ):
                continue
            if re.fullmatch(r"\d+", nxt.strip()):
                if RE_YEAR_ONLY.fullmatch(nxt.strip()):
                    meeting_subtype = nxt
                    break
                else:
                    continue
            meeting_subtype = clean_meeting_text(nxt) or nxt
            break

    # Collect all raw matches encountered (SUBMEET token list or descriptive parts)
    meeting_matches_all = []
    for _, ptxt, tks in matches:
        if tks:
            meeting_matches_all.extend(tks)
        else:
            meeting_matches_all.append(clean_meeting_text(ptxt) or ptxt)

    if not meeting_matches_all:
        meeting_matches_all = None

    return {
        "meeting_type": meeting_type,
        "meeting_subtype": meeting_subtype,
        "meeting_matches_all": meeting_matches_all,
    }


# ---------------------
# CATEGORY / TEMP DETECTION
# ---------------------


def file_category_from_ext(ext, is_temp):
    ext = ext.lower()
    if is_temp:
        return "Temp/Cache/Noise"
    for cat, exts in CATEGORY_MAP.items():
        if ext in exts:
            return cat
    return "Other"


# ---------------------
# MAIN PARSER FOR ONE PATH
# ---------------------


def extract_metadata_from_path(full_path, file_uuid):
    s_clean = clean_path_str(full_path)
    p = PurePosixPath(s_clean)
    filename = p.name
    extension = p.suffix.lower()
    is_temp = (
        filename.startswith("~$")
        or any(pref in s_clean for pref in TEMP_PREFIXES)
        or extension in TEMP_EXTS
    )

    # extract all dates and choose primary
    date_matches = extract_all_dates(s_clean)  # list of {'raw', 'iso'}
    chosen = choose_primary_date_and_year(date_matches, s_clean)
    parsed_dates = (
        [d["iso"] or d["raw"] for d in date_matches] if date_matches else None
    )

    # filename-specific dates
    fname_date_info = extract_filename_dates(filename)
    filename_date_matches = fname_date_info["filename_date_matches"]
    filename_primary_date_raw = fname_date_info["filename_primary_date_raw"]
    filename_primary_date_iso = fname_date_info["filename_primary_date_iso"]

    # IDs, species, assays, meetings
    exp_ids = RE_EXP.findall(full_path) or None
    if exp_ids:
        exp_ids = list(dict.fromkeys(exp_ids))
    compound_ids = RE_COMPOUND.findall(full_path) or None
    if compound_ids:
        compound_ids = list(dict.fromkeys(compound_ids))

    species = SPECIES_KW.findall(full_path)
    species = list(dict.fromkeys(species))[0] if species else None

    assays = ASSAY_KW.findall(full_path)
    assays = list(dict.fromkeys(assays)) if assays else None
    assay = assays[0] if assays else None

    # project / functional area / sub-context from path parts (root expected e.g. Apollo/Project/Area/...)
    parts = list(p.parts)
    project = parts[1] if len(parts) > 1 else None
    functional_area = parts[2] if len(parts) > 2 else None
    sub_context = (
        "/".join(parts[3:-1])
        if len(parts) > 4
        else (parts[3] if len(parts) > 3 else None)
    )

    # meeting extraction from entire cleaned path (prefers sub_context parts)
    meeting_info = extract_meeting_types_from_path(s_clean)
    meeting = meeting_info["meeting_type"]
    meeting_subtype = meeting_info["meeting_subtype"]
    meeting_matches_all = meeting_info["meeting_matches_all"]

    # author heuristics
    author = None
    for tok in re.findall(r"\b([A-Z]{2,4})\b", filename):
        if tok.lower() not in ("pdf", "ppt", "doc", "xls", "xlsx", "jpg", "png", "pse"):
            author = tok
            break

    if not author:
        for part in parts[-4:-1]:
            for tok in re.findall(r"\b([A-Z]{2,4})\b", part):
                if tok.lower() not in (
                    "pdf",
                    "ppt",
                    "doc",
                    "xls",
                    "xlsx",
                    "jpg",
                    "png",
                    "pse",
                ):
                    author = tok
                    break
            if author:
                break

    # safe filename
    safe_filename = make_safe_filename(filename)
    
    # file category
    category = file_category_from_ext(extension, is_temp)

    return {
        "article_id": file_uuid,
        "original_filename": filename,
        "safe_fileName": safe_filename,
        "title": full_path,
        "extension": extension,
        "full_path": full_path,
        "team": project,
        "sub_team": functional_area,
        "sub_context": sub_context.lower() if isinstance(sub_context, str) else None,
        "experiment_id_all": exp_ids,
        "compound_id_all": compound_ids,
        "species_extracted": species,
        "assay_protocol": assay,
        "assay_protocol_all": assays,
        "meeting_report_type": meeting,
        "meeting_report_subtype": meeting_subtype,
        "meeting_report_type_all": meeting_matches_all,
        "author_owner": author,
        "created_date": chosen["primary_date"],
        "primary_date_is_year_only": chosen["primary_is_year_only"],
        "primary_date_from_month_year": chosen["month_year_used"],
        "year": chosen["year_only"],
        "file_category": category,
        "source": "Apollo",
        "article_type": functional_area,
        "is_temp_file": is_temp,
    }


def apollo_articles_metadata_extractor(
    apollo_source_config: dict,
    extracted_files_to_uuid_map: dict,
    source: str = "apollo",
):
    storage_type = apollo_source_config["type"]  # will be s3
    src_data_path = apollo_source_config["s3_src_path"]

    # Get file handler instance from factory
    s3_file_handler = FileHandlerFactory.get_handler(storage_type)

    # src_files = s3_file_handler.s3_util.list_files(src_data_path)  # to get full path
    #
    # # Filter out unwanted files
    # filtered_files = []
    # for file_path in src_files:
    #     s_clean = clean_path_str(file_path)
    #     p = PurePosixPath(s_clean)
    #     filename = p.name
    #     extension = p.suffix.lower()
    #     is_temp = (
    #         filename.startswith("~$")
    #         or any(pref in s_clean for pref in TEMP_PREFIXES)
    #         or extension in TEMP_EXTS
    #     )
    #     if is_temp:
    #         continue
    #     filtered_files.append(file_path)
    #
    # Getting S3 path for writing metadata directly to S3
    # Initialize the config loader
    config_loader = YAMLConfigLoader()
    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    # Retrieve paths from config
    s3_paths = paths_config["storage"][storage_type]
    s3_metadata_path = s3_paths["metadata_path"].replace("{source}", source)

    for file_path, file_uuid in extracted_files_to_uuid_map.items():
        logger.info(f"Extracting metadata from {file_path}")
        metadata_json = extract_metadata_from_path(file_path, file_uuid)
        file_name = f"{file_uuid}_metadata.json"
        s3_full_metadata_path = s3_file_handler.get_file_path(
            s3_metadata_path, file_name
        )
        s3_file_handler.write_file_as_json(s3_full_metadata_path, metadata_json)
