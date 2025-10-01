import json
import re
import shutil
import tempfile
import os
import uuid
from pathlib import Path
from typing import Tuple, Union, Optional, Any, Dict

from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.logs_handler.logger import SingletonLogger

logger_instance = SingletonLogger()
logger = logger_instance.get_logger()

_C0_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")  # keep \t\n\r
_REPLACEMENT_RE = re.compile("\uFFFD+")
_LONG_WHITESPACE_RE = re.compile(r"\s{80,}")


def _clean_control_chars(text: str) -> str:
    return _C0_CONTROL_RE.sub("", text)


def decode_cp1252_and_to_utf8(
    data: Union[bytes, str],
    source_encoding: str = "cp1252",
    errors: str = "strict",
) -> Tuple[str, bytes]:
    if isinstance(data, str):
        text = data
    else:
        text = data.decode(source_encoding, errors=errors)
    return text, text.encode("utf-8", errors=errors)


def sanitize_json_strings(
    obj: Any,
    remove_replacement: bool = True,
    collapse_long_whitespace: bool = True,
    max_str_len: int = 200_000,
) -> Any:
    if isinstance(obj, str):
        s = obj
        if remove_replacement:
            s = _REPLACEMENT_RE.sub("\uFFFD", s)
        if collapse_long_whitespace:
            s = _LONG_WHITESPACE_RE.sub(" ", s)
        if len(s) > max_str_len:
            s = s[:max_str_len] + "...[truncated]"
        return s

    if isinstance(obj, dict):
        return {
            k: sanitize_json_strings(
                v, remove_replacement, collapse_long_whitespace, max_str_len
            )
            for k, v in obj.items()
        }

    if isinstance(obj, list):
        return [
            sanitize_json_strings(
                v, remove_replacement, collapse_long_whitespace, max_str_len
            )
            for v in obj
        ]

    return obj


def _unique_failed_path(failed_dir: Path, original_name: str) -> Path:
    """
    Return a Path in failed_dir that does not clobber existing files.
    Appends a uuid if necessary.
    """
    failed_path = failed_dir / original_name
    if not failed_path.exists():
        return failed_path
    # append short uuid
    stem = Path(original_name).stem
    suffix = Path(original_name).suffix
    unique_name = f"{stem}_{uuid.uuid4().hex[:8]}{suffix}"
    return failed_dir / unique_name


def _stream_json_to_tempfile_and_check_size(
    obj: Any,
    target_path: Union[str, Path],
    max_size_bytes: Optional[int],
    original_size: int,
    max_ratio: float = 4.0,
) -> bool:
    target_path = Path(target_path)
    tmp_dir = target_path.parent
    encoder = json.JSONEncoder(ensure_ascii=False, indent=2)
    fd, tmpname = tempfile.mkstemp(
        prefix=target_path.name + ".", suffix=".tmp", dir=str(tmp_dir)
    )
    os.close(fd)

    try:
        total_written = 0
        with open(tmpname, "wb") as fh:
            for chunk in encoder.iterencode(obj):
                cb = chunk.encode("utf-8")
                fh.write(cb)
                total_written += len(cb)
                # early exit if absolute cap reached
                if max_size_bytes is not None and total_written > max_size_bytes:
                    logger.warning(
                        f"Streaming write exceeded max_size_bytes ({total_written} > {max_size_bytes}). Aborting."
                    )
                    break

        # produced_size might exist even if we broke early
        produced_size = os.path.getsize(tmpname)
        ratio = produced_size / max(original_size, 1)
        logger.debug(
            f"Produced JSON size: {produced_size} bytes (orig: {original_size}, ratio: {ratio:.2f})"
        )

        if (
            max_size_bytes is not None and produced_size > max_size_bytes
        ) or ratio > max_ratio:
            logger.warning(
                f"Serialized JSON is too large: {produced_size} bytes (ratio {ratio:.2f}). Removing temp file."
            )
            try:
                os.remove(tmpname)
            except Exception:
                logger.exception("Failed to remove temporary oversized file")
            return False

        # move temp into place atomically (may overwrite original)
        try:
            shutil.move(tmpname, str(target_path))
            return True
        except Exception as e:
            logger.exception(f"Failed to move temporary JSON file into place: {e}")
            # attempt clean-up
            try:
                if os.path.exists(tmpname):
                    os.remove(tmpname)
            except Exception:
                logger.exception("Failed to remove temporary file after move failure")
            return False

    except Exception as e:
        logger.exception(f"Error while streaming JSON to temp file: {e}")
        try:
            if os.path.exists(tmpname):
                os.remove(tmpname)
        except Exception:
            logger.exception("Failed to remove temporary file after exception")
        return False


def json_formatter(
    filename: str,
    file_bytes: Union[bytes, str],
    encodings: Tuple[str, ...] = ("cp1252", "utf-8"),
    clean_control_chars: bool = True,
    errors_final_fallback: str = "replace",
) -> Any:
    last_exc: Optional[Exception] = None

    if isinstance(file_bytes, str):
        text_try = (
            _clean_control_chars(file_bytes) if clean_control_chars else file_bytes
        )
        try:
            return json.loads(text_try)
        except json.JSONDecodeError as e:
            last_exc = e

    if isinstance(file_bytes, (bytes, bytearray)):
        for enc in encodings:
            try:
                text = file_bytes.decode(enc)
            except UnicodeDecodeError as ude:
                last_exc = ude
                continue

            if clean_control_chars:
                text = _clean_control_chars(text)

            try:
                parsed = json.loads(text)
                logger.info(f"Loaded {filename} using encoding '{enc}'")
                return parsed
            except json.JSONDecodeError as jde:
                last_exc = jde
                continue

        # fallback decode with replace (last encoding)
        try:
            text_fallback = file_bytes.decode(
                encodings[-1], errors=errors_final_fallback
            )
            if clean_control_chars:
                text_fallback = _clean_control_chars(text_fallback)
            parsed = json.loads(text_fallback)
            logger.info(
                f"Loaded {filename} using fallback decode '{encodings[-1]}' with errors='{errors_final_fallback}'"
            )
            return parsed
        except Exception as e:
            last_exc = e

    raise RuntimeError(
        f"Failed to parse JSON for file {filename}. Last error: {repr(last_exc)}"
    )


def eln_article_json_formatter(
    eln_path: str,
    failed_path: str,
    eln_interim_path: str,
    file_handler: FileHandler,
    *,
    max_size_bytes: int = 200 * 1024 * 1024,
    max_ratio: float = 4.0,
    sanitize_on_failure: bool = True,
) -> Tuple[int, int]:
    formatted_files_cnt = 0
    formatted_files_path = []
    not_formatted_files_path = []
    not_formatted_files_cnt = 0

    failed_dir = Path(failed_path)
    failed_dir.mkdir(parents=True, exist_ok=True)

    for eln in file_handler.list_files(eln_path):
        if eln.endswith(".json") and not eln.startswith("~$"):
            logger.info(f"Formatting ELN file: {eln} to UTF8 JSON")
            eln_filepath = Path(eln_path) / eln
            try:
                file_bytes = file_handler.read_file_bytes(file_path=eln_filepath)
                orig_size = len(file_bytes)
                file_data = json_formatter(eln, file_bytes)

                success = _stream_json_to_tempfile_and_check_size(
                    file_data,
                    target_path=eln_filepath,
                    max_size_bytes=max_size_bytes,
                    original_size=orig_size,
                    max_ratio=max_ratio,
                )

                if success:
                    formatted_files_cnt += 1
                    formatted_files_path.append(eln)
                    logger.info(
                        f"Formatted and saved ELN file as utf8 JSON to: {eln_filepath}"
                    )
                    continue

                # sanitization fallback
                if sanitize_on_failure:
                    logger.warning(
                        f"{eln}: serialization expanded too much; attempting sanitization fallback."
                    )
                    sanitized = sanitize_json_strings(file_data)
                    success2 = _stream_json_to_tempfile_and_check_size(
                        sanitized,
                        target_path=eln_filepath,
                        max_size_bytes=max_size_bytes,
                        original_size=orig_size,
                        max_ratio=max_ratio,
                    )
                    if success2:
                        formatted_files_cnt += 1
                        formatted_files_path.append(eln)
                        logger.info(
                            f"Formatted (sanitized) and saved ELN file as utf8 JSON to: {eln_filepath}"
                        )
                        continue

                # failure -> move original to failed_dir with a unique name
                not_formatted_files_cnt += 1
                not_formatted_files_path.append(eln)
                failed_filepath = _unique_failed_path(failed_dir, eln)
                try:
                    if eln_filepath.exists():
                        shutil.move(str(eln_filepath), str(failed_filepath))
                    else:
                        # original may have been replaced/removed; create an empty marker
                        Path(failed_filepath).write_text("", encoding="utf-8")
                    logger.warning(
                        f"File {eln} could not be formatted to JSON (oversized after formatting). Moved to failed dir: {failed_filepath}"
                    )
                except Exception:
                    logger.exception(f"Failed to move or mark failed file {eln}")

            except Exception as e:
                logger.exception(f"Error processing file {eln}: {e}")
                not_formatted_files_cnt += 1
                not_formatted_files_path.append(eln)
                # move the problematic file to failed_path safely
                try:
                    failed_filepath = _unique_failed_path(failed_dir, eln)
                    if eln_filepath.exists():
                        shutil.move(str(eln_filepath), str(failed_filepath))
                        logger.warning(
                            f"File {eln} moved to failed dir: {failed_filepath}"
                        )
                    else:
                        Path(failed_filepath).write_text("", encoding="utf-8")
                except Exception:
                    logger.exception(f"Failed to move {eln} to failed dir")

    # Write the not-formatted files to a log
    if not_formatted_files_cnt > 0:
        log_path = Path(eln_interim_path) / "not_formatted_files.log"
        with open(log_path, "w", encoding="utf-8") as log_file:
            for nf in not_formatted_files_path:
                log_file.write(f"{nf}\n")
        logger.info(f"Not formatted files logged to: {log_path}")

    return formatted_files_cnt, not_formatted_files_cnt
