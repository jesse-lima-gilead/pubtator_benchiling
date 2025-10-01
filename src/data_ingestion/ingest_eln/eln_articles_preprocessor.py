import json
import re
import os
import uuid
import zipfile
from pathlib import Path
from rdkit import Chem
from typing import List, Dict, Optional, Union, Any
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger


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


def generate_safe_filename(eln_path: str):
    safe_file_name_cnt = 0
    logger.info(f"Generating Safe FileNames...")
    for internal_doc in file_handler.list_files(eln_path):
        # Replace all the special chars in the file name with '_'
        safe_doc_name = "".join(
            c if c.isalnum() or c in (".", "_") else "_" for c in internal_doc
        )
        safe_doc_name = safe_doc_name.replace("ascii", "")
        if internal_doc != safe_doc_name:
            logger.info(f"Renaming file {internal_doc} to {safe_doc_name}")
            file_handler.move_file(
                Path(eln_path) / internal_doc, Path(eln_path) / safe_doc_name
            )
            safe_file_name_cnt += 1
    return safe_file_name_cnt


def json_cleaner(input_eln_file: str, output_eln_file: str):
    """Clean and format a JSON file with encoding issues"""

    # Read the file in binary mode to handle encoding issues
    logger.info(f"Cleaning JSON file {input_eln_file}...")

    with open(input_eln_file, "rb") as f:
        data = f.read()

    # Replace problematic bytes
    # 0xa0 is non-breaking space, replace with regular space
    # 0xba is likely degree symbol (°), replace with proper UTF-8 encoding
    cleaned_data = data.replace(b"\xa0", b" ").replace(b"\xba", "°".encode("utf-8"))
    try:
        # Parse JSON
        json_obj = json.loads(cleaned_data.decode("utf-8"))

        # Check for key fields
        key_fields = [
            "ID",
            "EXPERIMENTNUMBER",
            "TITLE",
            "AUTHOR",
            "CREATED",
            "ASCIICONTENT",
        ]
        present_fields = [field for field in key_fields if field in json_obj]
        logger.info(f"- Key fields present: {', '.join(present_fields)}")

        # Check content size
        if "ASCIICONTENT" in json_obj:
            content_length = len(json_obj["ASCIICONTENT"])
            logger.info(f"- ASCII content length: {content_length:,} characters")

        # Write formatted JSON
        with open(output_eln_file, "w", encoding="utf-8") as f:
            json.dump(json_obj, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Output written to: {output_eln_file}")

        return True

    except json.JSONDecodeError as e:
        logger.warn(f"✗ JSON parsing error: {e}")
        logger.warn(f"✗ Line {e.lineno}, Column {e.colno}")
        return False
    except UnicodeDecodeError as e:
        logger.warn(f"✗ Unicode decoding error: {e}")
        return False


def parse_sdf_string_to_files(
    sdf_text: str,
    out_dir: Union[str, Path],
    name_scheme: str = "auto",
    make_zip: bool = True,
    zip_name: str = "extracted_molecules.zip",
) -> Dict[str, Union[str, List[str]]]:
    """
    Parse an SDF-like string containing one or more MDL mol blocks (V3000 possible)
    and write each molecule into its own .sdf and .mol file. Non-molecule trailing
    chunks are written as .txt metadata files.

    Parameters
    ----------
    sdf_text:
        The raw string containing one or more SDF/mol records. Records are expected
        to be separated by "$$$$".
    out_dir:
        Directory where extracted files will be written. Created if missing.
    name_scheme:
        How to name files:
          - 'auto'  : use first human-readable line from the record (sanitized) + index
          - 'index' : use index-only names: molecule_01.sdf, molecule_02.sdf, metadata_03.txt
          - 'title' : use the title line (if available) without index (falls back to index)
    make_zip:
        If True, produce a zip archive with all created files inside out_dir.
    zip_name:
        Name of zip file created inside the parent of out_dir.

    Returns
    -------
    dict with keys:
      - "out_dir": str path of output directory
      - "files": list of created file paths (strings)
      - "zip": path to zip file (string) if created, else None

    Notes
    -----
    - The function is conservative: considers a chunk a molecule if it contains
      'BEGIN CTAB' or 'M  V30' or 'M  END' (case-insensitive).
    - .sdf files are single-record SDFs that end with "$$$$".
    - .mol is extracted as everything up to and including the first 'M  END' if present,
      otherwise the entire chunk is saved as fallback.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # strip leading/trailing single quotes if the whole string is quoted in the input
    if sdf_text.startswith("'") and sdf_text.endswith("'"):
        sdf_text = sdf_text[1:-1]

    # split records on SDF separator $$$$ (allow trailing whitespace/newline)
    raw_chunks = re.split(r"\$\$\$\$\s*", sdf_text)

    def sanitize_filename(s: str, default: str) -> str:
        s = s.strip()
        if not s:
            return default
        # Keep first meaningful line (skip SciTegic header & lines beginning with 'M  V' or coordinates)
        lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
        title = None
        for ln in lines[:4]:
            if re.search(
                r"^(SciTegic|M\s+V|M\s+V30|BEGIN CTAB|BEGIN ATOM|COUNTS)",
                ln,
                re.IGNORECASE,
            ):
                continue
            # Accept short meaningful lines (no long coordinate-like patterns)
            if len(ln) <= 80 and not re.match(r"^[0-9\-\.\s]+$", ln):
                title = ln
                break
        if title is None and lines:
            title = lines[0]
        if not title:
            title = default
        # make filesystem-safe
        sanitized = re.sub(r"[^0-9A-Za-z\-\._]+", "_", title).strip("_")
        if not sanitized:
            sanitized = default
        return sanitized[:120]

    created_files: List[str] = []
    record_index = 0

    for chunk in raw_chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        # Determine whether this chunk looks like a molecule (conservative check)
        is_molecule = bool(
            re.search(r"BEGIN CTAB|M\s+V30|M\s+END", chunk, re.IGNORECASE)
        )
        record_index += 1

        if name_scheme == "index":
            base = f"{record_index:02d}_molecule"
        else:
            pretty = sanitize_filename(chunk, f"molecule_{record_index:02d}")
            if name_scheme == "title":
                base = pretty or f"{record_index:02d}_molecule"
            else:  # auto
                base = f"{record_index:02d}_{pretty}"

        if is_molecule:
            # Write single-record SDF
            sdf_path = out_dir / f"{base}.sdf"
            # ensure it ends with newline + $$$$ (consistent SDF single record)
            sdf_content = chunk.rstrip() + "\n$$$$\n"
            with open(sdf_path, "w", encoding="utf-8") as fh:
                fh.write(sdf_content)
            created_files.append(str(sdf_path))

            # Extract molfile portion: up to first 'M  END' (include M  END)
            m = re.search(r"^(.*?\bM\s+END\b)", chunk, re.IGNORECASE | re.DOTALL)
            if m:
                mol_text = m.group(1).rstrip() + "\n"
            else:
                # fallback: use entire chunk (so user still gets something to inspect)
                mol_text = chunk + "\n"
            mol_path = out_dir / f"{base}.mol"
            with open(mol_path, "w", encoding="utf-8") as fh:
                fh.write(mol_text)
            created_files.append(str(mol_path))
        else:
            # Non-molecule chunk (save as metadata / notes)
            meta_name = sanitize_filename(chunk, f"metadata_{record_index:02d}")
            meta_path = out_dir / f"{meta_name}.txt"
            with open(meta_path, "w", encoding="utf-8") as fh:
                fh.write(chunk + "\n")
            created_files.append(str(meta_path))

    zip_path: Optional[str] = None
    if make_zip and created_files:
        zip_path_obj = out_dir.parent / zip_name
        with zipfile.ZipFile(zip_path_obj, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in created_files:
                zf.write(fp, arcname=os.path.basename(fp))
        zip_path = str(zip_path_obj)

    return {"out_dir": str(out_dir), "files": created_files, "zip": zip_path}


def remove_sdfs_from_string(
    sdf_text: str,
    *,
    preserve_delimiters: bool = False,
    strip_outer_quotes: bool = True,
    collapse_blank_lines: bool = True,
) -> str:
    """
    Remove SDF / mol blocks from a string and return only the non-molecule text.

    Parameters
    ----------
    sdf_text:
        Input string that may contain one or more SDF records separated by "$$$$".
    preserve_delimiters:
        If True, keep the "$$$$" delimiters for non-molecule chunks as they appeared.
        If False (default) the returned text will not include "$$$$" delimiters.
    strip_outer_quotes:
        If True strip a single pair of outer single-quotes if the whole string is quoted.
    collapse_blank_lines:
        If True collapse runs of 3+ blank lines down to 2 and trim leading/trailing whitespace.

    Returns
    -------
    str: text containing everything from the input except the detected SDF/molecule blocks.
    """
    if strip_outer_quotes and sdf_text.startswith("'") and sdf_text.endswith("'"):
        sdf_text = sdf_text[1:-1]

    # split while preserving delimiters: parts will be [chunk, delimiter, chunk, delimiter, ...]
    parts = re.split(r"(\$\$\$\$\s*)", sdf_text, flags=re.IGNORECASE)

    # conservative molecule detector (same idea you used before)
    mol_re = re.compile(r"BEGIN\s+CTAB|M\s+V30|M\s+END\b", re.IGNORECASE)

    kept_parts = []
    i = 0
    n = len(parts)
    while i < n:
        chunk = parts[i]
        delim = parts[i + 1] if (i + 1) < n else ""

        # If the chunk looks like a molecule, skip chunk (+ optionally skip delim)
        is_molecule = bool(mol_re.search(chunk))

        if is_molecule:
            # skip molecule chunk and its following delimiter (if any)
            i += 2
            continue
        else:
            # keep non-molecule chunk; append delim only if preserve_delimiters True
            if preserve_delimiters and delim:
                kept_parts.append(chunk + delim)
            else:
                kept_parts.append(chunk)
            i += 2

    remaining = "".join(kept_parts)

    if collapse_blank_lines:
        # normalize line endings
        remaining = remaining.replace("\r\n", "\n").replace("\r", "\n")
        # remove very long runs of empty lines
        remaining = re.sub(r"\n{3,}", "\n\n", remaining)
        remaining = remaining.strip()

    return remaining


def extract_smiles_from_sdf(
    sdf_file_path: Union[str, Path],
    use_forward_supplier: bool = False,
    sanitize: bool = True,
) -> Dict[str, Any]:
    """
    Extract SMILES (and fragments) for every molecule in an SDF file.

    Args:
        sdf_file_path: path to the .sdf file
        use_forward_supplier: if True, use ForwardSDMolSupplier (streaming, low memory)
        sanitize: whether to run RDKit sanitization on each molecule

    Returns:
        dict with keys:
          - file_name: str
          - molecules: list of dicts: {"id": int, "smiles": str, "fragments": List[str], "properties": dict}
    """
    p = Path(sdf_file_path)
    logger.info("Processing sdf file: %s", p)
    result: Dict[str, Any] = {"file_name": p.name, "molecules": []}

    if p.suffix.lower() != ".sdf":
        logger.warning("Provided file does not have .sdf extension: %s", p)
        return result

    try:
        supplier = (
            Chem.ForwardSDMolSupplier(str(p))
            if use_forward_supplier
            else Chem.SDMolSupplier(str(p))
        )

        process_mol_cnt = 0
        unprocessed_mol_cnt = 0

        for idx, mol in enumerate(supplier, start=1):
            if mol is None:
                logger.warning(
                    "Skipping malformed/empty molecule at index %d in %s", idx, p.name
                )
                continue

            try:
                if sanitize:
                    Chem.SanitizeMol(mol)

                smile = Chem.MolToSmiles(mol, canonical=True)  # isomericSmiles=True,
                fragments = smile.split(".")
                try:
                    props = mol.GetPropsAsDict()
                except Exception:
                    # older/newer RDKit builds might behave slightly differently; fallback safe approach:
                    props = {k: mol.GetProp(k) for k in mol.GetPropNames()}

                mol_record = {
                    "mol_grsar_id": str(uuid.uuid4()),
                    "mol_index": idx,
                    "smile": smile,
                    "fragments": fragments,
                    "properties": props,
                    "processed": True,
                }
                result["molecules"].append(mol_record)
                process_mol_cnt += 1

            except Exception:
                # Don't let one bad molecule stop the whole file; log stack trace and continue
                logger.exception(
                    "Failed to process molecule at index %d in %s", idx, p.name
                )
                mol_record = {
                    "mol_grsar_id": str(uuid.uuid4()),
                    "mol_index": idx,
                    "smile": "",
                    "fragments": [],
                    "properties": {},
                    "processed": False,
                }
                result["molecules"].append(mol_record)
                unprocessed_mol_cnt += 1

        # Add stats of molecules processed
        result["total_molecules"] = len(result["molecules"])
        result["processed_molecules"] = process_mol_cnt
        result["unprocessed_molecules"] = unprocessed_mol_cnt

        logger.info(f"Processed {process_mol_cnt} molecules in {p.name}")

        # close supplier if supported
        if hasattr(supplier, "close"):
            try:
                supplier.close()
            except Exception:
                logger.debug("Supplier close() failed (ignored).", exc_info=True)

        # Write the result to a JSON file alongside the SDF
        json_output_path = p.with_suffix(".smiles.json")
        with open(json_output_path, "w", encoding="utf-8") as jf:
            json.dump(result, jf, indent=2, ensure_ascii=False)
        logger.info(f"SMILES extraction result written to {json_output_path}")

    except Exception:
        logger.exception("Failed to read SDF file: %s", p)

    return result


def preprocess_eln_files(
    eln_path: str,
    eln_interim_path: str,
    eln_metadata_path: str,
    eln_chunks_path: str,
    file_handler: FileHandler,
):
    for eln in file_handler.list_files(eln_path):
        logger.info(f"Processing ELN file: {eln}")
        input_eln_path = Path(eln_path) / eln
        if eln.endswith(".json"):
            with open(input_eln_path, "r", encoding="utf-8") as f:
                eln_json = json.load(f)

                if eln_json is None:
                    logger.warning(f"Skipping empty or invalid JSON file: {eln}")
                    continue

                logger.info(f"ELN JSON file found: {eln}. Extracting content...")
                eln_data = dict(eln_json)
                eln_data = {k.lower(): v for k, v in eln_data.items()}

                # Extracting the Chemical Structures from the ELN JSON
                ascii_content = str(eln_data.pop("ASCIICONTENT".lower()))
                sdf_file_path = eln_interim_path + "/" + eln.split(".")[0] + ".sdf"
                file_handler.write_file(sdf_file_path, ascii_content)
                logger.info(f"SDF file written: {sdf_file_path}")

                # Convert molecules in SDF file to SMILES and fragments
                smiles = extract_smiles_from_sdf(
                    sdf_file_path=sdf_file_path,
                    use_forward_supplier=False,
                    sanitize=True,
                )

                # chemical_structures = parse_sdf_string_to_files(
                #     sdf_text=ascii_content,
                #     out_dir=Path(eln_interim_path) / eln.replace(".json", ""),
                #     name_scheme="auto",
                #     make_zip=False,
                # )
                # logger.info(
                #     f"Extracted {len(chemical_structures)} chemical structures as .sdf and .mol files"
                # )

                # # Extract the Procedure text and other from the SDF file
                content_without_mol = remove_sdfs_from_string(ascii_content)

                # Save the modified ELN JSON without ASCIICONTENT as Metadata File
                eln_data["article_type"] = "ELN"
                eln_data["eln_content"] = content_without_mol
                logger.info("Extracting ELN Metadata...")
                metadata_filename = eln.replace(".json", "_metadata.json")
                metadata_file_path = Path(eln_metadata_path) / metadata_filename
                file_handler.write_file_as_json(metadata_file_path, eln_data)
                logger.info(f"Saved ELN metadata to {metadata_file_path}")

                # Save the SMILES and fragments as ELN Chunk
                chunks = []
                if len(smiles["molecules"]) > 0:
                    for mol in smiles["molecules"]:
                        chunk = {
                            "chunk_sequence": mol["mol_index"],
                            "smile": mol["smile"],
                            "payload": {
                                "mol_grsar_id": mol["mol_grsar_id"],
                                "smile": mol["smile"],
                                "fragments": mol["fragments"],
                                "properties": mol["properties"],
                                "processed": mol["processed"],
                                **eln_data,
                            },
                        }
                        chunks.append(chunk)
                    logger.info(
                        f"Added molecules to the chunks list along with metadata."
                    )
                else:
                    chunk = {
                        "chunk_sequence": 1,
                        "smile": None,
                        "payload": {**eln_data}
                    }
                    chunks.append(chunk)
                    logger.warn(
                        f"No molecules found in the SDF content of ELN file: {eln}. Saving metadata only."
                    )
                chunk_file_path = Path(eln_chunks_path) / eln
                file_handler.write_file_as_json(chunk_file_path, chunks)
                logger.info(f"Saved ELN chunk with SMILES to {chunk_file_path}")
