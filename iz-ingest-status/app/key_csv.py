"""
key.csv parser and editor.

Handles reading, parsing, and editing key.csv files that control ingest behavior.
key.csv files are the ONLY files this tool writes to disk.

Inputs: File paths on the NFS mount.
Outputs: Parsed key.csv dicts, or updated key.csv files on disk.

Ported from IzImporter._read_file_key() and IzImporter.find_key_file().
"""

import csv
import io
import os
from typing import Optional

from app.config import KEY_CSV_COLUMN_MAPPINGS, TRUTHY_VALUES


def find_key_csv(directory: str) -> Optional[str]:
    """
    Walk up the directory tree from `directory` to find the nearest key.csv.

    Inputs: directory (str) — starting directory path.
    Outputs: Absolute path to the first key.csv found, or None.

    Ported from IzImporter.find_key_file(). Walks parent directories
    until reaching the filesystem root.
    """
    current = directory
    while current != os.path.dirname(current):
        candidate = os.path.join(current, "key.csv")
        if os.path.isfile(candidate):
            return candidate
        current = os.path.dirname(current)
    return None


def parse_key_csv(file_path: str) -> dict:
    """
    Parse a key.csv file into a normalized dictionary.

    Inputs: file_path (str) — path to the key.csv file.
    Outputs: Dict with normalized keys (from KEY_CSV_COLUMN_MAPPINGS) and
             a special '_path' key holding the file path. Values are strings
             or None. The '_raw_rows' key holds the original rows for editing.

    Encoding: Tries UTF-8 first, falls back to Latin-1.
    Format: CSV where column 0 is the key name and column 1 is the value.
    Column names are matched case-insensitively via KEY_CSV_COLUMN_MAPPINGS.

    Ported from IzImporter._read_file_key().
    """
    result = {mapped: None for mapped in KEY_CSV_COLUMN_MAPPINGS.values()}
    result["_path"] = file_path

    raw_rows = _read_csv_rows(file_path)
    result["_raw_rows"] = raw_rows

    for row in raw_rows:
        if not row or len(row) < 2:
            continue
        key = row[0].strip().lower()
        if key in KEY_CSV_COLUMN_MAPPINGS:
            value = row[1].strip() if row[1].strip() else None
            result[KEY_CSV_COLUMN_MAPPINGS[key]] = value

    return result


def _read_csv_rows(file_path: str) -> list[list[str]]:
    """
    Read all rows from a CSV file, handling encoding fallback.

    Inputs: file_path (str).
    Outputs: List of rows, each a list of strings.

    Tries UTF-8 first, falls back to Latin-1 on UnicodeDecodeError.
    """
    for encoding in ("utf-8", "latin-1"):
        try:
            with open(file_path, encoding=encoding, newline="") as f:
                reader = csv.reader(f)
                return list(reader)
        except UnicodeDecodeError:
            continue
    return []


def is_removed(key_data: dict) -> bool:
    """
    Check if a parsed key.csv dict has the remove flag set to a truthy value.

    Inputs: key_data (dict) — parsed key.csv from parse_key_csv().
    Outputs: True if remove field is truthy (TRUE/True/true/1/yes).
    """
    remove_val = key_data.get("remove")
    if remove_val is None:
        return False
    return str(remove_val).strip().lower() in TRUTHY_VALUES


def build_key_csv_cache(
    scan_root: str, directories: set[str]
) -> dict[str, Optional[dict]]:
    """
    Build a cache mapping each directory to its governing key.csv data.

    Inputs:
        scan_root: The root scan folder path.
        directories: Set of directory paths encountered during scanning.
    Outputs: Dict mapping directory path -> parsed key.csv dict (or None if
             no key.csv found). Each dict includes '_path' pointing to the
             actual key.csv file.

    This avoids re-parsing the same key.csv for every file. We cache by
    the key.csv path itself, then map each directory to the right cache entry.
    """
    # First pass: find unique key.csv files and parse each once
    key_csv_by_path: dict[str, dict] = {}
    dir_to_key_path: dict[str, Optional[str]] = {}

    for directory in directories:
        key_path = find_key_csv(directory)
        dir_to_key_path[directory] = key_path
        if key_path and key_path not in key_csv_by_path:
            key_csv_by_path[key_path] = parse_key_csv(key_path)

    # Second pass: map each directory to parsed data
    cache: dict[str, Optional[dict]] = {}
    for directory in directories:
        key_path = dir_to_key_path[directory]
        if key_path:
            cache[directory] = key_csv_by_path[key_path]
        else:
            cache[directory] = None

    return cache


def save_key_csv(file_path: str, updates: dict[str, Optional[str]]) -> None:
    """
    Update specific fields in a key.csv file on disk.

    Inputs:
        file_path: Path to the key.csv to modify.
        updates: Dict of {normalized_key: new_value}. Keys should match
                 the values in KEY_CSV_COLUMN_MAPPINGS (e.g. 'CopyrightHolder').
                 Set value to None to clear a field.
    Outputs: None. Writes the modified file to disk.

    THIS IS THE ONE WRITE OPERATION. Be careful.

    Strategy: Read existing rows, update matching fields, write back.
    Preserves row ordering and any extra columns/rows not in our mappings.
    """
    # Build reverse mapping: normalized key -> csv key
    reverse_map = {v: k for k, v in KEY_CSV_COLUMN_MAPPINGS.items()}

    rows = _read_csv_rows(file_path)
    updated_keys = set()

    for i, row in enumerate(rows):
        if not row:
            continue
        csv_key = row[0].strip().lower()
        if csv_key in KEY_CSV_COLUMN_MAPPINGS:
            normalized = KEY_CSV_COLUMN_MAPPINGS[csv_key]
            if normalized in updates:
                new_val = updates[normalized] if updates[normalized] is not None else ""
                if len(row) > 1:
                    rows[i][1] = new_val
                else:
                    rows[i].append(new_val)
                updated_keys.add(normalized)

    # Append any fields from updates that weren't already in the file
    for normalized_key, new_val in updates.items():
        if normalized_key not in updated_keys and normalized_key in reverse_map:
            csv_key = reverse_map[normalized_key]
            rows.append([csv_key, new_val if new_val is not None else ""])

    # Write back
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)

    with open(file_path, "w", encoding="utf-8", newline="") as f:
        f.write(output.getvalue())
