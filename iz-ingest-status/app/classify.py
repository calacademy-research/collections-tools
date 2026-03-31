"""
File classification engine.

Ported from /admin/web-asset-importer/iz_importer.py.
Classifies each file on disk into one of 8 states by checking conditions in order.

Inputs: A file path (str) and context (set of ingested filenames, key.csv cache).
Outputs: ClassificationResult with state, casiz_numbers, casiz_source, key_csv_path.

The 9 states (checked in this order):
  1. forbidden_extension — Extension not in allowed set
  2. skipping_crrf — Path contains 'crrf'
  3. dot_prefixed — Basename starts with '.'
  4. missing_key_csv — No key.csv in directory tree (classification continues)
  5. removed — Nearest key.csv has remove=true
  6. no_casiz_match — No CASIZ number extractable from filename or directory
  7. ingested — origFilename found in Specify attachment table
  8. no_specimen_record — CASIZ number has no matching collectionobject in Specify
  9. pending — Has CASIZ number, specimen exists, not yet ingested
"""

import os
from dataclasses import dataclass, field
from typing import Optional

import regex

from app.config import (
    CASIZ_FALLBACK_REGEX,
    CASIZ_NUMBER_REGEX,
    IMAGE_PATH_REGEX,
    MAXIMUM_ID_DIGITS,
    MINIMUM_ID_DIGITS,
    MINIMUM_ID_DIGITS_WITH_PREFIX,
)


@dataclass
class ClassificationResult:
    """
    Result of classifying a single file.

    Fields:
        file_path: Absolute path to the file on disk.
        state: One of the 8 classification states.
        casiz_numbers: List of extracted CASIZ integers (may be empty).
        casiz_source: How CASIZ was found — 'filename', 'directory', or None.
        key_csv_path: Path to the governing key.csv, or None if missing.
        has_remove_flag: Whether the key.csv has remove=true.
        file_size: File size in bytes (from os.stat).
        file_mtime: File modification time as ISO string.
    """

    file_path: str
    state: str
    casiz_numbers: list[int] = field(default_factory=list)
    casiz_source: Optional[str] = None
    key_csv_path: Optional[str] = None
    has_remove_flag: bool = False
    file_size: Optional[int] = None
    file_mtime: Optional[str] = None


def has_valid_extension(file_path: str) -> bool:
    """
    Check if a file has an allowed image extension.

    Inputs: file_path (str) — full path or just filename.
    Outputs: True if extension matches (jpg|jpeg|tiff|tif|png|dng), case-insensitive.

    Ported from IzImporter.include_by_extension(). Uses the same regex pattern
    from iz_config.py IMAGE_SUFFIX.
    """
    return bool(IMAGE_PATH_REGEX.match(file_path.lower()))


def is_crrf_path(file_path: str) -> bool:
    """
    Check if path contains 'crrf' (case-insensitive).

    Inputs: file_path (str).
    Outputs: True if 'crrf' appears anywhere in the lowercased path.

    Ported from IzImporter.validate_path() — the importer rejects all CRRF files.
    """
    return "crrf" in file_path.lower()


def is_dot_prefixed(file_path: str) -> bool:
    """
    Check if the filename starts with a dot (hidden file).

    Inputs: file_path (str).
    Outputs: True if basename starts with '.'.

    Ported from IzImporter._should_skip_file().
    """
    return os.path.basename(file_path).startswith(".")


def extract_casiz_from_string(input_string: str) -> list[int]:
    """
    Extract CASIZ numbers from a string using the main regex, falling back
    to the simpler fallback regex.

    Inputs: input_string (str) — a filename, directory name, or other text.
    Outputs: List of unique CASIZ integers found. Empty list if none.

    Ported from IzImporter.extract_casiz_from_string(). Logic:
      1. Check if 'izacc' appears in the string (suppresses prefix-less matches).
      2. Iterate through main regex matches.
      3. For each match, validate digit count (with/without prefix).
      4. Check AND/OR bridging between consecutive matches.
      5. If no matches, try fallback regex.
      6. Return deduplicated list of integers.
    """
    # Check for IZACC presence — suppresses prefix-less matches
    has_izacc = regex.search(r"(?i)\bizacc\b", input_string) is not None

    matches = []
    pos = 0
    last_prefix_match_end = -1

    while pos < len(input_string):
        match = CASIZ_NUMBER_REGEX.search(input_string, pos)
        if not match:
            break

        number_str = match.group("number")
        prefix = match.group("prefix")
        number_start, number_end = match.span()

        # Strip leading zeros for digit-count validation
        stripped = number_str.lstrip("0") or "0"
        valid_length = len(stripped)

        # If IZACC is present, skip matches without a prefix
        if has_izacc and not prefix:
            pos = number_end
            continue

        # Validate digit count based on whether there's a prefix
        if prefix:
            if not (MINIMUM_ID_DIGITS_WITH_PREFIX <= valid_length <= MAXIMUM_ID_DIGITS):
                pos = number_end
                last_prefix_match_end = number_end
                continue
        else:
            if not (MINIMUM_ID_DIGITS <= valid_length <= MAXIMUM_ID_DIGITS):
                pos = number_end
                last_prefix_match_end = number_end
                continue

        # Check AND/OR bridge between consecutive matches
        if last_prefix_match_end != -1 and number_start > last_prefix_match_end:
            bridge_text = input_string[last_prefix_match_end:number_start]
            if not regex.fullmatch(r"(?i)[\s]*(AND|OR)[\s]*", bridge_text):
                break

        matches.append(int(stripped))
        pos = number_end
        last_prefix_match_end = number_end

    # Fallback regex if main regex found nothing
    if not matches:
        for match in CASIZ_FALLBACK_REGEX.finditer(input_string):
            number_str = match.group(1)
            stripped = number_str.lstrip("0") or "0"
            valid_length = len(stripped)
            if MINIMUM_ID_DIGITS_WITH_PREFIX <= valid_length <= MAXIMUM_ID_DIGITS:
                matches.append(int(stripped))

    return list(set(matches))


def extract_casiz_from_filename(file_path: str) -> list[int]:
    """
    Try to extract CASIZ numbers from the filename portion of a path.

    Inputs: file_path (str) — full file path.
    Outputs: List of CASIZ integers, or empty list.

    Ported from IzImporter.attempt_filename_match().
    """
    filename = os.path.basename(file_path)
    return extract_casiz_from_string(filename)


def extract_casiz_from_directory(file_path: str) -> list[int]:
    """
    Try to extract CASIZ numbers from directory path segments, walking
    from deepest to shallowest (reversed).

    Inputs: file_path (str) — full file path.
    Outputs: List of unique CASIZ integers, or empty list.

    Ported from IzImporter.attempt_directory_match(). Splits the directory
    portion on '/' and checks each segment in reverse order.
    """
    directory = os.path.dirname(file_path)
    segments = directory.split("/")
    all_numbers = []
    for segment in reversed(segments):
        numbers = extract_casiz_from_string(segment)
        all_numbers.extend(numbers)
    return list(set(all_numbers))


def get_casiz_numbers(file_path: str) -> tuple[list[int], Optional[str]]:
    """
    Extract CASIZ numbers using the priority chain: filename first, then directory.

    Inputs: file_path (str) — full file path.
    Outputs: Tuple of (casiz_numbers, source) where source is 'filename',
             'directory', or None if no numbers found.

    Note: The importer also checks EXIF metadata as a third source. We skip
    EXIF because it's too slow for 133K files. Files that only match via
    EXIF will show as no_casiz_match. This is a documented limitation.
    """
    # Priority 1: filename
    numbers = extract_casiz_from_filename(file_path)
    if numbers:
        return numbers, "filename"

    # Priority 2: directory segments
    numbers = extract_casiz_from_directory(file_path)
    if numbers:
        return numbers, "directory"

    return [], None


def classify_file(
    file_path: str,
    ingested_filenames: set[str],
    key_csv_cache: dict[str, dict],
    specimen_catalog_numbers: Optional[set[str]] = None,
) -> ClassificationResult:
    """
    Classify a single file into one of 9 states.

    Inputs:
        file_path: Absolute path to the file on disk.
        ingested_filenames: Set of lowercased origFilename values from Specify.
        key_csv_cache: Dict mapping directory path -> parsed key.csv dict.
            Populated by key_csv.find_key_csv_for_directory(). Value is None
            if no key.csv was found for that directory tree.
        specimen_catalog_numbers: Set of catalogNumber strings from Specify's
            collectionobject table (leading zeros stripped). If None, the
            no_specimen_record check is skipped (backwards compatible).

    Outputs: ClassificationResult with state and metadata.

    Classification order (first match wins, except missing_key_csv which
    sets a flag but continues):
      1. forbidden_extension
      2. skipping_crrf
      3. dot_prefixed
      4. missing_key_csv (flag set, classification continues)
      5. removed (key.csv has remove=true)
      6. no_casiz_match
      7. ingested (basename in Specify attachment table)
      8. no_specimen_record (CASIZ has no matching specimen in Specify)
      9. pending (has CASIZ, specimen exists, not yet ingested)
    """
    result = ClassificationResult(file_path=file_path, state="pending")

    # Grab file stats if available
    try:
        stat = os.stat(file_path)
        result.file_size = stat.st_size
        from datetime import datetime, timezone

        result.file_mtime = datetime.fromtimestamp(
            stat.st_mtime, tz=timezone.utc
        ).isoformat()
    except OSError:
        pass

    # 1. Check extension
    if not has_valid_extension(file_path):
        result.state = "forbidden_extension"
        return result

    # 2. Check CRRF
    if is_crrf_path(file_path):
        result.state = "skipping_crrf"
        return result

    # 3. Check dot-prefixed
    if is_dot_prefixed(file_path):
        result.state = "dot_prefixed"
        return result

    # 4. Find key.csv — look up the file's directory in the cache.
    # The cache is pre-populated by the scanner with resolved key.csv paths.
    file_dir = os.path.dirname(file_path)
    key_info = key_csv_cache.get(file_dir)
    missing_key = False

    if key_info is None:
        # No key.csv found walking up the tree
        missing_key = True
    else:
        result.key_csv_path = key_info.get("_path")

        # 5. Check remove flag
        remove_val = key_info.get("remove")
        if remove_val and str(remove_val).strip().lower() in ("true", "1", "yes"):
            result.has_remove_flag = True
            result.state = "removed"
            # Still extract CASIZ for reporting, but state is removed
            casiz_numbers, casiz_source = get_casiz_numbers(file_path)
            result.casiz_numbers = casiz_numbers
            result.casiz_source = casiz_source
            return result

    # 6. Extract CASIZ numbers
    casiz_numbers, casiz_source = get_casiz_numbers(file_path)
    result.casiz_numbers = casiz_numbers
    result.casiz_source = casiz_source

    if not casiz_numbers:
        # If also missing key.csv, prioritize missing_key_csv state
        if missing_key:
            result.state = "missing_key_csv"
        else:
            result.state = "no_casiz_match"
        return result

    # At this point we have CASIZ numbers. Set missing_key_csv if applicable.
    if missing_key:
        result.state = "missing_key_csv"
        return result

    # 7. Check if ingested (basename in Specify attachment set)
    basename_lower = os.path.basename(file_path).lower()
    if basename_lower in ingested_filenames:
        result.state = "ingested"
        return result

    # 8. Check if specimen exists in Specify for any extracted CASIZ number.
    # The importer does: SELECT collectionobjectid FROM collectionobject
    #   WHERE catalognumber=%s — if no row, the file is skipped entirely.
    # Files whose CASIZ numbers have no matching specimen will never be ingested.
    if specimen_catalog_numbers is not None:
        has_specimen = any(
            str(n) in specimen_catalog_numbers for n in casiz_numbers
        )
        if not has_specimen:
            result.state = "no_specimen_record"
            return result

    # 9. Pending — has CASIZ, specimen exists, not yet ingested
    result.state = "pending"
    return result
