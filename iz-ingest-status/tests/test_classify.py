"""
Unit tests for app/classify.py — CASIZ extraction and file state classification.

Tests cover:
  - Extension validation
  - CRRF path detection
  - Dot-prefix detection
  - CASIZ number extraction from strings, filenames, directories
  - Full file classification through all 9 states
"""

import os
import tempfile

import pytest

from app.classify import (
    ClassificationResult,
    classify_file,
    extract_casiz_from_directory,
    extract_casiz_from_filename,
    extract_casiz_from_string,
    get_casiz_numbers,
    has_valid_extension,
    is_crrf_path,
    is_dot_prefixed,
)


# -- Extension validation --


class TestHasValidExtension:
    def test_jpg_lowercase(self):
        assert has_valid_extension("/path/to/image.jpg") is True

    def test_jpg_uppercase(self):
        assert has_valid_extension("/path/to/IMAGE.JPG") is True

    def test_jpeg(self):
        assert has_valid_extension("/path/to/photo.jpeg") is True

    def test_tiff(self):
        assert has_valid_extension("/path/to/scan.tiff") is True

    def test_tif(self):
        assert has_valid_extension("/path/to/scan.tif") is True

    def test_png(self):
        assert has_valid_extension("/path/to/diagram.png") is True

    def test_dng(self):
        assert has_valid_extension("/path/to/raw.dng") is True

    def test_cr2_forbidden(self):
        assert has_valid_extension("/path/to/photo.CR2") is False

    def test_xmp_forbidden(self):
        assert has_valid_extension("/path/to/sidecar.xmp") is False

    def test_db_forbidden(self):
        assert has_valid_extension("/path/to/Thumbs.db") is False

    def test_csv_forbidden(self):
        assert has_valid_extension("/path/to/key.csv") is False

    def test_no_extension(self):
        assert has_valid_extension("/path/to/README") is False

    def test_complex_filename_with_valid_ext(self):
        assert has_valid_extension("/path/to/casiz_12345 (1).jpg") is True

    def test_filename_with_copyright_symbol(self):
        assert has_valid_extension("/path/to/photo©smith.jpg") is True


# -- CRRF detection --


class TestIsCrrfPath:
    def test_crrf_in_path(self):
        assert is_crrf_path("/path/to/crrf/image.jpg") is True

    def test_crrf_uppercase(self):
        assert is_crrf_path("/path/to/CRRF/image.jpg") is True

    def test_crrf_mixed_case(self):
        assert is_crrf_path("/path/to/Crrf_files/image.jpg") is True

    def test_no_crrf(self):
        assert is_crrf_path("/path/to/normal/image.jpg") is False


# -- Dot prefix detection --


class TestIsDotPrefixed:
    def test_dot_file(self):
        assert is_dot_prefixed("/path/to/.hidden.jpg") is True

    def test_dot_ds_store(self):
        assert is_dot_prefixed("/path/to/.DS_Store") is True

    def test_normal_file(self):
        assert is_dot_prefixed("/path/to/image.jpg") is False


# -- CASIZ extraction from strings --


class TestExtractCasizFromString:
    def test_casiz_prefix(self):
        """CASIZ followed by number."""
        result = extract_casiz_from_string("CASIZ 123456")
        assert 123456 in result

    def test_cas_prefix(self):
        """CAS followed by number."""
        result = extract_casiz_from_string("CAS 123456")
        assert 123456 in result

    def test_casiz_underscore(self):
        result = extract_casiz_from_string("CASIZ_098765")
        assert 98765 in result

    def test_casiz_hash(self):
        result = extract_casiz_from_string("CASIZ#12345")
        assert 12345 in result

    def test_casiz_dash(self):
        result = extract_casiz_from_string("CASIZ-54321")
        assert 54321 in result

    def test_no_prefix_5_digits(self):
        """Without prefix, needs at least 5 digits and no word char before."""
        result = extract_casiz_from_string("photo 12345 view1")
        assert 12345 in result

    def test_no_prefix_4_digits_rejected(self):
        """Without prefix, 4 digits is not enough."""
        result = extract_casiz_from_string("photo_1234_view1")
        assert 1234 not in result

    def test_with_prefix_3_digits_ok(self):
        """With prefix, 3 digits is enough."""
        result = extract_casiz_from_string("CASIZ 123")
        assert 123 in result

    def test_leading_zeros_stripped(self):
        result = extract_casiz_from_string("CASIZ 00123")
        assert 123 in result

    def test_camera_serial_excluded_dsc(self):
        """DSC_NNNNN should not match."""
        result = extract_casiz_from_string("DSC_12345.jpg")
        assert result == []

    def test_camera_serial_excluded_p(self):
        """P_NNNNN should not match."""
        result = extract_casiz_from_string("P12345.jpg")
        assert result == []

    def test_date_pattern_excluded(self):
        """YYYYMMDD date patterns should not match without a prefix."""
        result = extract_casiz_from_string("20250115_photo.jpg")
        assert result == []

    def test_date_with_casiz_prefix_ok(self):
        """Date-like number after CASIZ prefix should still match."""
        result = extract_casiz_from_string("CASIZ 20250115")
        assert 20250115 in result

    def test_izacc_suppresses_bare_numbers(self):
        """When IZACC is present, bare numbers (no prefix) are suppressed."""
        result = extract_casiz_from_string("IZACC 12345 67890")
        # 67890 is bare, should be suppressed due to izacc
        assert 12345 not in result
        assert 67890 not in result

    def test_izacc_with_casiz_prefix_allowed(self):
        """IZACC presence doesn't suppress prefixed matches."""
        result = extract_casiz_from_string("IZACC something CASIZ 12345")
        assert 12345 in result

    def test_multiple_numbers_and_or(self):
        """AND/OR bridges allow multiple matches (bridged numbers need 5+ digits without prefix)."""
        result = extract_casiz_from_string("CASIZ 11111 AND 22222 OR 33333")
        assert 11111 in result
        assert 22222 in result
        assert 33333 in result

    def test_fallback_regex(self):
        """Fallback regex catches simple CASIZ/CAS patterns."""
        # This string won't match the main regex but should hit fallback
        result = extract_casiz_from_string("cas12345")
        assert 12345 in result

    def test_empty_string(self):
        result = extract_casiz_from_string("")
        assert result == []

    def test_no_numbers(self):
        result = extract_casiz_from_string("just some text with no numbers")
        assert result == []

    def test_max_digits(self):
        """12-digit number is OK."""
        result = extract_casiz_from_string("CASIZ 123456789012")
        assert 123456789012 in result

    def test_over_max_digits_rejected(self):
        """13+ digit number should not match."""
        result = extract_casiz_from_string("CASIZ 1234567890123")
        assert 1234567890123 not in result


# -- CASIZ extraction from filenames and directories --


class TestExtractCasizFromFilename:
    def test_casiz_in_filename(self):
        result = extract_casiz_from_filename("/path/to/CASIZ_123456.jpg")
        assert 123456 in result

    def test_bare_number_in_filename(self):
        """Bare number in filename — must not be preceded by a word character."""
        result = extract_casiz_from_filename("/path/to/54321.jpg")
        assert 54321 in result

    def test_no_casiz_in_filename(self):
        result = extract_casiz_from_filename("/path/to/landscape.jpg")
        assert result == []


class TestExtractCasizFromDirectory:
    def test_casiz_in_directory_name(self):
        result = extract_casiz_from_directory("/scans/CASIZ 123456/image.jpg")
        assert 123456 in result

    def test_casiz_in_parent_directory(self):
        result = extract_casiz_from_directory("/scans/photographer/CASIZ 789/subfolder/image.jpg")
        assert 789 in result

    def test_no_casiz_in_directory(self):
        result = extract_casiz_from_directory("/scans/photographer/general/image.jpg")
        assert result == []


class TestGetCasizNumbers:
    def test_filename_takes_priority(self):
        """Filename match should be preferred over directory match."""
        numbers, source = get_casiz_numbers("/CASIZ_99999/CASIZ_11111.jpg")
        assert source == "filename"
        assert 11111 in numbers

    def test_falls_back_to_directory(self):
        numbers, source = get_casiz_numbers("/scans/CASIZ_22222/landscape.jpg")
        assert source == "directory"
        assert 22222 in numbers

    def test_no_match_returns_none(self):
        numbers, source = get_casiz_numbers("/scans/photographer/photo.jpg")
        assert numbers == []
        assert source is None


# -- Full classification --


class TestClassifyFile:
    """
    Test the full classification pipeline using a temp directory for real files.
    """

    @pytest.fixture
    def temp_tree(self, tmp_path):
        """
        Create a temporary directory tree with test files and key.csv files.
        Returns a dict with paths to the created items.
        """
        # Photographer folder with key.csv
        photographer = tmp_path / "photographer"
        photographer.mkdir()

        key_csv = photographer / "key.csv"
        key_csv.write_text("CopyrightHolder,John Smith\nIsPublic,TRUE\n")

        # Normal image file
        img = photographer / "CASIZ_12345.jpg"
        img.write_text("fake image data")

        # Folder with remove=true key.csv
        removed_dir = tmp_path / "removed_folder"
        removed_dir.mkdir()
        remove_key = removed_dir / "key.csv"
        remove_key.write_text("CopyrightHolder,Jane\nRemove,TRUE\n")
        removed_img = removed_dir / "CASIZ_99999.jpg"
        removed_img.write_text("fake")

        # Folder with no key.csv
        no_key_dir = tmp_path / "no_key_folder"
        no_key_dir.mkdir()
        no_key_img = no_key_dir / "CASIZ_55555.jpg"
        no_key_img.write_text("fake")

        return {
            "root": tmp_path,
            "photographer": photographer,
            "img": img,
            "removed_img": removed_img,
            "no_key_img": no_key_img,
        }

    def _build_cache(self, temp_tree):
        """Build key.csv cache for the test tree."""
        from app.key_csv import build_key_csv_cache

        dirs = {
            str(temp_tree["photographer"]),
            str(temp_tree["root"] / "removed_folder"),
            str(temp_tree["root"] / "no_key_folder"),
        }
        return build_key_csv_cache(str(temp_tree["root"]), dirs)

    def test_forbidden_extension(self, temp_tree):
        cache = self._build_cache(temp_tree)
        cr2 = temp_tree["photographer"] / "photo.CR2"
        cr2.write_text("fake")
        result = classify_file(str(cr2), set(), cache)
        assert result.state == "forbidden_extension"

    def test_crrf_path(self, temp_tree):
        cache = self._build_cache(temp_tree)
        crrf_dir = temp_tree["root"] / "crrf"
        crrf_dir.mkdir()
        crrf_img = crrf_dir / "image.jpg"
        crrf_img.write_text("fake")
        result = classify_file(str(crrf_img), set(), cache)
        assert result.state == "skipping_crrf"

    def test_dot_prefixed(self, temp_tree):
        cache = self._build_cache(temp_tree)
        dot_file = temp_tree["photographer"] / ".hidden.jpg"
        dot_file.write_text("fake")
        result = classify_file(str(dot_file), set(), cache)
        assert result.state == "dot_prefixed"

    def test_removed(self, temp_tree):
        cache = self._build_cache(temp_tree)
        result = classify_file(str(temp_tree["removed_img"]), set(), cache)
        assert result.state == "removed"
        assert result.has_remove_flag is True

    def test_missing_key_csv(self, temp_tree):
        cache = self._build_cache(temp_tree)
        result = classify_file(str(temp_tree["no_key_img"]), set(), cache)
        assert result.state == "missing_key_csv"

    def test_no_casiz_match(self, temp_tree):
        cache = self._build_cache(temp_tree)
        no_casiz = temp_tree["photographer"] / "landscape.jpg"
        no_casiz.write_text("fake")
        result = classify_file(str(no_casiz), set(), cache)
        assert result.state == "no_casiz_match"

    def test_ingested(self, temp_tree):
        cache = self._build_cache(temp_tree)
        ingested_set = {"casiz_12345.jpg"}
        result = classify_file(str(temp_tree["img"]), ingested_set, cache)
        assert result.state == "ingested"
        assert 12345 in result.casiz_numbers

    def test_pending(self, temp_tree):
        """File with CASIZ number, specimen exists in Specify, not yet ingested."""
        cache = self._build_cache(temp_tree)
        # Provide specimen catalog numbers so pending state is reachable
        specimen_nums = {"12345"}
        result = classify_file(str(temp_tree["img"]), set(), cache, specimen_nums)
        assert result.state == "pending"
        assert 12345 in result.casiz_numbers
        assert result.casiz_source == "filename"

    def test_pending_without_specimen_set(self, temp_tree):
        """When specimen_catalog_numbers is None, no_specimen check is skipped."""
        cache = self._build_cache(temp_tree)
        result = classify_file(str(temp_tree["img"]), set(), cache, None)
        assert result.state == "pending"

    def test_no_specimen_record(self, temp_tree):
        """File with CASIZ number but no matching specimen in Specify."""
        cache = self._build_cache(temp_tree)
        # Empty specimen set — no catalog numbers in Specify match
        specimen_nums: set[str] = set()
        result = classify_file(str(temp_tree["img"]), set(), cache, specimen_nums)
        assert result.state == "no_specimen_record"
        assert 12345 in result.casiz_numbers

    def test_no_specimen_with_unrelated_specimens(self, temp_tree):
        """CASIZ 12345 but only specimen 99999 exists — still no_specimen_record."""
        cache = self._build_cache(temp_tree)
        specimen_nums = {"99999", "88888"}
        result = classify_file(str(temp_tree["img"]), set(), cache, specimen_nums)
        assert result.state == "no_specimen_record"

    def test_file_metadata_populated(self, temp_tree):
        cache = self._build_cache(temp_tree)
        result = classify_file(str(temp_tree["img"]), set(), cache)
        assert result.file_size is not None
        assert result.file_size > 0
        assert result.file_mtime is not None
