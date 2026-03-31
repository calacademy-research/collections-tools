"""
Unit tests for app/key_csv.py — key.csv parsing, inheritance, and editing.

Tests cover:
  - Finding key.csv by walking up directory tree
  - Parsing key.csv with various formats and encodings
  - Detecting the remove flag
  - Building the key.csv cache for multiple directories
  - Saving edits to key.csv files
"""

import csv
import os
import tempfile

import pytest

from app.key_csv import (
    build_key_csv_cache,
    find_key_csv,
    is_removed,
    parse_key_csv,
    save_key_csv,
)


@pytest.fixture
def key_tree(tmp_path):
    """
    Create a directory tree with key.csv files at various levels.

    Structure:
        root/
            key.csv              (CopyrightHolder=Root Holder)
            subdir_a/
                key.csv          (CopyrightHolder=Sub A, Remove=TRUE)
                deep/
                    (no key.csv — inherits from subdir_a)
            subdir_b/
                (no key.csv — inherits from root)
            no_key/
                nested/
                    (no key.csv anywhere above no_key — root key.csv is the catch)
    """
    # Root key.csv
    root_key = tmp_path / "key.csv"
    root_key.write_text(
        "CopyrightHolder,Root Holder\n"
        "IsPublic,TRUE\n"
        "License,CC BY 4.0\n"
    )

    # subdir_a with its own key.csv
    subdir_a = tmp_path / "subdir_a"
    subdir_a.mkdir()
    key_a = subdir_a / "key.csv"
    key_a.write_text(
        "CopyrightHolder,Sub A Person\n"
        "Remove,TRUE\n"
        "Credit,CAS\n"
    )

    # Deep nested dir under subdir_a (no key.csv — inherits from subdir_a)
    deep = subdir_a / "deep"
    deep.mkdir()

    # subdir_b (no key.csv — inherits from root)
    subdir_b = tmp_path / "subdir_b"
    subdir_b.mkdir()

    return {
        "root": tmp_path,
        "subdir_a": subdir_a,
        "deep": deep,
        "subdir_b": subdir_b,
    }


# -- find_key_csv --


class TestFindKeyCsv:
    def test_finds_in_same_directory(self, key_tree):
        result = find_key_csv(str(key_tree["root"]))
        assert result == str(key_tree["root"] / "key.csv")

    def test_finds_in_parent(self, key_tree):
        """deep/ has no key.csv, should find subdir_a's."""
        result = find_key_csv(str(key_tree["deep"]))
        assert result == str(key_tree["subdir_a"] / "key.csv")

    def test_finds_in_grandparent(self, key_tree):
        """subdir_b has no key.csv, should find root's."""
        result = find_key_csv(str(key_tree["subdir_b"]))
        assert result == str(key_tree["root"] / "key.csv")

    def test_returns_none_at_root(self, tmp_path):
        """Empty directory with no key.csv anywhere."""
        empty = tmp_path / "empty"
        empty.mkdir()
        result = find_key_csv(str(empty))
        assert result is None


# -- parse_key_csv --


class TestParseKeyCsv:
    def test_basic_parsing(self, key_tree):
        result = parse_key_csv(str(key_tree["root"] / "key.csv"))
        assert result["CopyrightHolder"] == "Root Holder"
        assert result["IsPublic"] == "TRUE"
        assert result["License"] == "CC BY 4.0"
        assert result["_path"] == str(key_tree["root"] / "key.csv")

    def test_remove_flag_parsed(self, key_tree):
        result = parse_key_csv(str(key_tree["subdir_a"] / "key.csv"))
        assert result["remove"] == "TRUE"
        assert result["CopyrightHolder"] == "Sub A Person"

    def test_missing_fields_are_none(self, key_tree):
        result = parse_key_csv(str(key_tree["root"] / "key.csv"))
        assert result["remove"] is None
        assert result["Remarks"] is None

    def test_case_insensitive_keys(self, tmp_path):
        key = tmp_path / "key.csv"
        key.write_text("COPYRIGHTHOLDER,All Caps\ncredit,Lowercase\n")
        result = parse_key_csv(str(key))
        assert result["CopyrightHolder"] == "All Caps"
        assert result["Credit"] == "Lowercase"

    def test_latin1_fallback(self, tmp_path):
        """File with Latin-1 encoding should still parse."""
        key = tmp_path / "key.csv"
        key.write_bytes(b"CopyrightHolder,Ren\xe9 M\xfcller\n")
        result = parse_key_csv(str(key))
        assert result["CopyrightHolder"] == "René Müller"

    def test_empty_values_are_none(self, tmp_path):
        key = tmp_path / "key.csv"
        key.write_text("CopyrightHolder,\nCredit,  \n")
        result = parse_key_csv(str(key))
        assert result["CopyrightHolder"] is None
        assert result["Credit"] is None

    def test_raw_rows_preserved(self, key_tree):
        result = parse_key_csv(str(key_tree["root"] / "key.csv"))
        assert "_raw_rows" in result
        assert len(result["_raw_rows"]) == 3


# -- is_removed --


class TestIsRemoved:
    def test_true_uppercase(self):
        assert is_removed({"remove": "TRUE"}) is True

    def test_true_lowercase(self):
        assert is_removed({"remove": "true"}) is True

    def test_true_mixed_case(self):
        assert is_removed({"remove": "True"}) is True

    def test_yes(self):
        assert is_removed({"remove": "yes"}) is True

    def test_one(self):
        assert is_removed({"remove": "1"}) is True

    def test_false(self):
        assert is_removed({"remove": "FALSE"}) is False

    def test_none(self):
        assert is_removed({"remove": None}) is False

    def test_missing_key(self):
        assert is_removed({}) is False


# -- build_key_csv_cache --


class TestBuildKeyCsvCache:
    def test_cache_maps_directories_correctly(self, key_tree):
        dirs = {
            str(key_tree["root"]),
            str(key_tree["subdir_a"]),
            str(key_tree["deep"]),
            str(key_tree["subdir_b"]),
        }
        cache = build_key_csv_cache(str(key_tree["root"]), dirs)

        # Root uses root's key.csv
        assert cache[str(key_tree["root"])]["CopyrightHolder"] == "Root Holder"

        # subdir_a uses its own key.csv
        assert cache[str(key_tree["subdir_a"])]["CopyrightHolder"] == "Sub A Person"
        assert cache[str(key_tree["subdir_a"])]["remove"] == "TRUE"

        # deep inherits from subdir_a
        assert cache[str(key_tree["deep"])]["CopyrightHolder"] == "Sub A Person"

        # subdir_b inherits from root
        assert cache[str(key_tree["subdir_b"])]["CopyrightHolder"] == "Root Holder"

    def test_cache_returns_none_for_no_key(self, tmp_path):
        orphan = tmp_path / "orphan"
        orphan.mkdir()
        cache = build_key_csv_cache(str(tmp_path), {str(orphan)})
        assert cache[str(orphan)] is None


# -- save_key_csv --


class TestSaveKeyCsv:
    def test_update_existing_field(self, key_tree):
        key_path = str(key_tree["root"] / "key.csv")
        save_key_csv(key_path, {"CopyrightHolder": "New Holder"})

        result = parse_key_csv(key_path)
        assert result["CopyrightHolder"] == "New Holder"
        # Other fields preserved
        assert result["License"] == "CC BY 4.0"

    def test_add_new_field(self, key_tree):
        key_path = str(key_tree["root"] / "key.csv")
        save_key_csv(key_path, {"Remarks": "Test remark"})

        result = parse_key_csv(key_path)
        assert result["Remarks"] == "Test remark"
        # Existing fields preserved
        assert result["CopyrightHolder"] == "Root Holder"

    def test_clear_field(self, key_tree):
        key_path = str(key_tree["root"] / "key.csv")
        save_key_csv(key_path, {"CopyrightHolder": None})

        result = parse_key_csv(key_path)
        assert result["CopyrightHolder"] is None

    def test_toggle_remove(self, key_tree):
        key_path = str(key_tree["subdir_a"] / "key.csv")
        # Currently TRUE, set to FALSE
        save_key_csv(key_path, {"remove": "FALSE"})

        result = parse_key_csv(key_path)
        assert result["remove"] == "FALSE"
        assert is_removed(result) is False
