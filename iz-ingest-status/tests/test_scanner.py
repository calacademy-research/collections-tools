"""
Integration tests for app/scanner.py — filesystem scanning and classification.

Uses a fixture directory tree (created in tmp_path) to test the full scan
pipeline without needing the real NFS mount or Specify database.
"""

import os
from unittest.mock import patch

import pytest

from app.config import Settings
from app.database import Database
from app.scanner import run_scan


@pytest.fixture
def scan_tree(tmp_path):
    """
    Build a realistic fixture directory tree for integration testing.

    Structure:
        scan_root/
            photographer_a/
                key.csv         (CopyrightHolder=Photo A, IsPublic=TRUE)
                CASIZ_12345.jpg (should be pending or ingested)
                CASIZ_67890.jpg (should be pending or ingested)
                landscape.jpg   (no_casiz_match)
                photo.CR2       (forbidden_extension)
                .hidden.jpg     (dot_prefixed)
                subfolder/
                    CASIZ_11111.jpg (inherits key.csv from parent)
            photographer_b/
                key.csv         (Remove=TRUE)
                CASIZ_99999.jpg (removed)
            no_key_folder/
                CASIZ_55555.jpg (missing_key_csv)
            crrf_stuff/
                image.jpg       (skipping_crrf)
    """
    root = tmp_path / "scan_root"
    root.mkdir()

    # photographer_a
    photo_a = root / "photographer_a"
    photo_a.mkdir()
    (photo_a / "key.csv").write_text(
        "CopyrightHolder,Photo A\nIsPublic,TRUE\n"
    )
    (photo_a / "CASIZ_12345.jpg").write_text("img1")
    (photo_a / "CASIZ_67890.jpg").write_text("img2")
    (photo_a / "landscape.jpg").write_text("img3")
    (photo_a / "photo.CR2").write_text("raw")
    (photo_a / ".hidden.jpg").write_text("hidden")

    subfolder = photo_a / "subfolder"
    subfolder.mkdir()
    (subfolder / "CASIZ_11111.jpg").write_text("img4")

    # photographer_b with remove=true
    photo_b = root / "photographer_b"
    photo_b.mkdir()
    (photo_b / "key.csv").write_text("CopyrightHolder,Photo B\nRemove,TRUE\n")
    (photo_b / "CASIZ_99999.jpg").write_text("img5")

    # no_key_folder — no key.csv anywhere
    no_key = root / "no_key_folder"
    no_key.mkdir()
    (no_key / "CASIZ_55555.jpg").write_text("img6")

    # crrf folder
    crrf = root / "crrf_stuff"
    crrf.mkdir()
    (crrf / "image.jpg").write_text("img7")

    return root


@pytest.fixture
def settings(scan_tree):
    """Create settings pointing to the fixture tree."""
    return Settings(
        scan_root=str(scan_tree),
        sqlite_path=":memory:",
    )


@pytest.fixture
def db():
    """Create an in-memory database."""
    database = Database(":memory:")
    yield database
    database.close()


class TestRunScan:
    def test_full_scan_with_mock_specify(self, settings, db, scan_tree):
        """Run a full scan with mocked Specify data."""
        # Mock: CASIZ_12345.jpg is already ingested
        mock_filenames = {"casiz_12345.jpg"}
        # Specimen records exist for all test CASIZ numbers
        mock_specimens = {"12345", "67890", "11111", "99999", "55555"}

        with patch(
            "app.specify_client.fetch_ingested_filenames", return_value=mock_filenames
        ), patch(
            "app.specify_client.fetch_specimen_catalog_numbers",
            return_value=mock_specimens,
        ):
            scan_id = run_scan(settings, db)

        # Check scan record
        scan = db.get_scan(scan_id)
        assert scan["status"] == "completed"
        assert scan["total_files"] > 0

        # Check state counts
        counts = db.get_state_counts(scan_id)
        assert counts.get("ingested", 0) == 1  # CASIZ_12345.jpg
        # CASIZ_55555 is in no_key_folder → missing_key_csv, not pending
        # pending = CASIZ_67890.jpg + CASIZ_11111.jpg = 2
        assert counts.get("pending", 0) == 2
        assert counts.get("no_casiz_match", 0) == 1  # landscape.jpg
        # forbidden_extension includes photo.CR2 + 2 key.csv files = 3
        assert counts.get("forbidden_extension", 0) == 3
        assert counts.get("dot_prefixed", 0) == 1  # .hidden.jpg
        assert counts.get("removed", 0) == 1  # CASIZ_99999.jpg
        assert counts.get("missing_key_csv", 0) == 1  # CASIZ_55555.jpg
        assert counts.get("skipping_crrf", 0) == 1  # crrf image.jpg

    def test_no_specimen_record_state(self, settings, db, scan_tree):
        """Files whose CASIZ numbers have no specimen record get no_specimen_record."""
        mock_filenames = set()
        # Only specimen 12345 exists — 67890 and 11111 have no specimen
        mock_specimens = {"12345"}

        with patch(
            "app.specify_client.fetch_ingested_filenames", return_value=mock_filenames
        ), patch(
            "app.specify_client.fetch_specimen_catalog_numbers",
            return_value=mock_specimens,
        ):
            scan_id = run_scan(settings, db)

        counts = db.get_state_counts(scan_id)
        # CASIZ_12345.jpg: has specimen, not ingested → pending
        assert counts.get("pending", 0) == 1
        # CASIZ_67890.jpg, CASIZ_11111.jpg: no specimen → no_specimen_record
        assert counts.get("no_specimen_record", 0) == 2

    def test_directory_counts_populated(self, settings, db, scan_tree):
        """After scan, directory_counts should have aggregated data."""
        mock_filenames = set()
        mock_specimens = {"12345", "67890", "11111"}
        with patch(
            "app.specify_client.fetch_ingested_filenames", return_value=mock_filenames
        ), patch(
            "app.specify_client.fetch_specimen_catalog_numbers",
            return_value=mock_specimens,
        ):
            scan_id = run_scan(settings, db)

        photo_a_dir = str(scan_tree / "photographer_a")
        counts = db.get_directory_counts(scan_id, photo_a_dir)
        assert counts is not None
        assert counts["total_files"] >= 3  # At least the 3 files directly in photo_a

    def test_key_csvs_recorded(self, settings, db, scan_tree):
        """After scan, key_csvs table should have the key.csv files found."""
        mock_filenames = set()
        mock_specimens = set()
        with patch(
            "app.specify_client.fetch_ingested_filenames", return_value=mock_filenames
        ), patch(
            "app.specify_client.fetch_specimen_catalog_numbers",
            return_value=mock_specimens,
        ):
            scan_id = run_scan(settings, db)

        key_csvs = db.get_key_csvs(scan_id)
        assert len(key_csvs) >= 2  # photo_a and photo_b

    def test_scan_with_specify_failure(self, settings, db, scan_tree):
        """Scan should continue even if Specify connection fails."""
        with patch(
            "app.specify_client.fetch_ingested_filenames",
            side_effect=Exception("Connection refused"),
        ), patch(
            "app.specify_client.fetch_specimen_catalog_numbers",
            side_effect=Exception("Connection refused"),
        ):
            scan_id = run_scan(settings, db)

        scan = db.get_scan(scan_id)
        assert scan["status"] == "completed"
        # Without Specify data, nothing is ingested — all CASIZ files are pending
        # specimen_catalog_numbers is None when query fails, so no_specimen check skipped
        counts = db.get_state_counts(scan_id)
        assert counts.get("ingested", 0) == 0
