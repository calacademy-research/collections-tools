"""
Unit tests for app/database.py — SQLite schema and query operations.

All tests use in-memory SQLite (:memory:) for isolation and speed.
"""

import json

import pytest

from app.database import Database


@pytest.fixture
def db():
    """Create a fresh in-memory database for each test."""
    database = Database(":memory:")
    yield database
    database.close()


# -- Schema creation --


class TestSchema:
    def test_tables_created(self, db):
        """All four tables should exist after init."""
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row["name"] for row in cursor.fetchall()]
        assert "scans" in tables
        assert "file_results" in tables
        assert "key_csvs" in tables
        assert "directory_counts" in tables

    def test_indexes_created(self, db):
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name LIKE 'idx_%' ORDER BY name"
        )
        indexes = [row["name"] for row in cursor.fetchall()]
        assert len(indexes) >= 6


# -- Scan operations --


class TestScans:
    def test_create_scan(self, db):
        scan_id = db.create_scan()
        assert scan_id == 1
        scan = db.get_scan(scan_id)
        assert scan["status"] == "running"
        assert scan["started_at"] is not None

    def test_finish_scan(self, db):
        scan_id = db.create_scan()
        db.finish_scan(scan_id, total_files=1000, status="completed")
        scan = db.get_scan(scan_id)
        assert scan["status"] == "completed"
        assert scan["total_files"] == 1000
        assert scan["finished_at"] is not None

    def test_get_latest_scan(self, db):
        id1 = db.create_scan()
        db.finish_scan(id1, 100, "completed")
        id2 = db.create_scan()
        db.finish_scan(id2, 200, "completed")

        latest = db.get_latest_scan()
        assert latest["scan_id"] == id2

    def test_get_latest_scan_skips_failed(self, db):
        id1 = db.create_scan()
        db.finish_scan(id1, 100, "completed")
        id2 = db.create_scan()
        db.finish_scan(id2, 0, "failed")

        latest = db.get_latest_scan()
        assert latest["scan_id"] == id1

    def test_get_recent_scans(self, db):
        for _ in range(5):
            sid = db.create_scan()
            db.finish_scan(sid, 100, "completed")

        recent = db.get_recent_scans(limit=3)
        assert len(recent) == 3
        # Most recent first
        assert recent[0]["scan_id"] > recent[1]["scan_id"]

    def test_get_running_scan(self, db):
        assert db.get_running_scan() is None
        scan_id = db.create_scan()
        running = db.get_running_scan()
        assert running["scan_id"] == scan_id

    def test_no_latest_scan(self, db):
        assert db.get_latest_scan() is None


# -- File results --


class TestFileResults:
    def _make_results(self, count=5, state="pending"):
        """Generate test file result dicts."""
        return [
            {
                "file_path": f"/scan/dir/file_{i}.jpg",
                "directory": "/scan/dir",
                "filename": f"file_{i}.jpg",
                "state": state,
                "casiz_numbers": [10000 + i],
                "casiz_source": "filename",
                "key_csv_path": "/scan/dir/key.csv",
                "has_remove_flag": False,
                "file_size": 1024 * (i + 1),
                "file_mtime": "2025-01-01T00:00:00Z",
            }
            for i in range(count)
        ]

    def test_insert_and_query(self, db):
        scan_id = db.create_scan()
        results = self._make_results(3)
        db.insert_file_results_batch(scan_id, results)

        files = db.get_directory_files(scan_id, "/scan/dir")
        assert len(files) == 3

    def test_state_counts(self, db):
        scan_id = db.create_scan()
        results = (
            self._make_results(3, "pending")
            + self._make_results(2, "ingested")
        )
        # Fix duplicate filenames
        for i, r in enumerate(results):
            r["file_path"] = f"/scan/dir/file_{i}.jpg"
            r["filename"] = f"file_{i}.jpg"

        db.insert_file_results_batch(scan_id, results)
        counts = db.get_state_counts(scan_id)
        assert counts["pending"] == 3
        assert counts["ingested"] == 2

    def test_get_files_by_state(self, db):
        scan_id = db.create_scan()
        results = self._make_results(5, "pending") + self._make_results(3, "ingested")
        for i, r in enumerate(results):
            r["file_path"] = f"/scan/dir/file_{i}.jpg"
            r["filename"] = f"file_{i}.jpg"

        db.insert_file_results_batch(scan_id, results)

        pending = db.get_files_by_state(scan_id, "pending")
        assert len(pending) == 5

        ingested = db.get_files_by_state(scan_id, "ingested")
        assert len(ingested) == 3

    def test_count_files_by_state(self, db):
        scan_id = db.create_scan()
        db.insert_file_results_batch(scan_id, self._make_results(7, "no_casiz_match"))
        assert db.count_files_by_state(scan_id, "no_casiz_match") == 7

    def test_pagination(self, db):
        scan_id = db.create_scan()
        results = self._make_results(10, "pending")
        for i, r in enumerate(results):
            r["file_path"] = f"/scan/dir/file_{i:02d}.jpg"
            r["filename"] = f"file_{i:02d}.jpg"
        db.insert_file_results_batch(scan_id, results)

        page1 = db.get_files_by_state(scan_id, "pending", limit=3, offset=0)
        page2 = db.get_files_by_state(scan_id, "pending", limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0]["filename"] != page2[0]["filename"]

    def test_search_by_filename(self, db):
        scan_id = db.create_scan()
        results = [
            {
                "file_path": "/scan/CASIZ_12345.jpg",
                "directory": "/scan",
                "filename": "CASIZ_12345.jpg",
                "state": "pending",
                "casiz_numbers": [12345],
                "casiz_source": "filename",
            },
            {
                "file_path": "/scan/landscape.jpg",
                "directory": "/scan",
                "filename": "landscape.jpg",
                "state": "no_casiz_match",
                "casiz_numbers": [],
            },
        ]
        db.insert_file_results_batch(scan_id, results)

        found = db.search_files(scan_id, "CASIZ", "filename")
        assert len(found) == 1
        assert found[0]["filename"] == "CASIZ_12345.jpg"

    def test_search_by_casiz(self, db):
        scan_id = db.create_scan()
        results = [
            {
                "file_path": "/scan/img.jpg",
                "directory": "/scan",
                "filename": "img.jpg",
                "state": "pending",
                "casiz_numbers": [12345, 67890],
            },
        ]
        db.insert_file_results_batch(scan_id, results)

        found = db.search_files(scan_id, "12345", "casiz")
        assert len(found) == 1

    def test_casiz_stored_as_json(self, db):
        scan_id = db.create_scan()
        results = [
            {
                "file_path": "/scan/img.jpg",
                "directory": "/scan",
                "filename": "img.jpg",
                "state": "pending",
                "casiz_numbers": [111, 222, 333],
            },
        ]
        db.insert_file_results_batch(scan_id, results)

        files = db.get_directory_files(scan_id, "/scan")
        assert json.loads(files[0]["casiz_numbers"]) == [111, 222, 333]


# -- Directory counts --


class TestDirectoryCounts:
    def test_aggregate(self, db):
        scan_id = db.create_scan()
        results = [
            {"file_path": "/a/f1.jpg", "directory": "/a", "filename": "f1.jpg",
             "state": "pending", "casiz_numbers": [1]},
            {"file_path": "/a/f2.jpg", "directory": "/a", "filename": "f2.jpg",
             "state": "pending", "casiz_numbers": [2]},
            {"file_path": "/a/f3.jpg", "directory": "/a", "filename": "f3.jpg",
             "state": "ingested", "casiz_numbers": [3]},
            {"file_path": "/b/f4.jpg", "directory": "/b", "filename": "f4.jpg",
             "state": "no_casiz_match", "casiz_numbers": []},
        ]
        db.insert_file_results_batch(scan_id, results)
        db.aggregate_directory_counts(scan_id)

        counts_a = db.get_directory_counts(scan_id, "/a")
        assert counts_a["total_files"] == 3
        assert counts_a["pending"] == 2
        assert counts_a["ingested"] == 1

        counts_b = db.get_directory_counts(scan_id, "/b")
        assert counts_b["total_files"] == 1
        assert counts_b["no_casiz"] == 1

    def test_get_all_directory_counts(self, db):
        scan_id = db.create_scan()
        results = [
            {"file_path": "/a/f1.jpg", "directory": "/a", "filename": "f1.jpg",
             "state": "pending", "casiz_numbers": []},
            {"file_path": "/b/f2.jpg", "directory": "/b", "filename": "f2.jpg",
             "state": "ingested", "casiz_numbers": []},
        ]
        db.insert_file_results_batch(scan_id, results)
        db.aggregate_directory_counts(scan_id)

        all_counts = db.get_all_directory_counts(scan_id)
        assert len(all_counts) == 2


# -- Child directories --


class TestChildDirectories:
    def test_get_children(self, db):
        scan_id = db.create_scan()
        results = [
            {"file_path": "/root/a/f1.jpg", "directory": "/root/a",
             "filename": "f1.jpg", "state": "pending", "casiz_numbers": []},
            {"file_path": "/root/b/f2.jpg", "directory": "/root/b",
             "filename": "f2.jpg", "state": "pending", "casiz_numbers": []},
            {"file_path": "/root/a/deep/f3.jpg", "directory": "/root/a/deep",
             "filename": "f3.jpg", "state": "pending", "casiz_numbers": []},
        ]
        db.insert_file_results_batch(scan_id, results)

        children = db.get_child_directories(scan_id, "/root")
        assert "/root/a" in children
        assert "/root/b" in children
        assert "/root/a/deep" not in children  # Not immediate child of /root


# -- Key CSV tracking --


class TestKeyCsvTracking:
    def test_insert_and_retrieve(self, db):
        scan_id = db.create_scan()
        key_csvs = [
            {
                "file_path": "/scan/photographer/key.csv",
                "directory": "/scan/photographer",
                "parsed_data": {"CopyrightHolder": "John", "remove": None},
                "file_count": 42,
            },
        ]
        db.insert_key_csvs(scan_id, key_csvs)

        retrieved = db.get_key_csvs(scan_id)
        assert len(retrieved) == 1
        assert retrieved[0]["parsed_data"]["CopyrightHolder"] == "John"
        assert retrieved[0]["file_count"] == 42


# -- Diff --


class TestDiff:
    def test_diff_detects_changes(self, db):
        # Scan 1
        id1 = db.create_scan()
        db.insert_file_results_batch(id1, [
            {"file_path": "/a.jpg", "directory": "/", "filename": "a.jpg",
             "state": "pending", "casiz_numbers": [1]},
            {"file_path": "/b.jpg", "directory": "/", "filename": "b.jpg",
             "state": "pending", "casiz_numbers": [2]},
            {"file_path": "/c.jpg", "directory": "/", "filename": "c.jpg",
             "state": "no_casiz_match", "casiz_numbers": []},
        ])
        db.finish_scan(id1, 3)

        # Scan 2 — b.jpg is now ingested, c.jpg is gone, d.jpg is new
        id2 = db.create_scan()
        db.insert_file_results_batch(id2, [
            {"file_path": "/a.jpg", "directory": "/", "filename": "a.jpg",
             "state": "pending", "casiz_numbers": [1]},
            {"file_path": "/b.jpg", "directory": "/", "filename": "b.jpg",
             "state": "ingested", "casiz_numbers": [2]},
            {"file_path": "/d.jpg", "directory": "/", "filename": "d.jpg",
             "state": "pending", "casiz_numbers": [4]},
        ])
        db.finish_scan(id2, 3)

        diff = db.get_diff(id1, id2)
        assert "/d.jpg" in diff["new_files"]
        assert "/c.jpg" in diff["removed_files"]
        assert diff["new_files_count"] == 1
        assert diff["removed_files_count"] == 1
        assert diff["changed_count"] == 1
        assert len(diff["changed_state"]) == 1
        assert diff["changed_state"][0]["file_path"] == "/b.jpg"
        assert diff["changed_state"][0]["old_state"] == "pending"
        assert diff["changed_state"][0]["new_state"] == "ingested"
