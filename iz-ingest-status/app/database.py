"""
SQLite database layer.

Manages the scan cache: schema creation, scan lifecycle, file result storage,
directory count aggregation, and key.csv tracking.

Inputs: SQLite database path (from config).
Outputs: Query results as dicts/lists, scan IDs, aggregated counts.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LA_TZ = ZoneInfo("America/Los_Angeles")
from typing import Optional


# -- Schema --

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scans (
    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    total_files INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'  -- running, completed, failed
);

CREATE TABLE IF NOT EXISTS file_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL REFERENCES scans(scan_id),
    file_path TEXT NOT NULL,
    directory TEXT NOT NULL,
    filename TEXT NOT NULL,
    state TEXT NOT NULL,
    casiz_numbers TEXT,            -- JSON array of ints
    casiz_source TEXT,
    key_csv_path TEXT,
    has_remove_flag INTEGER DEFAULT 0,
    file_size INTEGER,
    file_mtime TEXT
);

CREATE TABLE IF NOT EXISTS key_csvs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    directory TEXT NOT NULL,
    parsed_data TEXT,              -- JSON
    file_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS directory_counts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL,
    directory TEXT NOT NULL,
    total_files INTEGER,
    ingested INTEGER,
    pending INTEGER,
    no_casiz INTEGER,
    forbidden_ext INTEGER,
    removed INTEGER,
    missing_key INTEGER,
    dot_prefixed INTEGER,
    crrf_skipped INTEGER,
    no_specimen INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_file_results_scan_state
    ON file_results(scan_id, state);
CREATE INDEX IF NOT EXISTS idx_file_results_scan_dir
    ON file_results(scan_id, directory);
CREATE INDEX IF NOT EXISTS idx_file_results_scan_path
    ON file_results(scan_id, file_path);
CREATE INDEX IF NOT EXISTS idx_file_results_scan_casiz
    ON file_results(scan_id, casiz_numbers);
CREATE INDEX IF NOT EXISTS idx_dir_counts_scan
    ON directory_counts(scan_id, directory);
CREATE INDEX IF NOT EXISTS idx_key_csvs_scan
    ON key_csvs(scan_id, directory);
"""


class Database:
    """
    SQLite database wrapper for scan cache operations.

    Inputs: db_path (str) — path to the SQLite file, or ':memory:' for tests.
    Outputs: Provides methods for all CRUD and query operations.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self):
        """Execute schema creation SQL, then run any needed migrations."""
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Run schema migrations for columns added after initial release."""
        # Add no_specimen column to directory_counts if missing
        cols = [
            row[1]
            for row in self.conn.execute("PRAGMA table_info(directory_counts)").fetchall()
        ]
        if "no_specimen" not in cols:
            self.conn.execute(
                "ALTER TABLE directory_counts ADD COLUMN no_specimen INTEGER DEFAULT 0"
            )
            self.conn.commit()

        # Add reviewed column to file_results if missing
        file_cols = [
            row[1]
            for row in self.conn.execute("PRAGMA table_info(file_results)").fetchall()
        ]
        if "reviewed" not in file_cols:
            self.conn.execute(
                "ALTER TABLE file_results ADD COLUMN reviewed INTEGER DEFAULT 0"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_results_reviewed "
                "ON file_results(scan_id, reviewed)"
            )
            self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.close()

    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transactions.

        Inputs: None.
        Outputs: Yields the connection cursor, commits on success, rolls back on error.
        """
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    # -- Scan operations --

    def create_scan(self) -> int:
        """
        Create a new scan record with status 'running'.

        Inputs: None.
        Outputs: The new scan_id (int).
        """
        now = datetime.now(LA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.execute(
            "INSERT INTO scans (started_at, status) VALUES (?, 'running')",
            (now,),
        )
        self.conn.commit()
        return cursor.lastrowid

    def finish_scan(self, scan_id: int, total_files: int, status: str = "completed"):
        """
        Mark a scan as finished.

        Inputs:
            scan_id: The scan to update.
            total_files: Total number of files processed.
            status: 'completed' or 'failed'.
        Outputs: None.
        """
        now = datetime.now(LA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            "UPDATE scans SET finished_at=?, total_files=?, status=? WHERE scan_id=?",
            (now, total_files, status, scan_id),
        )
        self.conn.commit()

    def get_scan(self, scan_id: int) -> Optional[dict]:
        """
        Get a single scan record.

        Inputs: scan_id (int).
        Outputs: Dict with scan fields, or None if not found.
        """
        row = self.conn.execute(
            "SELECT * FROM scans WHERE scan_id=?", (scan_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_latest_scan(self) -> Optional[dict]:
        """
        Get the most recent completed scan.

        Inputs: None.
        Outputs: Dict with scan fields, or None if no completed scans exist.
        """
        row = self.conn.execute(
            "SELECT * FROM scans WHERE status='completed' "
            "ORDER BY scan_id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def get_recent_scans(self, limit: int = 10) -> list[dict]:
        """
        Get recent scans ordered by most recent first.

        Inputs: limit (int) — max number of scans to return.
        Outputs: List of scan dicts.
        """
        rows = self.conn.execute(
            "SELECT * FROM scans ORDER BY scan_id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_running_scan(self) -> Optional[dict]:
        """
        Get the currently running scan, if any.

        Inputs: None.
        Outputs: Dict with scan fields, or None.
        """
        row = self.conn.execute(
            "SELECT * FROM scans WHERE status='running' ORDER BY scan_id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def purge_old_scans(self, keep: int = 10):
        """
        Delete old scans and their associated data, keeping only the most recent N.

        Inputs: keep (int) — number of most recent scans to retain.
        Outputs: None. Deletes rows from scans, file_results, directory_counts, key_csvs.
        """
        # Find scan IDs to delete (everything except the newest `keep`)
        old_scans = self.conn.execute(
            "SELECT scan_id FROM scans ORDER BY scan_id DESC LIMIT -1 OFFSET ?",
            (keep,),
        ).fetchall()

        if not old_scans:
            return

        old_ids = [row["scan_id"] for row in old_scans]
        placeholders = ",".join("?" * len(old_ids))

        self.conn.execute(
            f"DELETE FROM file_results WHERE scan_id IN ({placeholders})", old_ids
        )
        self.conn.execute(
            f"DELETE FROM directory_counts WHERE scan_id IN ({placeholders})", old_ids
        )
        self.conn.execute(
            f"DELETE FROM key_csvs WHERE scan_id IN ({placeholders})", old_ids
        )
        self.conn.execute(
            f"DELETE FROM scans WHERE scan_id IN ({placeholders})", old_ids
        )
        self.conn.commit()

    # -- File result operations --

    def insert_file_results_batch(self, scan_id: int, results: list[dict]):
        """
        Batch insert file classification results.

        Inputs:
            scan_id: The scan these results belong to.
            results: List of dicts with keys matching file_results columns.
                     casiz_numbers should be a list of ints (will be JSON-encoded).
        Outputs: None.

        Inserts in a single transaction for performance. Called every 1000 files
        during scanning.
        """
        rows = []
        for r in results:
            casiz_json = json.dumps(r.get("casiz_numbers", []))
            rows.append((
                scan_id,
                r["file_path"],
                r["directory"],
                r["filename"],
                r["state"],
                casiz_json,
                r.get("casiz_source"),
                r.get("key_csv_path"),
                1 if r.get("has_remove_flag") else 0,
                r.get("file_size"),
                r.get("file_mtime"),
            ))

        self.conn.executemany(
            "INSERT INTO file_results "
            "(scan_id, file_path, directory, filename, state, casiz_numbers, "
            "casiz_source, key_csv_path, has_remove_flag, file_size, file_mtime) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def get_state_counts(self, scan_id: int) -> dict[str, int]:
        """
        Get counts of each file state for a scan.

        Inputs: scan_id (int).
        Outputs: Dict mapping state name -> count (e.g. {'ingested': 64000, ...}).
        """
        rows = self.conn.execute(
            "SELECT state, COUNT(*) as cnt FROM file_results "
            "WHERE scan_id=? GROUP BY state",
            (scan_id,),
        ).fetchall()
        return {row["state"]: row["cnt"] for row in rows}

    def get_files_by_state(
        self, scan_id: int, state: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """
        Get files with a specific state, paginated.

        Inputs: scan_id, state name, limit, offset.
        Outputs: List of file result dicts.
        """
        rows = self.conn.execute(
            "SELECT * FROM file_results WHERE scan_id=? AND state=? "
            "ORDER BY file_path LIMIT ? OFFSET ?",
            (scan_id, state, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_files_by_state(self, scan_id: int, state: str) -> int:
        """
        Count files with a specific state.

        Inputs: scan_id, state name.
        Outputs: Integer count.
        """
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM file_results WHERE scan_id=? AND state=?",
            (scan_id, state),
        ).fetchone()
        return row["cnt"]

    def search_files(
        self, scan_id: int, query: str, search_type: str = "filename",
        limit: int = 100, offset: int = 0,
    ) -> list[dict]:
        """
        Search files by filename or CASIZ number.

        Inputs:
            scan_id: Scan to search within.
            query: Search string.
            search_type: 'filename' or 'casiz'.
            limit, offset: Pagination.
        Outputs: List of matching file result dicts.

        For 'casiz' searches, looks in the JSON casiz_numbers field.
        For 'filename' searches, uses LIKE on the filename column.
        """
        if search_type == "casiz":
            # Search for the number within the JSON array string
            rows = self.conn.execute(
                "SELECT * FROM file_results WHERE scan_id=? "
                "AND casiz_numbers LIKE ? ORDER BY file_path LIMIT ? OFFSET ?",
                (scan_id, f"%{query}%", limit, offset),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM file_results WHERE scan_id=? "
                "AND filename LIKE ? ORDER BY file_path LIMIT ? OFFSET ?",
                (scan_id, f"%{query}%", limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_directory_files(
        self, scan_id: int, directory: str
    ) -> list[dict]:
        """
        Get all files in a specific directory (not recursive).

        Inputs: scan_id, directory path.
        Outputs: List of file result dicts for that directory.
        """
        rows = self.conn.execute(
            "SELECT * FROM file_results WHERE scan_id=? AND directory=? "
            "ORDER BY filename",
            (scan_id, directory),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_child_directories(self, scan_id: int, parent_dir: str) -> list[str]:
        """
        Get immediate child directories of a given parent directory.

        Inputs: scan_id, parent_dir path.
        Outputs: Sorted list of unique child directory paths.

        Uses string matching on the directory column: children are directories
        that start with parent_dir + '/' and have no further '/' after that.
        """
        prefix = parent_dir.rstrip("/") + "/"
        rows = self.conn.execute(
            "SELECT DISTINCT directory FROM file_results "
            "WHERE scan_id=? AND directory LIKE ? AND directory != ?",
            (scan_id, prefix + "%", parent_dir),
        ).fetchall()

        # Filter to immediate children only
        children = set()
        for row in rows:
            d = row["directory"]
            # Remove prefix, split on /, take first segment
            remainder = d[len(prefix):]
            first_segment = remainder.split("/")[0]
            children.add(prefix + first_segment)

        return sorted(children)

    # -- Directory counts --

    def aggregate_directory_counts(self, scan_id: int):
        """
        Compute and store per-directory state counts from file_results.

        Inputs: scan_id (int).
        Outputs: None. Populates the directory_counts table.

        Runs after a scan completes. Groups file_results by directory and
        counts each state.
        """
        # State name -> column name mapping
        state_cols = {
            "ingested": "ingested",
            "pending": "pending",
            "no_casiz_match": "no_casiz",
            "forbidden_extension": "forbidden_ext",
            "removed": "removed",
            "missing_key_csv": "missing_key",
            "dot_prefixed": "dot_prefixed",
            "skipping_crrf": "crrf_skipped",
            "no_specimen_record": "no_specimen",
        }

        rows = self.conn.execute(
            "SELECT directory, state, COUNT(*) as cnt FROM file_results "
            "WHERE scan_id=? GROUP BY directory, state",
            (scan_id,),
        ).fetchall()

        # Build per-directory aggregation
        dir_data: dict[str, dict[str, int]] = {}
        for row in rows:
            d = row["directory"]
            if d not in dir_data:
                dir_data[d] = {col: 0 for col in state_cols.values()}
                dir_data[d]["total_files"] = 0
            col = state_cols.get(row["state"])
            if col:
                dir_data[d][col] = row["cnt"]
            dir_data[d]["total_files"] += row["cnt"]

        # Insert
        insert_rows = []
        for directory, counts in dir_data.items():
            insert_rows.append((
                scan_id,
                directory,
                counts["total_files"],
                counts["ingested"],
                counts["pending"],
                counts["no_casiz"],
                counts["forbidden_ext"],
                counts["removed"],
                counts["missing_key"],
                counts["dot_prefixed"],
                counts["crrf_skipped"],
                counts["no_specimen"],
            ))

        self.conn.executemany(
            "INSERT INTO directory_counts "
            "(scan_id, directory, total_files, ingested, pending, no_casiz, "
            "forbidden_ext, removed, missing_key, dot_prefixed, crrf_skipped, "
            "no_specimen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            insert_rows,
        )
        self.conn.commit()

    def get_directory_counts(self, scan_id: int, directory: str) -> Optional[dict]:
        """
        Get state counts for a specific directory.

        Inputs: scan_id, directory path.
        Outputs: Dict of counts, or None if not found.
        """
        row = self.conn.execute(
            "SELECT * FROM directory_counts WHERE scan_id=? AND directory=?",
            (scan_id, directory),
        ).fetchone()
        return dict(row) if row else None

    def get_all_directory_counts(self, scan_id: int) -> list[dict]:
        """
        Get state counts for all directories in a scan.

        Inputs: scan_id.
        Outputs: List of directory count dicts.
        """
        rows = self.conn.execute(
            "SELECT * FROM directory_counts WHERE scan_id=? ORDER BY directory",
            (scan_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_subtree_counts(self, scan_id: int, directory: str) -> dict:
        """
        Aggregate state counts for a directory and all its subdirectories using SQL.

        Inputs: scan_id, directory path.
        Outputs: Dict with summed counts across the entire subtree.

        Uses a single SQL query with SUM() instead of loading all rows into Python.
        """
        prefix = directory.rstrip("/") + "/"
        row = self.conn.execute(
            "SELECT "
            "  COALESCE(SUM(total_files), 0) as total_files, "
            "  COALESCE(SUM(ingested), 0) as ingested, "
            "  COALESCE(SUM(pending), 0) as pending, "
            "  COALESCE(SUM(no_casiz), 0) as no_casiz, "
            "  COALESCE(SUM(forbidden_ext), 0) as forbidden_ext, "
            "  COALESCE(SUM(removed), 0) as removed, "
            "  COALESCE(SUM(missing_key), 0) as missing_key, "
            "  COALESCE(SUM(dot_prefixed), 0) as dot_prefixed, "
            "  COALESCE(SUM(crrf_skipped), 0) as crrf_skipped, "
            "  COALESCE(SUM(no_specimen), 0) as no_specimen "
            "FROM directory_counts "
            "WHERE scan_id=? AND (directory=? OR directory LIKE ?)",
            (scan_id, directory, prefix + "%"),
        ).fetchone()
        return dict(row) if row else {
            "total_files": 0, "ingested": 0, "pending": 0,
            "no_casiz": 0, "forbidden_ext": 0, "removed": 0,
            "missing_key": 0, "dot_prefixed": 0, "crrf_skipped": 0,
            "no_specimen": 0,
        }

    # -- Key CSV tracking --

    def insert_key_csvs(self, scan_id: int, key_csvs: list[dict]):
        """
        Store key.csv metadata for a scan.

        Inputs:
            scan_id: Scan ID.
            key_csvs: List of dicts with 'file_path', 'directory', 'parsed_data',
                      'file_count' keys.
        Outputs: None.
        """
        rows = []
        for kc in key_csvs:
            parsed_json = json.dumps(kc.get("parsed_data", {}))
            rows.append((
                scan_id,
                kc["file_path"],
                kc["directory"],
                parsed_json,
                kc.get("file_count", 0),
            ))

        self.conn.executemany(
            "INSERT INTO key_csvs (scan_id, file_path, directory, parsed_data, file_count) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def get_key_csvs(self, scan_id: int) -> list[dict]:
        """
        Get all key.csv records for a scan.

        Inputs: scan_id.
        Outputs: List of key.csv dicts with parsed_data as a Python dict.
        """
        rows = self.conn.execute(
            "SELECT * FROM key_csvs WHERE scan_id=? ORDER BY file_path",
            (scan_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["parsed_data"] = json.loads(d["parsed_data"]) if d["parsed_data"] else {}
            result.append(d)
        return result

    # -- Diff --


    def toggle_reviewed(self, file_id: int) -> int:
        """Toggle the reviewed flag on a file_results row. Returns new value."""
        row = self.conn.execute(
            "SELECT reviewed FROM file_results WHERE id=?", (file_id,)
        ).fetchone()
        if not row:
            return 0
        new_val = 0 if row["reviewed"] else 1
        self.conn.execute(
            "UPDATE file_results SET reviewed=? WHERE id=?", (new_val, file_id)
        )
        self.conn.commit()
        return new_val

    def rename_file(self, file_id: int, new_filename: str):
        """Update filename and file_path for a file_results row."""
        row = self.conn.execute(
            "SELECT file_path, directory FROM file_results WHERE id=?", (file_id,)
        ).fetchone()
        if not row:
            return None
        new_path = row["directory"] + "/" + new_filename
        self.conn.execute(
            "UPDATE file_results SET filename=?, file_path=? WHERE id=?",
            (new_filename, new_path, file_id),
        )
        self.conn.commit()
        return dict(row)

    def get_diff(
        self, old_scan_id: int, new_scan_id: int, limit: int = 200
    ) -> dict:
        """
        Compare two scans using SQL JOINs instead of loading all rows into Python.

        Inputs: old_scan_id, new_scan_id, limit (max rows per section).
        Outputs: Dict with keys:
            'new_files_count': total new files
            'removed_files_count': total removed files
            'changed_count': total files that changed state
            'new_files': first N new file paths
            'removed_files': first N removed file paths
            'changed_state': first N changed-state records
            'old_counts': state counts for old scan
            'new_counts': state counts for new scan
        """
        # Files in new but not old
        new_file_count = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM file_results n "
            "WHERE n.scan_id=? AND NOT EXISTS ("
            "  SELECT 1 FROM file_results o WHERE o.scan_id=? AND o.file_path=n.file_path"
            ")",
            (new_scan_id, old_scan_id),
        ).fetchone()["cnt"]

        new_files = [
            row["file_path"]
            for row in self.conn.execute(
                "SELECT n.file_path FROM file_results n "
                "WHERE n.scan_id=? AND NOT EXISTS ("
                "  SELECT 1 FROM file_results o WHERE o.scan_id=? AND o.file_path=n.file_path"
                ") ORDER BY n.file_path LIMIT ?",
                (new_scan_id, old_scan_id, limit),
            ).fetchall()
        ]

        # Files in old but not new
        removed_file_count = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM file_results o "
            "WHERE o.scan_id=? AND NOT EXISTS ("
            "  SELECT 1 FROM file_results n WHERE n.scan_id=? AND n.file_path=o.file_path"
            ")",
            (old_scan_id, new_scan_id),
        ).fetchone()["cnt"]

        removed_files = [
            row["file_path"]
            for row in self.conn.execute(
                "SELECT o.file_path FROM file_results o "
                "WHERE o.scan_id=? AND NOT EXISTS ("
                "  SELECT 1 FROM file_results n WHERE n.scan_id=? AND n.file_path=o.file_path"
                ") ORDER BY o.file_path LIMIT ?",
                (old_scan_id, new_scan_id, limit),
            ).fetchall()
        ]

        # Files present in both but with different states
        changed_count = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM file_results o "
            "JOIN file_results n ON o.file_path=n.file_path "
            "WHERE o.scan_id=? AND n.scan_id=? AND o.state!=n.state",
            (old_scan_id, new_scan_id),
        ).fetchone()["cnt"]

        changed_state = [
            {
                "file_path": row["file_path"],
                "old_state": row["old_state"],
                "new_state": row["new_state"],
            }
            for row in self.conn.execute(
                "SELECT o.file_path, o.state as old_state, n.state as new_state "
                "FROM file_results o "
                "JOIN file_results n ON o.file_path=n.file_path "
                "WHERE o.scan_id=? AND n.scan_id=? AND o.state!=n.state "
                "ORDER BY o.file_path LIMIT ?",
                (old_scan_id, new_scan_id, limit),
            ).fetchall()
        ]

        return {
            "new_files": new_files,
            "removed_files": removed_files,
            "changed_state": changed_state,
            "new_files_count": new_file_count,
            "removed_files_count": removed_file_count,
            "changed_count": changed_count,
            "old_counts": self.get_state_counts(old_scan_id),
            "new_counts": self.get_state_counts(new_scan_id),
        }
