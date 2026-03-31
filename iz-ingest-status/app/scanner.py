"""
Filesystem scanner and orchestrator.

Walks the scan folder, classifies every file, and stores results in SQLite.
This is the main "scan" operation that ties together all the other modules.

Inputs: Scan root path, Settings, Database instance.
Outputs: Populates SQLite with file_results, directory_counts, key_csvs.

Expected duration: 30-90 seconds (NFS walk + one MySQL query, no EXIF reads).
"""

import logging
import os
import threading
from dataclasses import asdict
from typing import Optional

from app.classify import classify_file
from app.config import Settings
from app.database import Database
from app.key_csv import build_key_csv_cache, parse_key_csv

logger = logging.getLogger(__name__)

# Batch size for SQLite inserts — every N files, flush to disk
BATCH_SIZE = 1000


class ScanProgress:
    """
    Thread-safe scan progress tracker for UI polling.

    Fields:
        scan_id: The current scan's ID.
        total_files: Running count of files processed so far.
        status: 'running', 'completed', or 'failed'.
        current_directory: The directory currently being scanned.
        error: Error message if scan failed.
    """

    def __init__(self, scan_id: int):
        self.scan_id = scan_id
        self.total_files = 0
        self.status = "running"
        self.current_directory = ""
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    def update(self, files_processed: int, current_dir: str):
        with self._lock:
            self.total_files = files_processed
            self.current_directory = current_dir

    def finish(self, status: str, error: Optional[str] = None):
        with self._lock:
            self.status = status
            self.error = error

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "scan_id": self.scan_id,
                "total_files": self.total_files,
                "status": self.status,
                "current_directory": self.current_directory,
                "error": self.error,
            }


# Global progress tracker, set during active scans
_current_progress: Optional[ScanProgress] = None


def get_current_progress() -> Optional[ScanProgress]:
    """Return the current scan progress, or None if no scan is running."""
    return _current_progress


def run_scan(settings: Settings, db: Database) -> int:
    """
    Execute a full scan of the IZ image folder.

    Inputs:
        settings: App configuration with scan_root and DB credentials.
        db: Database instance for storing results.
    Outputs: scan_id (int) of the completed scan.

    Steps:
        1. Create scan record in SQLite.
        2. Bulk query Specify for ingested filenames (~98K).
        3. Walk the scan folder with os.walk() (~133K files, ~3.6K dirs).
        4. Build key.csv cache for all encountered directories.
        5. Classify each file → ClassificationResult.
        6. Batch insert into SQLite every BATCH_SIZE files.
        7. Aggregate directory_counts.
        8. Record key.csv files found.
        9. Mark scan as completed.
    """
    global _current_progress

    scan_id = db.create_scan()
    progress = ScanProgress(scan_id)
    _current_progress = progress

    try:
        # Step 2: Load ingested filenames and specimen catalog numbers from Specify
        logger.info("Loading ingested filenames from Specify...")
        from app.specify_client import (
            fetch_ingested_filenames,
            fetch_specimen_catalog_numbers,
        )

        try:
            ingested_filenames = fetch_ingested_filenames(settings)
        except Exception as e:
            logger.error(f"Failed to load Specify attachment data: {e}")
            ingested_filenames = set()

        logger.info("Loading specimen catalog numbers from Specify...")
        try:
            specimen_catalog_numbers = fetch_specimen_catalog_numbers(settings)
        except Exception as e:
            logger.error(f"Failed to load Specify specimen data: {e}")
            specimen_catalog_numbers = None  # None = skip the check

        # Step 3: Walk filesystem to discover all directories first
        logger.info(f"Walking scan folder: {settings.scan_root}")
        all_directories: set[str] = set()
        all_files: list[tuple[str, str]] = []  # (dirpath, filename)

        for dirpath, dirnames, filenames in os.walk(settings.scan_root):
            all_directories.add(dirpath)
            for filename in filenames:
                all_files.append((dirpath, filename))

        logger.info(
            f"Found {len(all_files)} files in {len(all_directories)} directories"
        )

        # Step 4: Build key.csv cache
        logger.info("Building key.csv cache...")
        key_csv_cache = build_key_csv_cache(settings.scan_root, all_directories)

        # Step 5-6: Classify and batch insert
        batch: list[dict] = []
        total_processed = 0
        key_csv_paths_seen: set[str] = set()

        for dirpath, filename in all_files:
            file_path = os.path.join(dirpath, filename)

            result = classify_file(
                file_path, ingested_filenames, key_csv_cache, specimen_catalog_numbers
            )

            # Track key.csv paths for later recording
            if result.key_csv_path:
                key_csv_paths_seen.add(result.key_csv_path)

            batch.append({
                "file_path": result.file_path,
                "directory": os.path.dirname(result.file_path),
                "filename": os.path.basename(result.file_path),
                "state": result.state,
                "casiz_numbers": result.casiz_numbers,
                "casiz_source": result.casiz_source,
                "key_csv_path": result.key_csv_path,
                "has_remove_flag": result.has_remove_flag,
                "file_size": result.file_size,
                "file_mtime": result.file_mtime,
            })

            total_processed += 1

            if len(batch) >= BATCH_SIZE:
                db.insert_file_results_batch(scan_id, batch)
                progress.update(total_processed, dirpath)
                batch = []

        # Flush remaining batch
        if batch:
            db.insert_file_results_batch(scan_id, batch)
            progress.update(total_processed, "")

        # Step 7: Aggregate directory counts
        logger.info("Aggregating directory counts...")
        db.aggregate_directory_counts(scan_id)

        # Step 8: Record key.csv files
        logger.info(f"Recording {len(key_csv_paths_seen)} key.csv files...")
        key_csv_records = []
        for key_path in key_csv_paths_seen:
            parsed = parse_key_csv(key_path)
            # Count how many files this key.csv governs
            key_dir = os.path.dirname(key_path)
            file_count = sum(
                1
                for d, info in key_csv_cache.items()
                if info and info.get("_path") == key_path
            )
            key_csv_records.append({
                "file_path": key_path,
                "directory": key_dir,
                "parsed_data": {
                    k: v
                    for k, v in parsed.items()
                    if not k.startswith("_")
                },
                "file_count": file_count,
            })

        if key_csv_records:
            db.insert_key_csvs(scan_id, key_csv_records)

        # Step 9: Finish
        db.finish_scan(scan_id, total_processed)
        progress.finish("completed")
        logger.info(
            f"Scan {scan_id} completed: {total_processed} files processed"
        )

        # Step 10: Purge old scans to keep DB size manageable
        db.purge_old_scans(keep=10)
        logger.info("Purged old scans (keeping last 10)")

        return scan_id

    except Exception as e:
        logger.error(f"Scan {scan_id} failed: {e}")
        db.finish_scan(scan_id, 0, "failed")
        progress.finish("failed", str(e))
        raise
    finally:
        _current_progress = None
