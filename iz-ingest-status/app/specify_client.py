"""
Read-only Specify database client.

Connects to the Specify MySQL database on ntobiko and bulk-loads all
attachment origFilename values for cross-referencing during scans.

Inputs: Database credentials from config.
Outputs: Set of lowercased origFilename strings.

THIS TOOL NEVER WRITES TO SPECIFY. Read-only access only.
"""

import logging
from typing import Optional

import mysql.connector

from app.config import Settings

logger = logging.getLogger(__name__)


def fetch_ingested_filenames(settings: Settings) -> set[str]:
    """
    Bulk query all origFilename values from the Specify attachment table.

    Inputs: settings (Settings) — database credentials.
    Outputs: Set of lowercased origFilename strings (~98K entries).

    Runs a single query: SELECT origFilename FROM attachment
    Loads all results into memory as a set for O(1) lookups.
    origFilename has zero duplicates in the current database.

    Data transformation: Each filename is lowercased and only the basename
    (last path segment) is kept, matching how the importer checks for
    already-processed files.
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=settings.specify_db_host,
            port=settings.specify_db_port,
            database=settings.specify_db_name,
            user=settings.specify_db_user,
            password=settings.iz_specify_db_password,
            connect_timeout=30,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT origFilename FROM attachment")

        filenames = set()
        for (orig_filename,) in cursor:
            if orig_filename:
                # The importer stores full lowercased paths in origFilename.
                # For matching, we use the basename portion lowercased.
                import os
                basename = os.path.basename(orig_filename).lower()
                if basename:
                    filenames.add(basename)

        logger.info(f"Loaded {len(filenames)} ingested filenames from Specify")
        return filenames

    except mysql.connector.Error as e:
        logger.error(f"Failed to connect to Specify database: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()


def fetch_specimen_catalog_numbers(settings: Settings) -> set[str]:
    """
    Bulk query all catalogNumber values from the Specify collectionobject table.

    Inputs: settings (Settings) — database credentials.
    Outputs: Set of catalogNumber strings (~X entries).

    The importer checks collectionobject.catalognumber to verify a specimen
    exists before attaching an image. Files whose CASIZ numbers don't appear
    here will never be ingested — they should be flagged as no_specimen_record
    rather than pending.
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=settings.specify_db_host,
            port=settings.specify_db_port,
            database=settings.specify_db_name,
            user=settings.specify_db_user,
            password=settings.iz_specify_db_password,
            connect_timeout=30,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CatalogNumber FROM collectionobject")

        catalog_numbers = set()
        for (cat_num,) in cursor:
            if cat_num:
                # Strip leading zeros to match how we store extracted CASIZ ints
                stripped = cat_num.lstrip("0") or "0"
                catalog_numbers.add(stripped)

        logger.info(
            f"Loaded {len(catalog_numbers)} specimen catalog numbers from Specify"
        )
        return catalog_numbers

    except mysql.connector.Error as e:
        logger.error(f"Failed to query Specify collectionobject table: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()


def get_attachment_count(settings: Settings) -> Optional[int]:
    """
    Get total count of attachment records in Specify.

    Inputs: settings (Settings) — database credentials.
    Outputs: Integer count, or None on error.

    Used for dashboard display to show total Specify records.
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=settings.specify_db_host,
            port=settings.specify_db_port,
            database=settings.specify_db_name,
            user=settings.specify_db_user,
            password=settings.iz_specify_db_password,
            connect_timeout=30,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM attachment")
        (count,) = cursor.fetchone()
        return count

    except mysql.connector.Error as e:
        logger.error(f"Failed to query Specify: {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()
