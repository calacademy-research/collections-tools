"""
Scan trigger and status routes — POST /scan/trigger, GET /scan/status

Starts background scans and provides progress polling for the UI.

Inputs: Database, Settings, scanner module.
Outputs: HTML fragments for htmx polling.
"""

import threading

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.scanner import get_current_progress, run_scan

router = APIRouter()


@router.post("/scan/trigger")
async def scan_trigger(request: Request):
    """
    Start a background scan.

    Inputs: request with app state (settings, db).
    Outputs: HTML fragment showing scan has started, with polling trigger.

    Launches the scan in a background thread so the UI remains responsive.
    Returns immediately with a status element that polls /scan/status.
    """
    db = request.app.state.db
    settings = request.app.state.settings

    # Don't start a new scan if one is already running
    if db.get_running_scan():
        return HTMLResponse(
            '<span style="color:#856404; font-size:0.85rem;">Scan already running.</span>'
        )

    # Start scan in background thread
    def _run():
        try:
            run_scan(settings, db)
        except Exception:
            pass  # Error is logged and stored in scan record

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return HTMLResponse(
        '<div hx-get="/scan/status" hx-trigger="every 2s" hx-swap="innerHTML">'
        '<span class="spinner"></span> '
        '<span style="font-size:0.85rem;">Starting scan...</span>'
        '</div>'
    )


@router.get("/scan/status")
async def scan_status(request: Request):
    """
    htmx endpoint: poll scan progress.

    Inputs: request with app state.
    Outputs: HTML fragment with current scan progress or completion status.

    Called by htmx every 2 seconds while a scan is running.
    """
    db = request.app.state.db
    templates = request.app.state.templates

    progress = get_current_progress()
    scan = db.get_running_scan()

    # If no running scan, check for the most recent one
    if not scan:
        scan = db.get_latest_scan()

    html = templates.get_template("scan_status.html").render(
        progress=progress.to_dict() if progress else None,
        scan=scan,
    )
    return HTMLResponse(html)


@router.delete("/scan/{scan_id}")
async def scan_delete(request: Request, scan_id: int):
    """
    Delete a specific scan and all its associated data.

    Inputs: scan_id — the scan to delete.
    Outputs: HTML fragment confirming deletion.

    Refuses to delete a currently running scan.
    """
    db = request.app.state.db

    scan = db.get_scan(scan_id)
    if not scan:
        return HTMLResponse(
            '<span style="color:#dc3545; font-size:0.85rem;">Scan not found.</span>'
        )

    if scan["status"] == "running":
        return HTMLResponse(
            '<span style="color:#856404; font-size:0.85rem;">Cannot delete a running scan.</span>'
        )

    # Delete associated data then the scan record
    db.conn.execute("DELETE FROM file_results WHERE scan_id=?", (scan_id,))
    db.conn.execute("DELETE FROM directory_counts WHERE scan_id=?", (scan_id,))
    db.conn.execute("DELETE FROM key_csvs WHERE scan_id=?", (scan_id,))
    db.conn.execute("DELETE FROM scans WHERE scan_id=?", (scan_id,))
    db.conn.commit()

    return HTMLResponse(
        f'<span style="color:#28a745; font-size:0.85rem;">Scan #{scan_id} deleted.</span>'
    )
