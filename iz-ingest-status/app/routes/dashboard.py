"""
Dashboard route — GET /

Shows scan summary: state counts, bar chart, recent scans.

Inputs: Database with scan results.
Outputs: Rendered dashboard.html template.
"""

import os
from datetime import datetime

from croniter import croniter
from fastapi import APIRouter, Request

from app.specify_client import get_attachment_count

router = APIRouter()

# IZ import cron schedule from /etc/crontab:
# 0 1 */2 * *  — runs at 1:00 AM Pacific every 2 days
IZ_IMPORT_CRON = "0 1 */2 * *"

LOCKFILE = "/host_tmp/iz_import.lock"


@router.get("/")
async def dashboard(request: Request):
    """
    Render the main dashboard page.

    Inputs: request (FastAPI Request with app state).
    Outputs: TemplateResponse with scan summary data.

    Data: Loads latest completed scan, its state counts, recent scan list,
    and total Specify attachment count.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    running_scan = db.get_running_scan()
    recent_scans = db.get_recent_scans(limit=10)

    counts = {}
    total = 0
    specify_count = None
    if scan:
        counts = db.get_state_counts(scan["scan_id"])
        total = sum(counts.values())
        specify_count = get_attachment_count(settings)

    # Compute next IZ import run from cron schedule
    now = datetime.now()
    cron = croniter(IZ_IMPORT_CRON, now)
    next_import = cron.get_next(datetime)
    next_import_str = next_import.strftime("%a %b %d, %I:%M %p")

    # Check if the IZ import process is currently running (lockfile exists)
    import_running = os.path.exists(LOCKFILE)
    import_started_at = None
    if import_running:
        try:
            mtime = os.path.getmtime(LOCKFILE)
            started = datetime.fromtimestamp(mtime)
            elapsed = now - started
            hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
            minutes = remainder // 60
            import_started_at = started.strftime("%a %b %d, %I:%M %p")
            import_elapsed = f"{hours}h {minutes}m" if hours else f"{minutes}m"
        except OSError:
            import_started_at = None
            import_elapsed = None
    else:
        import_elapsed = None

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "active_page": "dashboard",
            "scan": scan,
            "running_scan": running_scan,
            "counts": counts,
            "total": total,
            "specify_count": specify_count,
            "recent_scans": recent_scans,
            "next_import": next_import_str,
            "import_running": import_running,
            "import_started_at": import_started_at,
            "import_elapsed": import_elapsed,
        },
    )
