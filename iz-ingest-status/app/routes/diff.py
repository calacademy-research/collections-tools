"""
Diff route — GET /diff

Compare two scans to show what changed: new files, removed files,
and files that changed state.

Inputs: Database with scan results, old/new scan IDs.
Outputs: Rendered diff.html with comparison data.
"""

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/diff")
async def diff_page(request: Request, old: int = 0, new: int = 0):
    """
    Render the scan diff page.

    Inputs:
        old: Scan ID for the older scan.
        new: Scan ID for the newer scan.
    Outputs: TemplateResponse with diff data between two scans.
    """
    db = request.app.state.db
    templates = request.app.state.templates

    scans = db.get_recent_scans(limit=20)
    # Filter to completed scans only
    scans = [s for s in scans if s["status"] == "completed"]

    diff = None
    old_id = old
    new_id = new

    # Default to two most recent scans
    if len(scans) >= 2 and not old and not new:
        new_id = scans[0]["scan_id"]
        old_id = scans[1]["scan_id"]

    if old_id and new_id and old_id != new_id:
        diff = db.get_diff(old_id, new_id)

    return templates.TemplateResponse(
        "diff.html",
        {
            "request": request,
            "active_page": "diff",
            "scans": scans,
            "old_id": old_id,
            "new_id": new_id,
            "diff": diff,
        },
    )
