"""
Problem reports route — GET /problems, POST /problems/review, POST /problems/rename

Shows files with problem states, filterable by state type.
Allows marking files as reviewed and renaming files on disk.

Inputs: Database with scan results, filter parameters.
Outputs: Rendered problems.html with paginated file list.
"""

import math
import os

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

# All file states with display labels — shown as tabs
ALL_STATES = [
    ("ingested", "Ingested"),
    ("pending", "Pending"),
    ("no_casiz_match", "No CASIZ Match"),
    ("no_specimen_record", "No Specimen Record"),
    ("missing_key_csv", "Missing key.csv"),
    ("forbidden_extension", "Forbidden Extension"),
    ("removed", "Removed"),
    ("dot_prefixed", "Dot-Prefixed"),
    ("skipping_crrf", "CRRF Skipped"),
]

PAGE_SIZE = 100


@router.get("/problems")
async def problems(
    request: Request, type: str = "no_casiz_match", page: int = 1, filter: str = ""
):
    """
    Render the problem files page with filter tabs, search, and pagination.

    Inputs:
        type: Which problem state to display.
        page: Page number (1-indexed).
        filter: Optional filename filter string.
    Outputs: TemplateResponse with filtered, paginated file list.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    files = []
    total_count = 0
    total_pages = 0
    counts = {}

    if scan:
        scan_id = scan["scan_id"]
        counts = db.get_state_counts(scan_id)

        if filter:
            # Filtered query: search within a specific state
            all_matches = db.conn.execute(
                "SELECT * FROM file_results WHERE scan_id=? AND state=? "
                "AND filename LIKE ? ORDER BY file_path LIMIT ? OFFSET ?",
                (scan_id, type, f"%{filter}%", PAGE_SIZE, (max(1, page) - 1) * PAGE_SIZE),
            ).fetchall()
            files = [dict(r) for r in all_matches]

            count_row = db.conn.execute(
                "SELECT COUNT(*) as cnt FROM file_results WHERE scan_id=? AND state=? "
                "AND filename LIKE ?",
                (scan_id, type, f"%{filter}%"),
            ).fetchone()
            total_count = count_row["cnt"]
        else:
            total_count = db.count_files_by_state(scan_id, type)
            offset = (max(1, page) - 1) * PAGE_SIZE
            files = db.get_files_by_state(scan_id, type, limit=PAGE_SIZE, offset=offset)

        total_pages = max(1, math.ceil(total_count / PAGE_SIZE))
        page = max(1, min(page, total_pages))

    # Find the label for the current state
    current_label = type
    for state, label in ALL_STATES:
        if state == type:
            current_label = label
            break

    return templates.TemplateResponse(
        "problems.html",
        {
            "request": request,
            "active_page": "problems",
            "scan": scan,
            "scan_root": settings.scan_root,
            "problem_states": ALL_STATES,
            "current_type": type,
            "current_label": current_label,
            "counts": counts,
            "files": files,
            "total_count": total_count,
            "page": page,
            "total_pages": total_pages,
            "filter_q": filter,
        },
    )


@router.post("/problems/review")
async def toggle_review(request: Request, file_id: int = Form(...)):
    """Toggle the reviewed flag on a file. Returns updated checkbox via htmx."""
    db = request.app.state.db
    new_val = db.toggle_reviewed(file_id)
    checked = "checked" if new_val else ""
    return HTMLResponse(
        f'<input type="checkbox" {checked} '
        f'hx-post="/problems/review" '
        f"hx-vals='{{\"file_id\": \"{file_id}\"}}' "
        f'hx-swap="outerHTML" '
        f'title="Mark as reviewed" '
        f'style="cursor:pointer; width:1.1rem; height:1.1rem;">'
    )


@router.post("/problems/rename")
async def rename_file(
    request: Request, file_id: int = Form(...), new_name: str = Form(...)
):
    """Rename a file on disk and update the database record."""
    db = request.app.state.db

    row = db.conn.execute(
        "SELECT file_path, directory, filename FROM file_results WHERE id=?",
        (file_id,),
    ).fetchone()
    if not row:
        return HTMLResponse(
            '<span style="color:#dc3545; font-size:0.8rem;">File not found in DB</span>'
        )

    old_path = row["file_path"]
    new_name = new_name.strip()
    new_path = os.path.join(row["directory"], new_name)

    # Validate
    if "/" in new_name or new_name.startswith("."):
        return HTMLResponse(
            '<span style="color:#dc3545; font-size:0.8rem;">Invalid filename</span>'
        )

    if not os.path.isfile(old_path):
        return HTMLResponse(
            f'<span style="color:#dc3545; font-size:0.8rem;">File not found on disk</span>'
        )

    if os.path.exists(new_path):
        return HTMLResponse(
            f'<span style="color:#dc3545; font-size:0.8rem;">Target already exists: {_escape(new_name)}</span>'
        )

    try:
        os.rename(old_path, new_path)
    except OSError as e:
        return HTMLResponse(
            f'<span style="color:#dc3545; font-size:0.8rem;">Rename failed: {_escape(str(e))}</span>'
        )

    db.rename_file(file_id, new_name)

    return HTMLResponse(
        f'<span style="color:#28a745; font-size:0.8rem;">Renamed to {_escape(new_name)}</span>'
    )


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
