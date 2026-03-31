"""
Tree browser routes — GET /tree, GET /tree/expand

Provides a directory tree view with htmx-powered expansion.

Inputs: Database with scan results, directory path for expansion.
Outputs: Rendered tree.html or tree_node.html partials.
"""

import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


def _build_child_info(db, scan_id: int, directory: str) -> dict:
    """
    Build display info for a single child directory using SQL aggregation.

    Inputs: db, scan_id, directory path.
    Outputs: Dict with name, path, and aggregated state counts.
    """
    counts = db.get_subtree_counts(scan_id, directory)
    return {
        "path": directory,
        "name": os.path.basename(directory),
        "total": counts.get("total_files", 0),
        "ingested": counts.get("ingested", 0),
        "pending": counts.get("pending", 0),
        "no_casiz": counts.get("no_casiz", 0),
        "forbidden_ext": counts.get("forbidden_ext", 0),
        "removed": counts.get("removed", 0),
        "missing_key": counts.get("missing_key", 0),
        "no_specimen": counts.get("no_specimen", 0),
    }


@router.get("/tree")
async def tree_root(request: Request):
    """
    Render the top-level tree browser page.

    Inputs: request with app state.
    Outputs: TemplateResponse showing scan root's immediate children.

    Loads directory counts for each child of the scan root to display
    summary badges.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    children = []

    if scan:
        scan_id = scan["scan_id"]
        child_dirs = db.get_child_directories(scan_id, settings.scan_root)
        children = [_build_child_info(db, scan_id, d) for d in child_dirs]

    return templates.TemplateResponse(
        "tree.html",
        {
            "request": request,
            "active_page": "tree",
            "scan": scan,
            "scan_id": scan["scan_id"] if scan else None,
            "children": children,
        },
    )


@router.get("/tree/expand")
async def tree_expand(request: Request, path: str, scan_id: int):
    """
    htmx endpoint: expand a directory node in the tree.

    Inputs:
        path: Directory path to expand.
        scan_id: Which scan to show data from.
    Outputs: HTML fragment with child tree nodes.

    Returns tree_node.html for each immediate child directory,
    plus a file list for files directly in this directory.
    """
    db = request.app.state.db
    templates = request.app.state.templates

    child_dirs = db.get_child_directories(scan_id, path)
    children = [_build_child_info(db, scan_id, d) for d in child_dirs]

    # Files directly in this directory
    files = db.get_directory_files(scan_id, path)

    # Build HTML response
    html_parts = []
    for child in children:
        html_parts.append(
            templates.get_template("tree_node.html").render(
                child=child, scan_id=scan_id
            )
        )

    # File list
    if files:
        html_parts.append('<table style="font-size:0.8rem; margin-top:0.5rem;">')
        for f in files:
            state = f["state"]
            html_parts.append(
                f'<tr>'
                f'<td class="mono" style="padding:0.15rem 0.5rem;">{f["filename"]}</td>'
                f'<td><span class="badge badge-{state}">{state}</span></td>'
                f'<td class="mono" style="color:#888;">{f["casiz_numbers"] if f["casiz_numbers"] != "[]" else ""}</td>'
                f'</tr>'
            )
        html_parts.append("</table>")

    return HTMLResponse("".join(html_parts))
