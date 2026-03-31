"""
Key CSV editor routes — GET /key, GET /key/edit, POST /key/save

Lists all key.csv files and provides a form to edit them.
key.csv editing is the ONE write operation this tool performs.

Inputs: Database with key.csv records, form data for edits.
Outputs: Rendered templates and file modifications on disk.
"""

import os

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from app.key_csv import parse_key_csv, save_key_csv

router = APIRouter()


@router.get("/key")
async def key_list(request: Request):
    """
    List all key.csv files with metadata.

    Inputs: request with app state.
    Outputs: TemplateResponse with key.csv list from the latest scan.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    key_csvs = []

    if scan:
        key_csvs = db.get_key_csvs(scan["scan_id"])

    return templates.TemplateResponse(
        "key_editor.html",
        {
            "request": request,
            "active_page": "key",
            "scan": scan,
            "key_csvs": key_csvs,
            "scan_root": settings.scan_root,
        },
    )


@router.get("/key/edit")
async def key_edit(request: Request, path: str):
    """
    Show the edit form for a specific key.csv file.

    Inputs: path — the key.csv file path on disk.
    Outputs: TemplateResponse with parsed key.csv data in an edit form.

    Reads the current file from disk (not from cache) to ensure
    the form shows the latest values.
    """
    templates = request.app.state.templates

    data = {}
    error = None

    if os.path.isfile(path):
        data = parse_key_csv(path)
    else:
        error = f"File not found: {path}"

    return templates.TemplateResponse(
        "key_form.html",
        {
            "request": request,
            "active_page": "key",
            "path": path,
            "data": data,
            "saved": False,
            "error": error,
        },
    )


@router.post("/key/save")
async def key_save(
    request: Request,
    path: str = Form(...),
    CopyrightHolder: str = Form(""),
    CopyrightDate: str = Form(""),
    Credit: str = Form(""),
    License: str = Form(""),
    Remarks: str = Form(""),
    IsPublic: str = Form(""),
    creator: str = Form(""),
    createdByAgent: str = Form(""),
    subType: str = Form(""),
    remove: str = Form(""),
):
    """
    Save edits to a key.csv file on disk.

    Inputs: Form data with key.csv field values and the file path.
    Outputs: Redirect back to the edit form with success/error message.

    THIS IS THE ONE WRITE OPERATION. Modifies files on the NFS mount.
    """
    templates = request.app.state.templates

    error = None
    saved = False

    if not os.path.isfile(path):
        error = f"File not found: {path}"
    else:
        updates = {
            "CopyrightHolder": CopyrightHolder or None,
            "CopyrightDate": CopyrightDate or None,
            "Credit": Credit or None,
            "License": License or None,
            "Remarks": Remarks or None,
            "IsPublic": IsPublic or None,
            "creator": creator or None,
            "createdByAgent": createdByAgent or None,
            "subType": subType or None,
            "remove": remove or None,
        }
        try:
            save_key_csv(path, updates)
            saved = True
        except Exception as e:
            error = f"Failed to save: {e}"

    # Re-read the file to show current state
    data = parse_key_csv(path) if os.path.isfile(path) else {}

    return templates.TemplateResponse(
        "key_form.html",
        {
            "request": request,
            "active_page": "key",
            "path": path,
            "data": data,
            "saved": saved,
            "error": error,
        },
    )
