"""
Search routes — GET /search, GET /search/results

Provides filename and CASIZ number search with debounced htmx results.

Inputs: Database with scan results, search parameters.
Outputs: Rendered search.html or search_results.html partial.
"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/search")
async def search_page(request: Request, q: str = "", type: str = "filename"):
    """
    Render the search page with input form.

    Inputs: q (search query), type (filename or casiz).
    Outputs: TemplateResponse with search form and initial results.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    files = []

    if scan and q:
        files = db.search_files(scan["scan_id"], q, type, limit=100)

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "active_page": "search",
            "scan": scan,
            "query": q,
            "search_type": type,
            "files": files,
            "scan_root": settings.scan_root,
        },
    )


@router.get("/search/results")
async def search_results(request: Request, q: str = "", type: str = "filename"):
    """
    htmx endpoint: return search results as HTML fragment.

    Inputs: q (search query), type (filename or casiz).
    Outputs: HTMLResponse with rendered search_results.html partial.

    Called by the debounced htmx input on the search page.
    """
    db = request.app.state.db
    templates = request.app.state.templates
    settings = request.app.state.settings

    scan = db.get_latest_scan()
    files = []

    if scan and q:
        files = db.search_files(scan["scan_id"], q, type, limit=100)

    html = templates.get_template("search_results.html").render(
        files=files, query=q, scan_root=settings.scan_root
    )
    return HTMLResponse(html)
