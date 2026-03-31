"""
FastAPI application entry point.

Sets up the app with lifespan management (DB init/close), template engine,
and all route registrations.

Inputs: Configuration from app.config.
Outputs: FastAPI app instance ready for uvicorn.

Run with: uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from app.config import get_settings
from app.database import Database
from app.routes import dashboard, diff, ingest, key_editor, problems, scan, search, tree

LOCKFILE = "/host_tmp/iz_import.lock"


def is_import_running():
    """Check if the IZ import process is currently running (lockfile exists)."""
    return os.path.exists(LOCKFILE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan: initialize and clean up resources.

    Startup:
        - Load settings
        - Create/connect SQLite database
        - Initialize Jinja2 template engine
        - Store everything in app.state for route access

    Shutdown:
        - Close database connection
    """
    settings = get_settings()

    # Ensure data directory exists for SQLite
    db_dir = os.path.dirname(settings.sqlite_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    db = Database(settings.sqlite_path)

    # Jinja2 template engine
    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    templates = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=True,
    )

    # Make import_running available in all templates
    templates.globals["is_import_running"] = is_import_running

    # Wrap templates to provide TemplateResponse compatibility
    app.state.db = db
    app.state.settings = settings
    app.state.templates = TemplateEngine(templates)

    yield

    db.close()


class TemplateEngine:
    """
    Wrapper around Jinja2 Environment that provides both TemplateResponse
    (for full page renders) and get_template (for htmx partials).

    Inputs: Jinja2 Environment.
    Outputs: Rendered HTML responses.
    """

    def __init__(self, env: Environment):
        self.env = env

    def TemplateResponse(self, name: str, context: dict):
        """
        Render a full template and return an HTMLResponse.

        Inputs: template name, context dict (must include 'request').
        Outputs: Starlette HTMLResponse with rendered HTML.
        """
        from starlette.responses import HTMLResponse

        template = self.env.get_template(name)
        html = template.render(**context)
        return HTMLResponse(html)

    def get_template(self, name: str):
        """
        Get a Jinja2 template for manual rendering (htmx partials).

        Inputs: template name.
        Outputs: Jinja2 Template object.
        """
        return self.env.get_template(name)


app = FastAPI(
    title="IZ Ingest Status",
    description="Status dashboard for the IZ image ingest pipeline",
    lifespan=lifespan,
)

# Register route modules
app.include_router(dashboard.router)
app.include_router(tree.router)
app.include_router(problems.router)
app.include_router(search.router)
app.include_router(diff.router)
app.include_router(key_editor.router)
app.include_router(scan.router)
app.include_router(ingest.router)
