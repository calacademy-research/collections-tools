"""
Ingest trigger routes — POST /ingest/trigger, GET /ingest/status

Allows users to fire off an IZ image ingest for a specific directory
from the Key Files page. Calls iz_import.sh — the same script cron uses —
so lockfile handling is in one place.

Inputs: Directory path from key.csv row.
Outputs: HTML fragments for htmx polling of subprocess progress.
"""

import os
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

LOCKFILE = "/host_tmp/iz_import.lock"
IMPORT_SCRIPT = "/admin/web-asset-importer/iz_import.sh"


@dataclass
class IngestState:
    """Thread-safe ingest progress tracker."""
    directory: str = ""
    status: str = "running"  # running, completed, failed
    output_lines: list = field(default_factory=list)
    exit_code: Optional[int] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def append_output(self, line: str):
        with self._lock:
            self.output_lines.append(line)

    def finish(self, exit_code: int):
        with self._lock:
            self.exit_code = exit_code
            self.status = "completed" if exit_code == 0 else "failed"

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "directory": self.directory,
                "status": self.status,
                "output_lines": list(self.output_lines),
                "exit_code": self.exit_code,
                "line_count": len(self.output_lines),
            }


_current_ingest: Optional[IngestState] = None


def get_current_ingest() -> Optional[IngestState]:
    return _current_ingest


def _run_ingest(directory: str):
    """Run iz_import.sh in a subprocess, streaming output to IngestState."""
    global _current_ingest

    state = IngestState(directory=directory)
    _current_ingest = state

    try:
        # Call the same shell script cron uses — it handles lockfile, venv, cleanup
        proc = subprocess.Popen(
            [IMPORT_SCRIPT, directory],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            state.append_output(line.rstrip("\n"))

        proc.wait()
        state.finish(proc.returncode)

    except Exception as e:
        state.append_output(f"ERROR: {e}")
        state.finish(1)


@router.post("/ingest/trigger")
async def ingest_trigger(request: Request, path: str = Form(...)):
    """
    Start an ingest for the directory containing the given key.csv.

    Inputs: path — the key.csv file path; ingest runs on its parent directory.
    Outputs: HTML fragment with polling trigger or error message.
    """
    # Determine the directory to ingest (parent of key.csv)
    if os.path.isfile(path):
        ingest_dir = os.path.dirname(path)
    else:
        ingest_dir = path

    if not os.path.isdir(ingest_dir):
        return HTMLResponse(
            f'<span style="color:#dc3545; font-size:0.85rem;">Directory not found: {ingest_dir}</span>'
        )

    # Check lockfile — same one used by cron and iz_import.sh
    if os.path.exists(LOCKFILE):
        return HTMLResponse(
            '<span style="color:#856404; font-size:0.85rem;">'
            'An import is already running (lockfile exists). Try again later.</span>'
        )

    # Check if we already have an ingest running in this process
    current = get_current_ingest()
    if current and current.status == "running":
        return HTMLResponse(
            '<span style="color:#856404; font-size:0.85rem;">'
            'An ingest is already in progress.</span>'
        )

    # Launch in background thread
    thread = threading.Thread(target=_run_ingest, args=(ingest_dir,), daemon=True)
    thread.start()

    short_dir = ingest_dir.split("/")[-1] or ingest_dir
    return HTMLResponse(
        f'<div hx-get="/ingest/status" hx-trigger="every 2s" hx-swap="innerHTML">'
        f'<span class="spinner"></span> '
        f'<span style="font-size:0.85rem;">Starting ingest for {short_dir}...</span>'
        f'</div>'
    )


@router.get("/ingest/status")
async def ingest_status(request: Request):
    """
    htmx endpoint: poll ingest progress.

    Returns HTML fragment with current output and status.
    Called by htmx every 2 seconds while an ingest is running.
    """
    state = get_current_ingest()

    if not state:
        return HTMLResponse(
            '<span style="font-size:0.85rem; color:#6c757d;">No ingest running.</span>'
        )

    info = state.to_dict()
    # Show last few lines of output
    recent = info["output_lines"][-8:] if info["output_lines"] else []
    output_html = "<br>".join(
        f'<span style="font-size:0.75rem; font-family:monospace;">{_escape(line)}</span>'
        for line in recent
    )

    if info["status"] == "running":
        return HTMLResponse(
            f'<div hx-get="/ingest/status" hx-trigger="every 2s" hx-swap="innerHTML">'
            f'<div style="display:flex; align-items:center; gap:0.5rem; margin-bottom:0.5rem;">'
            f'<span class="spinner"></span>'
            f'<span style="font-size:0.85rem;">Ingesting... ({info["line_count"]} lines of output)</span>'
            f'</div>'
            f'<div style="background:#1a1a2e; padding:0.5rem; border-radius:4px; max-height:200px; overflow-y:auto;">'
            f'{output_html}'
            f'</div>'
            f'</div>'
        )
    elif info["status"] == "completed":
        return HTMLResponse(
            f'<div>'
            f'<div style="font-size:0.85rem; color:#28a745; margin-bottom:0.5rem;">'
            f'Ingest completed successfully for {_escape(info["directory"].split("/")[-1])}.'
            f'</div>'
            f'<div style="background:#1a1a2e; padding:0.5rem; border-radius:4px; max-height:200px; overflow-y:auto;">'
            f'{output_html}'
            f'</div>'
            f'</div>'
        )
    else:
        return HTMLResponse(
            f'<div>'
            f'<div style="font-size:0.85rem; color:#dc3545; margin-bottom:0.5rem;">'
            f'Ingest failed (exit code {info["exit_code"]}).'
            f'</div>'
            f'<div style="background:#1a1a2e; padding:0.5rem; border-radius:4px; max-height:200px; overflow-y:auto;">'
            f'{output_html}'
            f'</div>'
            f'</div>'
        )


def _escape(text: str) -> str:
    """Basic HTML escaping."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
