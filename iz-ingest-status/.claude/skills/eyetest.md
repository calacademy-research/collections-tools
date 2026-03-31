# Eyetest — Screenshot-Based Visual Verification (Server-Side Adaptation)

## What it is

Eyetest is a visual verification discipline adapted for server-rendered HTML + htmx applications.
Since this is a server-side app (FastAPI + Jinja2 + htmx), we verify by fetching rendered pages
and checking that the HTML output matches expectations. For visual layout checks, use Playwright
to screenshot the running app.

## When to use

Invoke with `/eyetest` when:
- Verifying that a page renders correctly after changes
- Checking that htmx partials return the expected HTML fragments
- Verifying visual layout of dashboard, tree browser, or key.csv editor
- Any time a test passes assertions but you need to confirm it *looks* right

## How to write an eyetest

### For HTML content verification (fast, no browser needed):

```python
import httpx
from bs4 import BeautifulSoup

def test_dashboard_renders():
    """Verify dashboard shows scan summary with state counts."""
    r = httpx.get("http://localhost:8000/")
    assert r.status_code == 200
    soup = BeautifulSoup(r.text, "html.parser")
    # Check key elements exist
    assert soup.find("div", class_="state-counts"), "Missing state counts section"
    assert soup.find("table", class_="scan-history"), "Missing scan history table"
```

### For visual verification (Playwright screenshots):

```python
from playwright.sync_api import sync_playwright

def capture_page(url: str, output_path: str):
    """Capture a screenshot of a page for visual review."""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})
        page.goto(url, wait_until="networkidle")
        page.screenshot(path=output_path)
        browser.close()
```

## Verification protocol — MANDATORY

You may not say "test passed" or "eyetest looks good" until ALL of these are done:

### Step 1: Check the rendered HTML
Fetch the page, parse with BeautifulSoup or read raw HTML. Verify key elements exist,
tables have expected columns, state badges show correct colors/text.

### Step 2: Describe what you see
For each page or screenshot, write one sentence: "Dashboard showing 5 state categories with
bar chart" or "Tree browser with expanded Piotrowski folder showing 12 children."

### Step 3: Check data proportionality
If the dashboard shows "ingested: 2, pending: 0" but the scan folder has 133K files,
stop. Something is wrong. Cross-reference counts against known baselines.

### Step 4: Flag anything unexpected
Missing nav links, broken htmx endpoints, empty tables where data should be, wrong
state colors — call it out even if assertions pass.

## Known baselines

These are approximate counts from the scan folder (use to sanity-check):
- Total files on disk: ~133K
- Ingested in Specify: ~98.7K attachment records
- key.csv files: ~245
- Top-level photographer folders: ~87
- Folders with Remove=TRUE: ~126 key.csv files
- Folders missing key.csv: at least 2 top-level

## Key files

| File | Purpose |
|------|---------|
| `tests/` | Unit and integration tests |
| `app/templates/` | Jinja2 templates (the visual output) |
| `eyetest-results/` | Screenshot PNGs from visual checks |
