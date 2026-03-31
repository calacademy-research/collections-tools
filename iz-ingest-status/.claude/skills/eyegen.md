# Eyegen — Iterative Visual Design with AI Feedback (Server-Side Adaptation)

## What it is

Eyegen is an iterative UX implementation loop where the AI builds or modifies a UI
component, renders it in a real browser, visually evaluates the result, and iterates
until the output matches the intent. Adapted for server-rendered FastAPI + Jinja2 + htmx.

The loop:
```
write template/CSS → render in browser → screenshot → evaluate → adjust → repeat
```

## When to use

Invoke with `/eyegen` when:
- Implementing a new page template or layout
- Fixing a visual bug in the HTML/CSS
- Tuning the dashboard, tree browser, or any visual component
- The user describes a visual outcome ("make the tree look like X", "state badges should be colorful")
- Any implementation task where "looks right" is the spec

Do NOT use eyegen for:
- Pure backend logic (scanner, classifier, DB queries)
- API-only endpoints with no HTML output
- Cases where unit tests fully cover correctness

## The eyegen loop

### 1. Understand the target
Before writing code, establish what "correct" looks like. Ask the user if unclear.

### 2. Write the code change
Modify the Jinja2 template, CSS, or htmx attributes.

### 3. Render and capture
Use curl for quick HTML checks, Playwright for visual screenshots:

```bash
# Quick HTML check
curl -s http://localhost:8000/ | python3 -c "
import sys; from bs4 import BeautifulSoup
soup = BeautifulSoup(sys.stdin.read(), 'html.parser')
print(soup.prettify()[:2000])
"

# Visual screenshot
python3 -c "
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch()
    pg = b.new_page(viewport={'width': 1280, 'height': 720})
    pg.goto('http://localhost:8000/', wait_until='networkidle')
    pg.screenshot(path='eyetest-results/eyegen/iteration-1.png')
    b.close()
"
```

### 4. Evaluate
Read the screenshot or HTML output. Describe what you see in one sentence.
Compare against the target. Identify gaps.

### 5. Adjust and repeat
If the output doesn't match, modify and go back to step 3.
Track what you changed and why — prevent circular edits.

### 6. Final verification
Once it matches the target, run the test suite to confirm no regressions.

## Practical patterns

### Dashboard layout iterations
The dashboard has state counts, a bar chart, scan history. Iterate on layout and colors.

### Tree browser iterations
The tree browser uses htmx to expand directories. Iterate on indentation, state badges,
expand/collapse behavior.

### State color tuning
State colors: ingested=green, pending=blue, no_casiz=orange, forbidden=gray,
removed=strikethrough-gray, missing_key=yellow, dot_prefixed=light-gray, crrf=purple.
These need to be visible and distinguishable.

## Iteration discipline

- **Log each iteration.** Note what changed, what the screenshot showed, what's next.
- **Limit iterations.** Past 5 rounds without convergence → stop and reassess.
- **Don't chase perfection.** Correct, not pixel-perfect.
- **Always finish with eyetest.** The final state should pass the test suite.

## Relationship to eyetest

| | Eyetest | Eyegen |
|---|---------|--------|
| **When** | After implementation | During implementation |
| **Purpose** | Verify visual correctness | Drive toward visual correctness |
| **Loop** | Single pass: render → review → pass/fail | Multi-pass: code → render → evaluate → adjust |
| **Output** | Pass/fail verdict | Working implementation |
