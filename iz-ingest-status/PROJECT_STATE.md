# IZ Ingest Status Tool — Project State

This document captures everything needed to implement the IZ Ingest Status Tool.
It was written at the end of a planning session and should be read before starting implementation.

---

## Background

Chrissy Piotrowski (cpiotrowski), the IZ collection manager, needs visibility into the automated
image ingest pipeline. Currently the only output is a `file_log.tsv` that's overwritten every run —
no persistent state, no GUI, no way to see what's broken without reading a 98K-line TSV.

The ingest pipeline lives at `/admin/web-asset-importer/` (GitHub: `calacademy-research/web-asset-importer`,
branch `prod_edits`). It runs every 2 days at 1AM via cron:
```
0 1 */2 * * admin /admin/web-asset-importer/iz_import.sh
```

This status tool is a **standalone companion** — it does NOT modify the importer code.

---

## What We're Building

A web app that:
1. Scans the IZ image folder (~133K files) on disk
2. Cross-references with the Specify attachment database (~98.7K records)
3. Classifies each file into one of 8 states
4. Caches results in SQLite for fast browsing and diffing
5. Provides a web UI for browsing, searching, and editing key.csv files

---

## The 8 File States

For each file on disk, classification proceeds in order:

1. **forbidden_extension** — Extension not in (jpg|jpeg|tiff|tif|png|dng)
2. **skipping_crrf** — Path contains 'crrf'
3. **dot_prefixed** — Basename starts with '.'
4. **missing_key_csv** — No key.csv found walking up the directory tree (continue classifying)
5. **removed** — Nearest key.csv has `remove=true`
6. **no_casiz_match** — No CASIZ number could be extracted from filename or directory
7. **ingested** — File's `origFilename` found in Specify attachment table
8. **pending** — Has CASIZ number, not yet in Specify → ready for next import run

---

## Classification Logic to Port

Port from `/admin/web-asset-importer/iz_importer.py`. Key details:

### Extension Filter
```python
# From iz_config.py
IMAGE_SUFFIX = r'[a-z\-\(\)0-9 ©_,.]*(\.(jpg|jpeg|tiff|tif|png|dng))'
# Full path regex: r'^.*' + IMAGE_SUFFIX
```

### Skip Rules
- Basename starts with `.` → dot_prefixed
- Lowercase path contains `crrf` → skipping_crrf

### key.csv Lookup
Walk up directory tree from file's directory. First key.csv found wins. Parse with CSV reader.
Try UTF-8, fall back to Latin-1. Column mappings (case-insensitive):
```
copyrightdate → CopyrightDate
copyrightholder → CopyrightHolder
credit → Credit
license → License
remarks → Remarks
ispublic → IsPublic
subtype → subType
createdbyagent → createdByAgent
metadatatext → creator
remove → remove
erase_exif_fields → erase_exif_fields
```
If `remove` field is truthy (TRUE/True/true/1/yes) → state is `removed`.

### CASIZ Number Extraction

Uses the `regex` library (NOT stdlib `re`) because of conditional patterns.

**Priority chain:** filename → directory name (reversed path segments) → fallback regex

**Main regex** (from iz_config.py CASIZ_NUMBER_REGEX):
- Verbose, case-insensitive
- Handles prefixes: CASIZ, CAS, CAS:IZ, IZ, INV
- Excludes camera serials: DSC_NNNNN, P_NNNNN
- Excludes dates: YYYYMMDD patterns
- Suppresses IZACC matches
- Bridges AND/OR between multiple numbers
- Min digits with prefix: 3, without prefix: 5, max: 12

**Fallback regex:**
```python
CASIZ_FALLBACK_REGEX = r'(?i)(?:CASIZ|CAS)[\s_#-]*(\d{3,12})(?!\d)'
```

The extract function:
1. Run main regex against input string
2. For each match, check it's not suppressed (IZACC) or a camera serial
3. If no matches, try fallback regex
4. Return list of integer CASIZ numbers

### "Already Processed" Check (simplified for status tool)
The importer checks both Specify AND the image server. We only check Specify (one bulk query):
```sql
SELECT origFilename FROM attachment
```
Load all ~98.7K filenames into a set. For each file on disk, check if its basename (lowercased) is in the set.

**Important:** `origFilename` has zero duplicates in the current database. It's the unique dedup key.

---

## Scan Folder Structure

Root: `/letter_drives/n_drive/izg/iz images_curated for cas sci computing ingest/`

- 133,418 total files, 3,632 directories
- 87 top-level photographer folders
- 245 key.csv files (85 at top level, 160 nested)
- 2 top-level folders missing key.csv: "Elizabeth Kools Copyright CAS/", "repair card catalog 4 digits/"
- File extensions: jpg (80K), JPG (43K), CR2 (3K), xmp (2.5K), tif (1.9K), db (496), dng (391), etc.
- Tree depth: up to 8 levels
- Top 3 folders by file count: Card Catalog (~40K), Piotrowski (~30K), Label Scans (~29K) = 74% of files
- 126 key.csv files have Remove=TRUE, 42 have Remove=FALSE

---

## Specify Attachment Table Schema

On ntobiko.calacademy.org:3306/casiz:

```sql
-- Key columns from the attachment table (32 total columns)
AttachmentID         INT AUTO_INCREMENT PRIMARY KEY
TimestampCreated     DATETIME
TimestampModified    DATETIME
AttachmentLocation   VARCHAR(128)    -- always non-NULL
origFilename         MEDIUMTEXT NOT NULL  -- unique dedup key, zero duplicates
CopyrightHolder      VARCHAR(64)
Credit               VARCHAR(64)
License              VARCHAR(64)
IsPublic             BIT(1) DEFAULT 1
GUID                 VARCHAR(128)
MimeType             VARCHAR(64)
CreatedByAgentID     INT             -- 26280 for automated imports
Visibility           TINYINT(4)      -- NULL for all records currently

-- Join table for specimen linkage
collectionobjectattachment:
  CollectionObjectAttachmentID  INT PK
  CollectionObjectID            INT     -- links to the specimen
  AttachmentID                  INT     -- links to attachment
  Ordinal                       INT
  CollectionMemberID            INT
```

Stats: 98,708 records. Date range: 2025-06-26 to 2026-02-21. Records are write-once (Created = Modified).

---

## ibss-central Environment

- Ubuntu 24.04.2 LTS, 12 CPUs, 32GB RAM
- Python 3.12.3, pip3, SQLite 3.45.1
- Docker with Traefik v2.5 on ports 80/443
- External Docker network: `proxy`
- Projects live in `/admin/` (owned by admin user)
- SSH access: `ssh ibss-alt@ibss-central`, then `sudo su - admin` or use sudo

### Traefik routing pattern
Services register via Docker labels on the `proxy` network:
```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.SERVICENAME.rule=Host(`HOSTNAME.ibss.calacademy.org`)"
  - "traefik.http.routers.SERVICENAME.entrypoints=web"
  - "traefik.http.services.SERVICENAME.loadbalancer.server.port=PORT"
networks:
  - proxy
```

---

## Approved Implementation Plan

### Project Structure

```
/admin/iz-ingest-status/
├── .claude/                     # Claude dev config
├── CLAUDE.md                    # Project instructions
├── PROJECT_STATE.md             # This file
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app, lifespan, router registration
│   ├── config.py                # Settings from env vars, compiled regexes
│   ├── scanner.py               # Filesystem walker + orchestrator
│   ├── classify.py              # CASIZ extraction, file state classification
│   ├── key_csv.py               # key.csv parse/edit/inheritance
│   ├── database.py              # SQLite schema + queries
│   ├── specify_client.py        # Read-only MySQL to Specify attachment table
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── dashboard.py         # GET /
│   │   ├── tree.py              # GET /tree, /tree/expand
│   │   ├── problems.py          # GET /problems
│   │   ├── search.py            # GET /search
│   │   ├── diff.py              # GET /diff
│   │   ├── key_editor.py        # GET/POST /key
│   │   └── scan.py              # POST /scan/trigger, GET /scan/status
│   └── templates/
│       ├── base.html
│       ├── dashboard.html
│       ├── tree.html
│       ├── tree_node.html       # htmx partial
│       ├── problems.html
│       ├── search.html
│       ├── search_results.html  # htmx partial
│       ├── diff.html
│       ├── key_editor.html
│       ├── key_form.html        # htmx partial
│       └── scan_status.html     # htmx partial
└── tests/
    ├── test_classify.py
    ├── test_key_csv.py
    ├── test_scanner.py
    ├── test_database.py
    └── fixtures/
        └── sample_tree/         # Fake dir tree for tests
```

### SQLite Schema

```sql
CREATE TABLE scans (
    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    total_files INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'  -- running, completed, failed
);

CREATE TABLE file_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL REFERENCES scans(scan_id),
    file_path TEXT NOT NULL,
    directory TEXT NOT NULL,
    filename TEXT NOT NULL,
    state TEXT NOT NULL,           -- ingested/pending/no_casiz_match/forbidden_extension/removed/missing_key_csv/dot_prefixed/skipping_crrf
    casiz_numbers TEXT,            -- JSON array of ints
    casiz_source TEXT,             -- filename/directory/NULL
    key_csv_path TEXT,
    has_remove_flag INTEGER DEFAULT 0,
    file_size INTEGER,
    file_mtime TEXT
);

CREATE TABLE key_csvs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    directory TEXT NOT NULL,
    parsed_data TEXT,              -- JSON
    file_count INTEGER DEFAULT 0  -- how many image files this key.csv governs
);

CREATE TABLE directory_counts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER NOT NULL,
    directory TEXT NOT NULL,
    total_files INTEGER, ingested INTEGER, pending INTEGER,
    no_casiz INTEGER, forbidden_ext INTEGER, removed INTEGER,
    missing_key INTEGER, dot_prefixed INTEGER, crrf_skipped INTEGER
);

-- Indexes
CREATE INDEX idx_file_results_scan_state ON file_results(scan_id, state);
CREATE INDEX idx_file_results_scan_dir ON file_results(scan_id, directory);
CREATE INDEX idx_file_results_scan_path ON file_results(scan_id, file_path);
CREATE INDEX idx_file_results_scan_casiz ON file_results(scan_id, casiz_numbers);
CREATE INDEX idx_dir_counts_scan ON directory_counts(scan_id, directory);
CREATE INDEX idx_key_csvs_scan ON key_csvs(scan_id, directory);
```

### Scan Flow

1. Create scan record in SQLite
2. Bulk query Specify: `SELECT origFilename FROM attachment` → set of ~98K lowercased strings
3. `os.walk()` the scan folder (133K files, 3.6K dirs)
4. For each file: `classify_file()` → ClassificationResult
5. Batch insert into SQLite every 1000 files
6. Aggregate `directory_counts` after scan completes
7. Record key.csv files found + parse them

Expected duration: 30-90 seconds (NFS walk + one MySQL query, no EXIF reads).

### Web UI Routes

| Route | Purpose |
|-------|---------|
| `GET /` | Dashboard: state counts, bar chart, last scan, recent scans |
| `GET /tree` | Tree browser root |
| `GET /tree/expand?path=...` | htmx: expand directory, show children with state badges |
| `GET /problems?type=...&page=N` | Problem reports with filter tabs |
| `GET /search?q=...&type=casiz\|filename` | Search with debounced htmx results |
| `GET /diff?old=N&new=N` | Diff between two scans |
| `GET /key` | List all 245 key.csv files with metadata |
| `GET /key/edit?path=...` | Edit form for one key.csv |
| `POST /key/save` | Save key.csv to disk |
| `POST /scan/trigger` | Start background scan |
| `GET /scan/status` | htmx: poll scan progress |

State colors: ingested=green, pending=blue, no_casiz=orange, forbidden=gray,
removed=strikethrough-gray, missing_key=yellow, dot_prefixed=light-gray, crrf=purple.

### Docker Deployment

```yaml
services:
  iz-ingest-status:
    build: .
    container_name: iz-ingest-status
    restart: unless-stopped
    volumes:
      - /letter_drives/n_drive:/letter_drives/n_drive  # rw for key.csv editing
      - iz-status-data:/data
    environment:
      - IZ_SPECIFY_DB_PASSWORD=${IZ_SPECIFY_DB_PASSWORD}
    networks:
      - proxy
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.iz-status.rule=Host(`iz-status.ibss.calacademy.org`)"
      - "traefik.http.routers.iz-status.entrypoints=web"
      - "traefik.http.services.iz-status.loadbalancer.server.port=8000"
```

### Dependencies

```
fastapi==0.115.0
uvicorn[standard]==0.32.0
jinja2==3.1.4
regex==2024.9.11
mysql-connector-python==9.1.0
pydantic-settings==2.5.0
python-multipart==0.0.12
```

### Implementation Order

**Phase 1: Core engine (testable locally)**
1. Project skeleton: requirements.txt, config.py, Dockerfile, docker-compose.yml
2. `classify.py` — CASIZ extraction + file state logic. Port exactly from iz_importer.py
3. `key_csv.py` — Parse/edit key.csv with inheritance
4. `test_classify.py` + `test_key_csv.py` — Unit tests against known inputs
5. `database.py` — SQLite schema + all queries
6. `test_database.py` — In-memory SQLite tests

**Phase 2: Scanner + DB client**
7. `specify_client.py` — Bulk query Specify attachments
8. `scanner.py` — Walk filesystem, classify, store in SQLite
9. `test_scanner.py` — Integration test with fixture directory tree

**Phase 3: Web UI**
10. `main.py` + `base.html` — FastAPI app shell with nav
11. `dashboard.py` + `dashboard.html`
12. `tree.py` + `tree.html` + `tree_node.html` (htmx expansion)
13. `problems.py` + `problems.html`
14. `search.py` + `search.html`
15. `diff.py` + `diff.html`
16. `key_editor.py` + `key_editor.html` + `key_form.html`
17. `scan.py` + `scan_status.html`

**Phase 4: Deploy**
18. Build Docker image on ibss-central
19. `docker compose up -d`, verify Traefik routing
20. Run first scan, verify counts match reality

### Verification Targets

- First scan should show ~133K total files, ~64K ingested, ~12K no_casiz, ~7K forbidden, ~692 removed
- `SELECT COUNT(*) FROM attachment` on ntobiko should match ingested count
- Tree browser: navigate to a known photographer folder, verify counts
- key.csv editor: edit a test key.csv, verify file on disk changes
- Diff: run two scans, verify diff shows changes

---

## Key Design Decisions

1. **No EXIF reading** — Too slow for 133K files. Files that only match via EXIF show as `no_casiz_match`. Documented limitation.
2. **SQLite, not PostgreSQL** — Single-user tool, no concurrent writes needed. Simpler deployment.
3. **htmx, not React** — Server-rendered, minimal JS. Matches the tool's admin-panel nature.
4. **key.csv editing is the only write operation** — Everything else is read-only.
5. **Removal is reversible** — Since files remain on disk, toggling key.csv `remove` back to false makes them eligible for the next import run.
6. **key.csv inheritance is a key feature** — Show which key.csv impacts which file, make the inheritance chain visible.
