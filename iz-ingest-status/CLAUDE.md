# IZ Ingest Status Tool

## What This Is

A standalone web tool for the IZ collection manager (Chrissy Piotrowski) to see what's been ingested, what's pending, what's broken, and to edit key.csv files that control ingest behavior. It cross-references the scan folder (133K files on NFS), the Specify attachment table (98.7K records on ntobiko), and cached scan results in SQLite.

## Stack

- Python 3.12 + FastAPI + uvicorn
- htmx for interactivity (no JS framework)
- SQLite for scan cache
- Docker + Traefik (matching ibss-central's existing pattern)
- Deployed at hostname `iz-status.ibss.calacademy.org`

## Development Environment

The app runs on ibss-central (10.4.90.123 / 10.2.22.43). During development, run directly:

```bash
cd /admin/iz-ingest-status
source venv/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

For production, use Docker:
```bash
docker compose up -d
```

## Key Paths on This Machine

| Path | What |
|------|------|
| `/admin/iz-ingest-status/` | This project |
| `/admin/web-asset-importer/` | The actual IZ import tool (DO NOT MODIFY) |
| `/admin/web-asset-importer/iz_importer.py` | Classification logic we ported from |
| `/admin/web-asset-importer/config_files/iz_config.py` | Regex patterns, DB config |
| `/letter_drives/n_drive/izg/iz images_curated for cas sci computing ingest/` | The scan folder (133K files, NFS mount) |
| `/admin/docker/compose.yml` | Main Docker compose with Traefik |
| `/admin/docker/traefik_config/traefik.yml` | Traefik reverse proxy config |

## Specify Database (READ-ONLY)

- Host: ntobiko.calacademy.org:3306
- Database: casiz
- User: jfong
- Password: J0nd@vid
- Key table: `attachment` (98.7K records, `origFilename` is the dedup key)

**This tool reads Specify. It never writes to it.**

## Development Guidelines

- Persistent: finish tasks, don't give up
- Only satisfied when the job is done as requested
- Follow instructions 100%; stop and ask questions without assumptions before making changes
- Write clear code with comments for those with less background knowledge
- When implementing external libraries or APIs, search for latest documentation first
- All functions should list inputs, outputs, and any data transformations in comment blocks
- Write unit tests down to the function level, maintain them with every update
- Create a test running script at root level
- Create integration tests when possible
- When identifying a problem in chat, reproduce it with a failing test BEFORE producing fixes
- When adding features or bug reports, add a requirement to the appropriate doc, then a test, then the fix

## Writing Style

When writing prose (docs, stories, descriptions), keep it plain and factual:
- No grandiose language: avoid "extraordinary", "remarkable", "unprecedented"
- Prefer short, direct sentences over rhetorical flourishes
- State what happened, not how impressed the reader should be

## Data Safety

- **Never modify the Specify database.** Read-only access only.
- **Never modify files in `/admin/web-asset-importer/`.** That's the production importer.
- **key.csv edits are the one write operation.** They modify files on the NFS mount. Be careful.

## Relationship to web-asset-importer

This is a **standalone, read-only companion** to the IZ import pipeline at `/admin/web-asset-importer/`. The classification logic is ported from `iz_importer.py` but this tool:
- Does NOT run imports
- Does NOT upload images
- Does NOT modify the Specify database
- Does NOT read EXIF data (too slow for 133K files)
- DOES edit key.csv files on disk (the one write operation)
- DOES scan the filesystem and cross-reference with Specify

## Docker / Traefik Deployment

ibss-central uses Traefik v2.5 as a reverse proxy. Services register via Docker labels:

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.iz-status.rule=Host(`iz-status.ibss.calacademy.org`)"
  - "traefik.http.routers.iz-status.entrypoints=web"
  - "traefik.http.services.iz-status.loadbalancer.server.port=8000"
```

External Docker network: `proxy`
