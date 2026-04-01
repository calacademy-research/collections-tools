# Collections Tools

Internal tools for CAS collection management, deployed on ibss-central.

## Tools

### [fill-higher-taxa](fill-higher-taxa/)
Web app that fills in higher taxonomic ranks in .xls spreadsheets by looking them up in the Specify casiz database. Upload a spreadsheet with partial taxonomy (genus/species, family, etc.) and get back a complete hierarchy with color-coded cells showing what was found, replaced, or flagged as new.

**URL:** http://ibss-central:8001

### [iz-ingest-status](iz-ingest-status/)
Dashboard for tracking the ingest status of ~133K IZ image files. Scans the NFS image folder, cross-references with the Specify casiz attachment table, and classifies files into states (ingested, pending, no match, etc.). Built for the IZ collection manager to monitor the web-asset-importer pipeline.

**URL:** http://ibss-central:8000
