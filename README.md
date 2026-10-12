# Collections Tools

Internal tools for CAS collection management, deployed on ibss-central.

## Tools

### [fill-higher-taxa](fill-higher-taxa/)
Web app that fills in higher taxonomic ranks in .xls spreadsheets by looking them up in the Specify casiz database. Upload a spreadsheet with partial taxonomy (genus/species, family, etc.) and get back a complete hierarchy with color-coded cells showing what was found, replaced, or flagged as new.

**URL:** https://fill-higher-taxa.ibss.calacademy.org (old http://ibss-central:8001 redirects)

### [iz-ingest-status](iz-ingest-status/)
Dashboard for tracking the ingest status of ~133K IZ image files. Scans the NFS image folder, cross-references with the Specify casiz attachment table, and classifies files into states (ingested, pending, no match, etc.). Built for the IZ collection manager to monitor the web-asset-importer pipeline.

**URL:** https://iz-status.ibss.calacademy.org (old http://ibss-central:8000 redirects)
