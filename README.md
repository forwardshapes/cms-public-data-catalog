# CMS Public Data Catalog

A metadata-only dbt catalog of CMS datasets from data.cms.gov. This repository contains pure source definitions (YML files) with no actual data ingestion. Browse the CMS catalog, find the datasets you need, and integrate them directly into your own dbt projects.

## dbt docs

Generate the documentation:
```bash
dbt docs generate --profiles-dir .
```

Serve the documentation locally:
```bash
dbt docs serve --profiles-dir .
```
