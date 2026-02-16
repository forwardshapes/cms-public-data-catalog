# CMS Public Data Catalog

A metadata-only dbt catalog of CMS datasets from data.cms.gov. This repository contains pure source definitions (YML files) with no actual data ingestion. Browse the CMS catalog, find the datasets you need, and integrate them directly into your own dbt projects.

## dbt docs

Generate the documentation:
```bash
uv run dbt docs generate --profiles-dir .
```

Serve the documentation locally:
```bash
uv run dbt docs serve --profiles-dir .
```
## Generating Source Files

The `scripts/` folder contains two Python scripts used to generate and maintain the source YML files in `models/sources/`.

### Prerequisites

Install dependencies with [uv](https://github.com/astral-sh/uv):
```bash
uv sync
```

Place the CMS catalog JSON at `input/data.json`. Download it from:
```
https://data.cms.gov/data.json
```

### Step 1 — Fetch Column Metadata

Fetches column names and types for each dataset from the CMS data-viewer API and caches them locally.

```bash
uv run python scripts/fetch_columns.py
```

Results are saved to `input/columns/{uuid}.json`. Already-cached datasets are skipped automatically.

Options:
```
--uuid UUID    Fetch a single dataset by UUID (useful for testing)
--force        Re-fetch all datasets, ignoring existing cache
--delay N      Seconds to wait between API requests (default: 1.0)
```

### Step 2 — Generate Source YML Files

Reads `input/data.json` and the cached column data to generate one source YML file per CMS dataset.

```bash
uv run python scripts/generate_sources.py
```

Output files are written to `models/sources/` with the naming pattern `cms_{dataset_name}_sources.yml`.

### Re-generating After a Catalog Update

To refresh everything when CMS publishes a new `data.json`:

```bash
# Download new data.json to input/data.json, then:
uv run python scripts/fetch_columns.py   # fetches only new/missing datasets
uv run python scripts/generate_sources.py
```

Use `--force` on `fetch_columns.py` to re-fetch all column data regardless of cache.
