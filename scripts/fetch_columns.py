#!/usr/bin/env python3
"""
Fetch column metadata from CMS data-viewer API endpoints.
Caches results in input/columns/{uuid}.json for later use.

Input:  <project_root>/input/data.json
Output: <project_root>/input/columns/{uuid}.json
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
import argparse
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project root is one level above this script (scripts/)
PROJECT_ROOT = Path(__file__).parent.parent
INPUT_FILE = PROJECT_ROOT / "input" / "data.json"
COLUMNS_DIR = PROJECT_ROOT / "input" / "columns"


def extract_uuid_from_identifier(identifier):
    """Extract UUID from CMS identifier URL."""
    match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', identifier)
    return match.group(1) if match else None


def fetch_column_data(dataset_uuid, timeout=30):
    """
    Fetch column metadata from CMS data-viewer API.

    Args:
        dataset_uuid: The dataset UUID
        timeout: Request timeout in seconds

    Returns:
        dict: API response JSON or None on error
    """
    url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_uuid}/data-viewer"

    try:
        logger.info(f"Fetching: {url}")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        logger.info(f"Success! API response received")
        return data

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching {dataset_uuid}")
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.error(f"Rate limit exceeded for {dataset_uuid} - consider increasing delay")
            remaining = e.response.headers.get('X-RateLimit-Remaining', 'unknown')
            logger.error(f"Rate limit remaining: {remaining}")
        else:
            logger.error(f"HTTP error {e.response.status_code} for {dataset_uuid}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {dataset_uuid}: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response for {dataset_uuid}")
        return None


def extract_fields(api_response):
    """
    Extract fields from API response and normalize to lowercase.

    Args:
        api_response: Full API response JSON

    Returns:
        list: Normalized fields with lowercase names
    """
    try:
        fields = api_response.get('meta', {}).get('data_file_meta_data', {}).get('tableSchema', {}).get('descriptor', {}).get('fields', [])

        normalized_fields = []
        for field in fields:
            normalized_fields.append({
                'name': field.get('name', '').lower(),
                'type': field.get('type', 'string')
            })

        return normalized_fields
    except Exception as e:
        logger.error(f"Error extracting fields: {e}")
        return []


def save_column_data(dataset_uuid, data, output_dir=None):
    """
    Save column data to cache file.

    Args:
        dataset_uuid: The dataset UUID
        data: API response data
        output_dir: Directory to save files (defaults to input/columns/ under project root)
    """
    if output_dir is None:
        output_dir = COLUMNS_DIR

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{dataset_uuid}.json"

    fields = extract_fields(data)

    output_data = {
        'dataset_uuid': dataset_uuid,
        'fields': fields
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)

    logger.info(f"Saved to {output_file}")


def fetch_single_dataset(dataset_uuid):
    """Fetch column data for a single dataset (test mode)."""
    logger.info(f"=== TEST MODE: Fetching single dataset {dataset_uuid} ===")

    data = fetch_column_data(dataset_uuid)

    if data:
        fields = extract_fields(data)
        save_column_data(dataset_uuid, data)

        logger.info(f"\n=== Summary ===")
        logger.info(f"Dataset UUID: {dataset_uuid}")
        logger.info(f"Total fields: {len(fields)}")
        logger.info(f"\nFirst 5 fields:")
        for field in fields[:5]:
            logger.info(f"  - {field.get('name')} ({field.get('type')})")

        return True
    else:
        logger.error("Failed to fetch data")
        return False


def fetch_all_datasets(force=False, delay=1.0):
    """Fetch column data for all datasets in input/data.json."""
    logger.info("=== FULL MODE: Fetching all datasets ===")

    if not INPUT_FILE.exists():
        logger.error(f"{INPUT_FILE} not found!")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    datasets = data.get('dataset', [])
    logger.info(f"Found {len(datasets)} datasets to process\n")

    successful = 0
    failed = 0
    skipped = 0

    for idx, dataset in enumerate(datasets, 1):
        identifier = dataset.get('identifier', '')
        dataset_uuid = extract_uuid_from_identifier(identifier)

        if not dataset_uuid:
            logger.warning(f"[{idx}/{len(datasets)}] No UUID found for: {dataset.get('title', 'Unknown')}")
            failed += 1
            continue

        output_file = COLUMNS_DIR / f"{dataset_uuid}.json"
        if output_file.exists() and not force:
            logger.info(f"[{idx}/{len(datasets)}] Skipping {dataset_uuid} (already cached)")
            skipped += 1
            continue

        logger.info(f"[{idx}/{len(datasets)}] Processing {dataset_uuid}...")
        response_data = fetch_column_data(dataset_uuid)

        if response_data:
            save_column_data(dataset_uuid, response_data)
            successful += 1
        else:
            failed += 1

        if idx < len(datasets):
            time.sleep(delay)

    logger.info(f"\n=== Final Summary ===")
    logger.info(f"Total datasets: {len(datasets)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Skipped (cached): {skipped}")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch column metadata from CMS data-viewer API'
    )
    parser.add_argument(
        '--uuid',
        help='Fetch only a specific dataset UUID (test mode)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-fetch even if cached file exists'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay between requests in seconds (default: 1.0)'
    )

    args = parser.parse_args()

    if args.uuid:
        success = fetch_single_dataset(args.uuid)
        sys.exit(0 if success else 1)
    else:
        fetch_all_datasets(force=args.force, delay=args.delay)


if __name__ == '__main__':
    main()
