"""
CMS dbt Catalog Parser

Generates dbt source YML files from CMS data.cms.gov JSON API responses.
Main data source: https://data.cms.gov/data.json

Output: models/sources/cms_{dataset_name}_sources.yml
"""

import json
import re
import os
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
STATIC_HEADER = """sources:
  - name: Data.CMS.gov
    description: Public data released by the Centers for Medicare & Medicaid Services (CMS)
    meta:
      data_source_url: https://data.cms.gov
    tables:"""


def transform_title_to_snake_case(title: str) -> str:
    """
    Convert title to snake_case for table names.
    Example: "Accountable Care Organization Participants" -> "accountable_care_organization_participants"
    """
    if not title:
        return "unknown_dataset"

    # Convert to lowercase
    result = title.lower()

    # Replace spaces and special characters with underscores
    result = re.sub(r'[^\w\s-]', '', result)
    result = re.sub(r'[\s-]+', '_', result)

    # Remove leading/trailing underscores and collapse multiple underscores
    result = re.sub(r'_+', '_', result).strip('_')

    return result


def transform_keywords_to_tags(keywords: List[str]) -> List[str]:
    """
    Convert keywords array to lowercase tags.
    Example: ["Medicare", "Value-Based Care"] -> ["medicare", "value-based care"]
    """
    if not keywords:
        return []

    return [keyword.lower() for keyword in keywords]


def extract_uuid_from_identifier(identifier: str) -> str:
    """
    Extract UUID from identifier URL.
    Example: "https://data.cms.gov/data-api/v1/dataset/9767cb68-8ea9-4f0b-8179-9431abc89f11/data-viewer"
         -> "9767cb68-8ea9-4f0b-8179-9431abc89f11"
    """
    if not identifier:
        return "N/A"

    # UUID pattern: 8-4-4-4-12 hex characters
    uuid_pattern = r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
    match = re.search(uuid_pattern, identifier, re.IGNORECASE)

    if match:
        return match.group(1)

    logger.warning(f"Could not extract UUID from identifier: {identifier}")
    return "N/A"


def strip_mailto(email: str) -> str:
    """
    Remove mailto: prefix from email.
    Example: "mailto:SharedSavingsProgram@cms.hhs.gov" -> "SharedSavingsProgram@cms.hhs.gov"
    """
    if not email:
        return "N/A"

    return email.replace('mailto:', '')


def load_columns_data(dataset_uuid: str, columns_dir: Path = None) -> Optional[List[Dict]]:
    """
    Load cached column data from input/columns/{uuid}.json

    Args:
        dataset_uuid: The dataset UUID
        columns_dir: Directory containing cached column files (defaults to input/columns/)

    Returns:
        List of column dicts with 'name' and 'type', or None if not found
    """
    if dataset_uuid == "N/A":
        return None

    if columns_dir is None:
        columns_dir = Path(__file__).parent.parent / "input" / "columns"

    columns_file = columns_dir / f"{dataset_uuid}.json"

    if not columns_file.exists():
        logger.debug(f"Column data not found for UUID {dataset_uuid}")
        return None

    try:
        with open(columns_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('fields', [])
    except Exception as e:
        logger.warning(f"Error loading columns for {dataset_uuid}: {e}")
        return None


def format_columns_yaml(fields: List[Dict], indent_level: int = 8) -> str:
    """
    Convert fields array to YAML columns format.

    Args:
        fields: List of dicts with 'name' and 'type' keys
        indent_level: Base indentation level for 'columns:' (default 8 spaces)

    Returns:
        Formatted YAML string for columns section
    """
    if not fields:
        return ""

    lines = []
    lines.append(" " * indent_level + "columns:")

    for field in fields:
        name = field.get('name', 'unknown')
        field_type = field.get('type', 'string')

        # Escape special characters for YAML double-quoted strings
        # Must escape backslashes first, then double quotes
        name = name.replace('\\', '\\\\').replace('"', '\\"')

        lines.append(" " * (indent_level + 2) + f'- name: "{name}"')
        lines.append(" " * (indent_level + 4) + f"type: {field_type}")

    return '\n'.join(lines)


def format_multiline_description(description: str, indent_level: int = 10, max_line_length: int = 120) -> str:
    """
    Format description with YAML pipe syntax and proper indentation.
    Wraps long lines at max_line_length for readability.
    Each line of the description should be indented by indent_level spaces.
    """
    if not description:
        return " " * indent_level + "N/A"

    # Split into paragraphs (separated by blank lines) and filter out empty ones
    paragraphs = [p.strip() for p in description.split('\n\n') if p.strip()]

    indented_lines = []
    for idx, paragraph in enumerate(paragraphs):
        # Remove existing line breaks within the paragraph
        paragraph = ' '.join(paragraph.split())

        # Wrap the paragraph at max_line_length
        words = paragraph.split()
        current_line = []
        current_length = indent_level  # Start with indent

        for word in words:
            word_length = len(word) + 1  # +1 for space

            # If adding this word exceeds max length, start a new line
            if current_length + word_length > max_line_length and current_line:
                indented_lines.append(" " * indent_level + ' '.join(current_line))
                current_line = [word]
                current_length = indent_level + len(word)
            else:
                current_line.append(word)
                current_length += word_length

        # Add the last line of this paragraph
        if current_line:
            indented_lines.append(" " * indent_level + ' '.join(current_line))

        # Add blank line between paragraphs (not after the last one)
        if idx < len(paragraphs) - 1:
            indented_lines.append("")

    return '\n'.join(indented_lines)


def extract_dataset_metadata(dataset: dict) -> dict:
    """
    Extract all required and optional fields from dataset dict.
    Handle missing fields gracefully with 'N/A' placeholders.
    """
    # Required fields with fallbacks
    title = dataset.get('title', 'Unknown Dataset')
    table_name = transform_title_to_snake_case(title)

    # Extract keywords/tags
    keywords = dataset.get('keyword', [])
    tags = transform_keywords_to_tags(keywords)

    # Extract description
    description = dataset.get('description', 'N/A')

    # Remove "Below is the list of tables" and everything after it
    if 'Below is the list of tables' in description:
        description = description.split('Below is the list of tables')[0].strip()

    # Extract contact point info
    contact_point = dataset.get('contactPoint', {})
    cms_category = contact_point.get('fn', 'N/A')
    contact_email = contact_point.get('hasEmail', 'N/A')
    contact = strip_mailto(contact_email)

    # Extract URLs
    data_source_url = dataset.get('landingPage', 'N/A')
    data_dictionary_url = dataset.get('describedBy', 'N/A')

    # Extract references (optional field)
    references = dataset.get('references', [])
    if references and len(references) > 0:
        data_methodology_url = references[0]
    else:
        data_methodology_url = 'N/A'
        if title != 'Unknown Dataset':
            logger.warning(f"Missing 'references' field for dataset: {title}")

    # Extract identifier and UUID
    identifier = dataset.get('identifier', 'N/A')
    sample_data_url = identifier  # Use the full identifier URL as sample_data_url
    dataset_uuid = extract_uuid_from_identifier(identifier)

    return {
        'table_name': table_name,
        'title': title,
        'description': description,
        'tags': tags,
        'cms_category': cms_category,
        'data_source_url': data_source_url,
        'data_dictionary_url': data_dictionary_url,
        'data_methodology_url': data_methodology_url,
        'sample_data_url': sample_data_url,
        'contact': contact,
        'dataset_uuid': dataset_uuid
    }


def generate_yml_content(metadata: dict) -> str:
    """
    Generate complete YML content for a single dataset.
    Follows the template structure with proper 2-space indentation.
    """
    # Start with static header
    yml_lines = [STATIC_HEADER]

    # Add table name (6 spaces indent)
    yml_lines.append(f"      - name: {metadata['table_name']}")

    # Add tags (8 spaces for 'tags:', 10 spaces for each tag)
    yml_lines.append("        tags:")
    for tag in metadata['tags']:
        yml_lines.append(f"          - {tag}")

    # Add description with pipe syntax (8 spaces for 'description:', 10 spaces for content)
    yml_lines.append("        description: |")
    formatted_description = format_multiline_description(metadata['description'], indent_level=10)
    yml_lines.append(formatted_description)

    # Add meta section (8 spaces for 'meta:', 10 spaces for fields)
    yml_lines.append("        meta:")
    yml_lines.append(f"          cms_category: {metadata['cms_category']}")
    yml_lines.append(f"          data_source_url: {metadata['data_source_url']}")
    yml_lines.append(f"          data_dictionary_url: {metadata['data_dictionary_url']}")
    yml_lines.append(f"          data_methodology_url: {metadata['data_methodology_url']}")
    yml_lines.append(f"          sample_data_url: {metadata['sample_data_url']}")
    yml_lines.append(f"          contact: {metadata['contact']}")
    yml_lines.append(f"          dataset_uuid: {metadata['dataset_uuid']}")

    # Add columns section if available (8 spaces for 'columns:', 10 spaces for '- name:', 12 spaces for 'type:')
    columns = load_columns_data(metadata['dataset_uuid'])
    if columns:
        columns_yaml = format_columns_yaml(columns, indent_level=8)
        yml_lines.append(columns_yaml)
        logger.debug(f"Added {len(columns)} columns for {metadata['table_name']}")

    return '\n'.join(yml_lines)


def write_yml_file(table_name: str, content: str, output_dir: Path) -> None:
    """
    Write YML content to file in the output directory.
    Filename pattern: cms_{table_name}_sources.yml
    """
    # Construct filename with cms_ prefix
    filename = f"cms_{table_name}_sources.yml"
    filepath = output_dir / filename

    # Write file with UTF-8 encoding
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.debug(f"Created: {filename}")
    except Exception as e:
        logger.error(f"Failed to write file {filename}: {e}")
        raise


def generate_all_yml_files(input_path: Path, output_dir: Path) -> None:
    """
    Main processing function.
    Reads input JSON, processes all datasets, and generates YML files.
    """
    # Load input JSON
    logger.info(f"Loading datasets from: {input_path}")
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        raise

    # Extract datasets array
    datasets = data.get('dataset', [])
    total_datasets = len(datasets)

    if total_datasets == 0:
        logger.warning("No datasets found in input JSON")
        return

    logger.info(f"Found {total_datasets} datasets to process")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Process each dataset
    success_count = 0
    for idx, dataset in enumerate(datasets, start=1):
        try:
            # Extract metadata
            metadata = extract_dataset_metadata(dataset)

            # Log progress
            logger.info(f"Processing {idx}/{total_datasets}: {metadata['title']}")

            # Generate YML content
            yml_content = generate_yml_content(metadata)

            # Write to file
            write_yml_file(metadata['table_name'], yml_content, output_dir)

            success_count += 1

        except Exception as e:
            logger.error(f"Error processing dataset {idx}: {e}")
            continue

    # Log summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Successfully generated {success_count}/{total_datasets} YML files")
    logger.info(f"Output location: {output_dir}")
    logger.info(f"{'='*60}")


def main():
    """
    CLI entry point.
    Sets up paths and runs the parser.

    Input:  <project_root>/input/data.json
    Output: <project_root>/models/sources/cms_{dataset_name}_sources.yml
    """
    # Set up paths relative to project root (one level above scripts/)
    project_root = Path(__file__).parent.parent
    input_path = project_root / "input" / "data.json"
    output_dir = project_root / "models" / "sources"

    # Validate input file exists
    if not input_path.exists():
        logger.error(f"Input file does not exist: {input_path}")
        logger.error("Please ensure input/data.json is present")
        return 1

    # Run generation
    try:
        generate_all_yml_files(input_path, output_dir)
        return 0
    except Exception as e:
        logger.error(f"Parser failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
