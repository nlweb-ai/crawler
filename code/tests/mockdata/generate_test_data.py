#!/usr/bin/env python3
"""
Generate test data from real schema.org JSONL files for testing the crawler system.
This script processes JSONL files, chunks them, and creates test directories with schema files.
"""

import json
import os
import sys
from urllib.parse import urlparse
from pathlib import Path
import xml.etree.ElementTree as ET

# Configuration
SOURCE_DIR = Path.home() / "mahi" / "data" / "sites" / "jsonl"
OUTPUT_DIR = Path(__file__).parent.parent.parent.parent / "data"  # crawler/data/
CHUNK_SIZE = 400

# Files to process
TARGET_FILES = [
    "backcountry_schemas.txt",
    "hebbarskitchen_schemas.txt",
    "imdb_schemas.txt",
    "seattle_schemas.txt"
]

def parse_jsonl_line(line):
    """Parse a single JSONL line to extract URL and schema data."""
    try:
        # Split by tab - format is: URL\tJSON
        parts = line.strip().split('\t', 1)
        if len(parts) != 2:
            return None, None

        url, json_str = parts

        # Parse the JSON - it's often an array of arrays
        schema_data = json.loads(json_str)

        # Flatten nested arrays if needed
        flattened = []
        if isinstance(schema_data, list):
            for item in schema_data:
                if isinstance(item, list):
                    flattened.extend(item)
                else:
                    flattened.append(item)
        else:
            flattened = [schema_data]

        # Add synthetic @id to items that don't have one
        items_with_id = []
        for idx, item in enumerate(flattened):
            if isinstance(item, dict):
                # If no @id, create one from URL and index
                if '@id' not in item:
                    # Create a synthetic ID from the page URL and item index
                    item['@id'] = f"{url}#schema-{idx}"
                items_with_id.append(item)

        return url, items_with_id
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing line: {e}")
        return None, None

def extract_site_from_url(url):
    """Extract site domain from URL."""
    try:
        parsed = urlparse(url)
        # Clean the domain for use as directory name
        domain = parsed.netloc.replace('www.', '')
        domain = domain.replace(':', '_').replace('.', '_')
        return domain
    except:
        return None

def process_file(file_path, output_dir):
    """Process a single JSONL file and generate chunked output."""
    print(f"\nProcessing {file_path.name}...")

    # Collect all schema items by site
    sites_data = {}
    line_count = 0
    items_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            if line_count % 1000 == 0:
                print(f"  Processed {line_count} lines, found {items_count} items with @id...")

            url, schema_items = parse_jsonl_line(line)
            if not url or not schema_items:
                continue

            site = extract_site_from_url(url)
            if not site:
                continue

            if site not in sites_data:
                sites_data[site] = []

            sites_data[site].extend(schema_items)
            items_count += len(schema_items)

    print(f"  Total: {line_count} lines processed, {items_count} items found")
    print(f"  Sites found: {list(sites_data.keys())}")

    # Process each site
    for site, items in sites_data.items():
        print(f"\n  Processing site: {site} ({len(items)} items)")

        # Create site directory
        site_dir = output_dir / site
        site_dir.mkdir(parents=True, exist_ok=True)

        # Chunk the items
        chunks = []
        for i in range(0, len(items), CHUNK_SIZE):
            chunk = items[i:i + CHUNK_SIZE]
            chunks.append(chunk)

        print(f"    Created {len(chunks)} chunks")

        # Write chunk files
        chunk_files = []
        for i, chunk in enumerate(chunks, 1):
            chunk_file = site_dir / f"{i}.json"
            with open(chunk_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=2, ensure_ascii=False)
            chunk_files.append(f"{i}.json")
            print(f"    Wrote {chunk_file.name} ({len(chunk)} items)")

        # Create schema_map.xml with first 2 files
        create_schema_map(site_dir, chunk_files[:2], site)

    return sites_data.keys()

def create_schema_map(site_dir, files_to_list, site_domain, num_files=2):
    """Create a schema_map.xml file listing the schema files.

    Args:
        site_dir: Directory for the site
        files_to_list: List of available files
        site_domain: Domain name for the site
        num_files: Number of files to include in schema_map (default 2)
    """

    # Create the root element
    urlset = ET.Element('urlset', xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    # Add URL entries for each file
    base_url = f"http://localhost:8000/{site_domain}/"

    # Only include up to num_files in the schema_map
    for file_name in files_to_list[:num_files]:
        url = ET.SubElement(urlset, 'url')
        url.set('contentType', 'structuredData/schema.org')
        loc = ET.SubElement(url, 'loc')
        loc.text = base_url + file_name

    # Write XML file with pretty formatting
    # ET.indent is available in Python 3.9+
    if hasattr(ET, 'indent'):
        ET.indent(urlset, space="  ")  # This adds proper indentation

    tree = ET.ElementTree(urlset)
    schema_map_file = site_dir / "schema_map.xml"

    with open(schema_map_file, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        tree.write(f, encoding='utf-8', xml_declaration=False)
        f.write(b'\n')  # Add final newline

    print(f"    Created schema_map.xml listing {min(len(files_to_list), num_files)} files")

def main():
    """Main function to process all target files."""
    print(f"Starting test data generation...")
    print(f"Source directory: {SOURCE_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Chunk size: {CHUNK_SIZE} items")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_sites = set()

    # Process each target file
    for file_name in TARGET_FILES:
        file_path = SOURCE_DIR / file_name

        if not file_path.exists():
            print(f"\nWarning: {file_name} not found, skipping...")
            continue

        # Get file size
        size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"\n{'='*60}")
        print(f"File: {file_name} ({size_mb:.1f} MB)")

        sites = process_file(file_path, OUTPUT_DIR)
        all_sites.update(sites)

    # Create a summary file
    summary_file = OUTPUT_DIR / "test_sites.json"
    summary = {
        "sites": sorted(list(all_sites)),
        "base_url": "http://localhost:8000/",
        "instructions": "Run 'python3 -m http.server 8000' in the data/ directory to serve these files"
    }

    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Test data generation complete!")
    print(f"Created data for {len(all_sites)} sites:")
    for site in sorted(all_sites):
        print(f"  - {site}")
    print(f"\nTo serve the test data:")
    print(f"  cd {OUTPUT_DIR}")
    print(f"  python3 -m http.server 8000")
    print(f"\nThen add sites to crawler with URLs like:")
    print(f"  http://localhost:8000/<site_name>/schema_map.xml")

if __name__ == "__main__":
    main()