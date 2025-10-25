#!/usr/bin/env python3
"""
Test file removal scenario:
1. Update schema_map.xml files to include 10 files
2. Load sites and let crawler process them
3. Update schema_map.xml files to remove 3 files from each
4. Trigger reprocessing to verify database updates
"""

import sys
import os
import time
import xml.etree.ElementTree as ET
import requests
import subprocess

sys.path.insert(0, 'code/core')
import config
import db

# Test sites
TEST_SITES = ['backcountry_com', 'hebbarskitchen_com', 'imdb_com', 'seattle_gov']
DATA_DIR = 'data'
API_BASE = "http://localhost:5001/api"


def update_schema_maps(num_files=10):
    """Update all schema_map.xml files to include specified number of files"""
    print(f"\nUpdating schema_map.xml files to include {num_files} files each...")

    for site in TEST_SITES:
        site_dir = os.path.join(DATA_DIR, site)
        schema_map_path = os.path.join(site_dir, 'schema_map.xml')

        if not os.path.exists(site_dir):
            print(f"  ✗ Site directory not found: {site_dir}")
            continue

        # Count available JSON files
        json_files = sorted([f for f in os.listdir(site_dir) if f.endswith('.json')])
        available = len(json_files)

        # Create new schema_map.xml
        urlset = ET.Element('urlset', xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
        base_url = f"http://localhost:8000/{site}/"

        # Add up to num_files entries
        files_to_add = min(num_files, available)
        for i in range(1, files_to_add + 1):
            url = ET.SubElement(urlset, 'url')
            url.set('contentType', 'structuredData/schema.org')
            loc = ET.SubElement(url, 'loc')
            loc.text = f"{base_url}{i}.json"

        # Write the file with proper formatting
        tree = ET.ElementTree(urlset)
        ET.indent(tree, space='  ')

        with open(schema_map_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)
            f.write(b'\n')

        print(f"  ✓ {site}: Updated to include {files_to_add} files (of {available} available)")


def clear_all_data():
    """Clear database and queue"""
    print("\nClearing all existing data...")

    # Clear database
    conn = db.get_connection()
    db.clear_all_data(conn)
    conn.close()

    # Clear queue
    queue_dir = os.getenv('QUEUE_DIR', 'queue')
    if os.path.exists(queue_dir):
        for f in os.listdir(queue_dir):
            if f.endswith('.json') or f.endswith('.processing'):
                os.remove(os.path.join(queue_dir, f))

    print("  ✓ Database and queue cleared")


def add_and_process_sites():
    """Add test sites and trigger processing"""
    print("\nAdding sites and triggering processing...")

    for site in TEST_SITES:
        site_url = f"http://localhost:8000/{site}"

        # Add site
        response = requests.post(
            f"{API_BASE}/sites",
            json={"site_url": site_url, "interval_hours": 24}
        )
        if response.status_code == 200:
            print(f"  ✓ Added {site}")

        # Trigger processing
        import urllib.parse
        encoded_url = urllib.parse.quote(site_url, safe='')
        response = requests.post(f"{API_BASE}/process/{encoded_url}")
        if response.status_code == 200:
            print(f"  ✓ Triggered processing for {site}")


def wait_for_processing(expected_files_per_site=10, timeout=60):
    """Wait for initial processing to complete"""
    print(f"\nWaiting for processing (expecting {expected_files_per_site} files per site)...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check queue status
        response = requests.get(f"{API_BASE}/queue/status")
        if response.status_code == 200:
            data = response.json()
            pending = data.get('pending_jobs', 0)
            processing = data.get('processing_jobs', 0)

            if pending == 0 and processing == 0:
                print("  ✓ All jobs completed")
                return True

        time.sleep(2)

    print("  ✗ Timeout waiting for processing")
    return False


def check_database_state(expected_files_per_site=10):
    """Check current database state and return statistics"""
    print(f"\nChecking database state (expecting {expected_files_per_site} files per site)...")

    conn = db.get_connection()
    cursor = conn.cursor()

    # Get detailed statistics
    cursor.execute("""
        SELECT
            s.site_url,
            COUNT(DISTINCT f.file_url) as file_count,
            COUNT(DISTINCT i.id) as id_count
        FROM sites s
        LEFT JOIN files f ON s.site_url = f.site_url AND f.is_active = 1
        LEFT JOIN ids i ON f.file_url = i.file_url
        GROUP BY s.site_url
        ORDER BY s.site_url
    """)

    results = cursor.fetchall()

    print("\n  Current state:")
    total_files = 0
    total_ids = 0

    for site_url, file_count, id_count in results:
        site_name = site_url.split('/')[-1]
        total_files += file_count
        total_ids += id_count
        status = "✓" if file_count == expected_files_per_site else "✗"
        print(f"    {status} {site_name}: {file_count} files, {id_count} IDs")

    print(f"\n  Totals: {total_files} files, {total_ids} IDs")

    conn.close()
    return total_files, total_ids


def remove_files_from_schema_maps(files_to_remove=3):
    """Remove specified number of files from each schema_map.xml"""
    print(f"\nRemoving {files_to_remove} files from each schema_map.xml...")

    removed_files = {}

    for site in TEST_SITES:
        site_dir = os.path.join(DATA_DIR, site)
        schema_map_path = os.path.join(site_dir, 'schema_map.xml')

        if not os.path.exists(schema_map_path):
            continue

        # Parse existing schema_map
        tree = ET.parse(schema_map_path)
        root = tree.getroot()

        # Find all URL elements
        namespace = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = root.findall('sitemap:url', namespace) or root.findall('url')

        # Track which files we're removing
        site_removed = []

        # Remove the last 'files_to_remove' entries
        for i in range(min(files_to_remove, len(urls))):
            url_to_remove = urls[-(i+1)]
            loc = url_to_remove.find('sitemap:loc', namespace) or url_to_remove.find('loc')
            if loc is not None:
                file_url = loc.text
                file_name = file_url.split('/')[-1]
                site_removed.append(file_name)
            root.remove(url_to_remove)

        removed_files[site] = site_removed

        # Write updated schema_map
        ET.indent(tree, space='  ')
        with open(schema_map_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)
            f.write(b'\n')

        remaining = len(root.findall('sitemap:url', namespace) or root.findall('url'))
        print(f"  ✓ {site}: Removed {len(site_removed)} files, {remaining} remaining")
        if site_removed:
            print(f"      Removed: {', '.join(site_removed)}")

    return removed_files


def trigger_reprocessing():
    """Trigger reprocessing for all sites"""
    print("\nTriggering reprocessing after file removal...")

    for site in TEST_SITES:
        site_url = f"http://localhost:8000/{site}"
        import urllib.parse
        encoded_url = urllib.parse.quote(site_url, safe='')
        response = requests.post(f"{API_BASE}/process/{encoded_url}")
        if response.status_code == 200:
            print(f"  ✓ Triggered reprocessing for {site}")


def verify_removed_files(removed_files):
    """Verify that removed files are marked as inactive in database"""
    print("\nVerifying removed files are marked as inactive...")

    conn = db.get_connection()
    cursor = conn.cursor()

    all_correct = True

    for site, files in removed_files.items():
        site_url = f"http://localhost:8000/{site}"

        for file_name in files:
            file_url = f"{site_url}/{file_name}"

            # Check if file is marked as inactive
            cursor.execute("""
                SELECT is_active
                FROM files
                WHERE file_url = ?
            """, file_url)

            result = cursor.fetchone()
            if result and result[0] == 0:
                print(f"  ✓ {site}/{file_name}: Correctly marked as inactive")
            else:
                print(f"  ✗ {site}/{file_name}: Still active or missing!")
                all_correct = False

            # Check that IDs are removed
            cursor.execute("""
                SELECT COUNT(*)
                FROM ids
                WHERE file_url = ?
            """, file_url)

            count = cursor.fetchone()[0]
            if count == 0:
                print(f"      ✓ IDs removed (0 remaining)")
            else:
                print(f"      ✗ {count} IDs still present!")
                all_correct = False

    conn.close()
    return all_correct


def main():
    print("=" * 70)
    print("FILE REMOVAL TEST")
    print("=" * 70)
    print("\nThis test will:")
    print("  1. Update schema_map.xml files to include 10 files each")
    print("  2. Load and process all sites")
    print("  3. Remove 3 files from each schema_map.xml")
    print("  4. Verify database correctly updates")

    # Phase 1: Setup with 10 files
    print("\n" + "=" * 70)
    print("PHASE 1: SETUP WITH 10 FILES")
    print("=" * 70)

    update_schema_maps(num_files=10)
    clear_all_data()
    add_and_process_sites()

    if wait_for_processing(expected_files_per_site=10):
        initial_files, initial_ids = check_database_state(expected_files_per_site=10)
        print(f"\n✓ Phase 1 complete: {initial_files} files, {initial_ids} IDs")
    else:
        print("\n✗ Phase 1 failed: Processing timeout")
        return

    # Phase 2: Remove files and reprocess
    print("\n" + "=" * 70)
    print("PHASE 2: REMOVE 3 FILES FROM EACH SITE")
    print("=" * 70)

    removed_files = remove_files_from_schema_maps(files_to_remove=3)
    trigger_reprocessing()

    if wait_for_processing(expected_files_per_site=7):
        final_files, final_ids = check_database_state(expected_files_per_site=7)
        print(f"\n✓ Phase 2 complete: {final_files} files, {final_ids} IDs")
    else:
        print("\n✗ Phase 2 failed: Processing timeout")
        return

    # Phase 3: Verify
    print("\n" + "=" * 70)
    print("PHASE 3: VERIFICATION")
    print("=" * 70)

    if verify_removed_files(removed_files):
        print("\n✓ All removed files correctly marked as inactive")
    else:
        print("\n✗ Some files not correctly updated")

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    print(f"\nInitial state: {initial_files} files, {initial_ids} IDs")
    print(f"Final state:   {final_files} files, {final_ids} IDs")
    print(f"Difference:    {initial_files - final_files} files removed, {initial_ids - final_ids} IDs removed")

    expected_files_removed = len(TEST_SITES) * 3
    if (initial_files - final_files) == expected_files_removed:
        print(f"\n✓ TEST PASSED: Correctly removed {expected_files_removed} files")
    else:
        print(f"\n✗ TEST FAILED: Expected to remove {expected_files_removed} files, but removed {initial_files - final_files}")


if __name__ == '__main__':
    # Check if services are running
    try:
        response = requests.get(f"{API_BASE}/status", timeout=2)
        if response.status_code != 200:
            print("Error: API server not running. Start services first.")
            sys.exit(1)
    except:
        print("Error: Cannot connect to API server. Start services first with:")
        print("  Terminal 1: python3 launch_test.py master")
        print("  Terminal 2: python3 launch_test.py worker")
        sys.exit(1)

    main()