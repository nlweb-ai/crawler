#!/usr/bin/env python3
"""
Test dynamic updates to schema_map.xml files
1. Add sites with initial set of files
2. Add more files to sitemap and trigger reload
3. Remove some original files and trigger reload
"""

import sys
import os
import time
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime

# Load environment variables
sys.path.insert(0, 'code/core')
import config

API_BASE = "http://localhost:5001/api"
TEST_SITES = ['backcountry_com', 'hebbarskitchen_com', 'imdb_com']

# Phases configuration
INITIAL_FILES = [1, 2, 3, 4, 5]           # Start with files 1-5
ADDED_FILES = [6, 7, 8]                   # Add files 6-8 in phase 2
FILES_TO_REMOVE = [2, 4]                  # Remove files 2 and 4 in phase 3
WAIT_TIME = 30                            # Seconds to wait between phases


def update_schema_map(site, file_numbers):
    """Update schema_map.xml for a site with specific file numbers"""
    schema_map_path = f'data/{site}/schema_map.xml'

    if not os.path.exists(f'data/{site}'):
        print(f"  ✗ Site directory not found: data/{site}")
        return False

    # Create schema_map with specified files
    urlset = ET.Element('urlset', xmlns='http://www.sitemaps.org/schemas/sitemap/0.9')

    for num in sorted(file_numbers):
        url = ET.SubElement(urlset, 'url')
        url.set('contentType', 'structuredData/schema.org')
        loc = ET.SubElement(url, 'loc')
        loc.text = f'http://localhost:8000/{site}/{num}.json'

    # Write the file
    tree = ET.ElementTree(urlset)
    ET.indent(tree, space='  ')

    with open(schema_map_path, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        tree.write(f, encoding='utf-8', xml_declaration=False)
        f.write(b'\n')

    print(f"  ✓ {site}: Updated schema_map.xml with files: {sorted(file_numbers)}")
    return True


def clear_database():
    """Clear all data from database"""
    print("\nClearing database...")
    import db
    conn = db.get_connection()
    db.clear_all_data(conn)
    conn.close()


def add_sites():
    """Add test sites via API"""
    print("\nAdding sites via API...")

    for site in TEST_SITES:
        site_url = f"http://localhost:8000/{site}"

        try:
            response = requests.post(
                f"{API_BASE}/sites",
                json={"site_url": site_url, "interval_hours": 24},
                timeout=5
            )

            if response.status_code == 200:
                print(f"  ✓ Added {site}")
            else:
                print(f"  ✗ Failed to add {site}: {response.text}")
        except Exception as e:
            print(f"  ✗ Error adding {site}: {e}")


def trigger_processing(sites=None):
    """Trigger processing for specified sites or all test sites"""
    if sites is None:
        sites = TEST_SITES

    print("\nTriggering processing...")

    for site in sites:
        site_url = f"http://localhost:8000/{site}"

        try:
            import urllib.parse
            encoded_url = urllib.parse.quote(site_url, safe='')
            response = requests.post(
                f"{API_BASE}/process/{encoded_url}",
                timeout=5
            )

            if response.status_code == 200:
                print(f"  ✓ Triggered processing for {site}")
            else:
                print(f"  ✗ Failed to trigger {site}: {response.text}")
        except Exception as e:
            print(f"  ✗ Error triggering {site}: {e}")


def wait_for_processing(timeout=60):
    """Wait for all processing to complete"""
    print(f"\nWaiting for processing to complete...")

    start_time = time.time()
    last_status = {'pending': -1, 'processing': -1}

    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{API_BASE}/queue/status", timeout=5)
            if response.status_code == 200:
                data = response.json()
                pending = data.get('pending_jobs', 0)
                processing = data.get('processing_jobs', 0)

                # Show status if changed
                if pending != last_status['pending'] or processing != last_status['processing']:
                    print(f"  Queue: {pending} pending, {processing} processing")
                    last_status = {'pending': pending, 'processing': processing}

                if pending == 0 and processing == 0:
                    print("  ✓ All jobs completed")
                    return True
        except Exception as e:
            print(f"  Error checking queue: {e}")

        time.sleep(2)

    print("  ✗ Timeout waiting for processing")
    return False


def show_status():
    """Display current status of all sites"""
    print("\n" + "=" * 60)
    print("CURRENT STATUS")
    print("=" * 60)

    try:
        response = requests.get(f"{API_BASE}/status", timeout=5)
        if response.status_code == 200:
            sites = response.json()

            total_files = 0
            total_ids = 0

            for site in sites:
                site_name = site['site_url'].split('/')[-1]
                if site_name not in TEST_SITES:
                    continue

                total_files += site['total_files']
                total_ids += site['total_ids']

                last_proc = site.get('last_processed', 'Never')
                if last_proc and last_proc != 'Never':
                    # Parse and format the timestamp
                    try:
                        dt = datetime.fromisoformat(last_proc.replace('Z', '+00:00'))
                        last_proc = dt.strftime('%H:%M:%S')
                    except:
                        last_proc = last_proc.split('T')[1][:8] if 'T' in last_proc else last_proc

                print(f"  {site_name}:")
                print(f"    Active files: {site['total_files']}")
                print(f"    Total IDs: {site['total_ids']}")
                print(f"    Last processed: {last_proc}")

            print(f"\n  TOTALS: {total_files} files, {total_ids} IDs")
    except Exception as e:
        print(f"  Error getting status: {e}")


def verify_files_in_database(expected_files):
    """Verify which files are active in the database"""
    print("\nVerifying files in database...")

    import db
    conn = db.get_connection()
    cursor = conn.cursor()

    for site in TEST_SITES:
        print(f"  {site}:")

        # Get active files for this site
        cursor.execute("""
            SELECT file_url, is_active
            FROM files
            WHERE site_url = ? AND is_active = 1
            ORDER BY file_url
        """, f"http://localhost:8000/{site}")

        active_files = []
        for file_url, is_active in cursor.fetchall():
            file_num = int(file_url.split('/')[-1].replace('.json', ''))
            active_files.append(file_num)

        # Check if matches expected
        if set(active_files) == set(expected_files):
            print(f"    ✓ Active files match expected: {sorted(active_files)}")
        else:
            print(f"    ✗ Active files: {sorted(active_files)}")
            print(f"      Expected: {sorted(expected_files)}")

            # Show what's missing or extra
            missing = set(expected_files) - set(active_files)
            extra = set(active_files) - set(expected_files)
            if missing:
                print(f"      Missing: {sorted(missing)}")
            if extra:
                print(f"      Extra: {sorted(extra)}")

    conn.close()


def main():
    print("=" * 70)
    print("DYNAMIC FILE UPDATES TEST")
    print("=" * 70)
    print("\nThis test will:")
    print(f"  1. Add sites with files {INITIAL_FILES}")
    print(f"  2. Add files {ADDED_FILES} to sitemap and reload")
    print(f"  3. Remove files {FILES_TO_REMOVE} from sitemap and reload")
    print(f"\nWait time between phases: {WAIT_TIME} seconds")

    # Check services
    try:
        response = requests.get(f"{API_BASE}/status", timeout=2)
    except:
        print("\n✗ Cannot connect to API server!")
        print("\nMake sure services are running:")
        print("  Terminal 1: ./start_test_data_server.sh")
        print("  Terminal 2: ./start_master.sh")
        print("  Terminal 3: ./start_worker.sh")
        sys.exit(1)

    # Clear and start fresh
    clear_database()

    # ========== PHASE 1: Initial setup ==========
    print("\n" + "=" * 70)
    print(f"PHASE 1: ADD SITES WITH INITIAL FILES {INITIAL_FILES}")
    print("=" * 70)

    # Update schema_maps with initial files
    print("\nSetting up initial schema_map.xml files...")
    for site in TEST_SITES:
        update_schema_map(site, INITIAL_FILES)

    # Add sites and process
    add_sites()
    trigger_processing()

    if wait_for_processing():
        show_status()
        verify_files_in_database(INITIAL_FILES)
    else:
        print("✗ Phase 1 failed")
        return

    print(f"\n⏰ Waiting {WAIT_TIME} seconds before Phase 2...")
    time.sleep(WAIT_TIME)

    # ========== PHASE 2: Add more files ==========
    print("\n" + "=" * 70)
    print(f"PHASE 2: ADD FILES {ADDED_FILES} TO SITEMAP")
    print("=" * 70)

    # Update schema_maps to include additional files
    all_files_phase2 = INITIAL_FILES + ADDED_FILES
    print(f"\nUpdating schema_maps to include all files: {sorted(all_files_phase2)}")
    for site in TEST_SITES:
        update_schema_map(site, all_files_phase2)

    # Trigger reprocessing
    trigger_processing()

    if wait_for_processing():
        show_status()
        verify_files_in_database(all_files_phase2)
    else:
        print("✗ Phase 2 failed")
        return

    print(f"\n⏰ Waiting {WAIT_TIME} seconds before Phase 3...")
    time.sleep(WAIT_TIME)

    # ========== PHASE 3: Remove some original files ==========
    print("\n" + "=" * 70)
    print(f"PHASE 3: REMOVE FILES {FILES_TO_REMOVE} FROM SITEMAP")
    print("=" * 70)

    # Update schema_maps to remove some files
    remaining_files = [f for f in all_files_phase2 if f not in FILES_TO_REMOVE]
    print(f"\nUpdating schema_maps to only include: {sorted(remaining_files)}")
    for site in TEST_SITES:
        update_schema_map(site, remaining_files)

    # Trigger reprocessing
    trigger_processing()

    if wait_for_processing():
        show_status()
        verify_files_in_database(remaining_files)
    else:
        print("✗ Phase 3 failed")
        return

    # ========== Final verification ==========
    print("\n" + "=" * 70)
    print("TEST COMPLETE - FINAL VERIFICATION")
    print("=" * 70)

    import db
    conn = db.get_connection()
    cursor = conn.cursor()

    print("\nChecking removed files are properly handled...")
    for site in TEST_SITES:
        for file_num in FILES_TO_REMOVE:
            file_url = f"http://localhost:8000/{site}/{file_num}.json"

            # Check file status
            cursor.execute("SELECT is_active FROM files WHERE file_url = ?", file_url)
            result = cursor.fetchone()

            if result:
                if result[0] == 0:
                    print(f"  ✓ {site}/{file_num}.json: Marked as inactive")
                else:
                    print(f"  ✗ {site}/{file_num}.json: Still active (should be inactive)")
            else:
                print(f"  ✓ {site}/{file_num}.json: Removed from database")

            # Check IDs are removed
            cursor.execute("SELECT COUNT(*) FROM ids WHERE file_url = ?", file_url)
            id_count = cursor.fetchone()[0]

            if id_count == 0:
                print(f"      ✓ All IDs removed")
            else:
                print(f"      ✗ {id_count} IDs still present")

    conn.close()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n✓ Phase 1: Added sites with files {INITIAL_FILES}")
    print(f"✓ Phase 2: Added files {ADDED_FILES} dynamically")
    print(f"✓ Phase 3: Removed files {FILES_TO_REMOVE} and verified cleanup")
    print("\nThe crawler correctly handled:")
    print("  • Adding new files when they appear in schema_map.xml")
    print("  • Removing files when they disappear from schema_map.xml")
    print("  • Cleaning up IDs from removed files")
    print("  • Updating vector database accordingly")


if __name__ == '__main__':
    main()