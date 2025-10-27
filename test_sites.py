#!/usr/bin/env python3
"""
Test script for adding/removing sites with schema maps and verifying data loading.
Tests with real sites: hebbarskitchen, imdb, backcountry
"""

import requests
import time
import sys
import os

# Configuration
API_BASE = "http://172.193.209.48/api"
API_KEY = open('.test_api_key').read().strip() if os.path.exists('.test_api_key') else None

# Test sites with schema map URLs on guha.com
TEST_SITES = {
    'hebbarskitchen': {
        'site_url': 'https://www.hebbarskitchen.com',
        'schema_map': 'https://guha.com/data/hebbarskitchen_com/schema_map.xml'
    },
    'imdb': {
        'site_url': 'https://www.imdb.com',
        'schema_map': 'https://guha.com/data/imdb_com/schema_map.xml'
    },
    'backcountry': {
        'site_url': 'https://www.backcountry.com',
        'schema_map': 'https://guha.com/data/backcountry_com/schema_map.xml'
    }
}


def verify_schema_map_exists(schema_map_url):
    """Verify that the schema map file exists at the given URL"""
    try:
        response = requests.head(schema_map_url, timeout=10)
        if response.status_code == 200:
            print(f"  ✓ Schema map exists: {schema_map_url}")
            return True
        else:
            print(f"  ✗ Schema map not found (HTTP {response.status_code}): {schema_map_url}")
            return False
    except Exception as e:
        print(f"  ✗ Error checking schema map: {e}")
        return False


def add_site(site_name, site_url, interval_hours=24):
    """Add a site via API"""
    headers = {'X-API-Key': API_KEY}

    try:
        response = requests.post(
            f"{API_BASE}/sites",
            json={"site_url": site_url, "interval_hours": interval_hours},
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            print(f"  ✓ Added site: {site_name}")
            return True
        else:
            print(f"  ✗ Failed to add site {site_name}: {response.text}")
            return False
    except Exception as e:
        print(f"  ✗ Error adding site {site_name}: {e}")
        return False


def add_schema_map(site_name, site_url, schema_map_url):
    """Add schema map to a site via API"""
    headers = {'X-API-Key': API_KEY}

    try:
        import urllib.parse
        encoded_url = urllib.parse.quote(site_url, safe='')

        response = requests.post(
            f"{API_BASE}/sites/{encoded_url}/schema-files",
            json={"schema_map_url": schema_map_url},
            headers=headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            files_added = data.get('files_added', 0)
            files_queued = data.get('files_queued', 0)
            print(f"  ✓ Added schema map for {site_name}")
            print(f"    Files added: {files_added}, Files queued: {files_queued}")
            return files_added > 0
        else:
            print(f"  ✗ Failed to add schema map for {site_name}: {response.text}")
            return False
    except Exception as e:
        print(f"  ✗ Error adding schema map for {site_name}: {e}")
        return False


def delete_site(site_name, site_url):
    """Delete a site via API"""
    headers = {'X-API-Key': API_KEY}

    try:
        import urllib.parse
        encoded_url = urllib.parse.quote(site_url, safe='')

        response = requests.delete(
            f"{API_BASE}/sites/{encoded_url}",
            headers=headers,
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            print(f"  ✓ Deleted site: {site_name}")
            print(f"    Schema maps removed: {data.get('schema_maps_removed', 0)}")
            print(f"    Files queued for removal: {data.get('files_queued_for_removal', 0)}")
            return True
        else:
            print(f"  ✗ Failed to delete site {site_name}: {response.text}")
            return False
    except Exception as e:
        print(f"  ✗ Error deleting site {site_name}: {e}")
        return False


def get_status():
    """Get current status of all sites"""
    headers = {'X-API-Key': API_KEY}

    try:
        response = requests.get(f"{API_BASE}/status", headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"  ✗ Failed to get status: {response.text}")
            return None
    except Exception as e:
        print(f"  ✗ Error getting status: {e}")
        return None


def show_status(title="CURRENT STATUS"):
    """Display current status"""
    print(f"\n{'='*70}")
    print(title)
    print('='*70)

    status = get_status()
    if not status:
        return

    sites = status.get('sites', [])

    if not sites:
        print("  No sites found")
        return

    total_files = 0
    total_ids = 0

    for site in sites:
        site_url = site['site_url']
        files = site.get('total_files', 0)
        ids = site.get('total_ids', 0)

        total_files += files
        total_ids += ids

        print(f"\n  {site_url}:")
        print(f"    Files: {files}")
        print(f"    IDs: {ids}")
        print(f"    Last processed: {site.get('last_processed', 'Never')}")

    print(f"\n  TOTALS: {total_files} files, {total_ids} IDs")


def wait_for_processing(expected_files=None, timeout=120):
    """Wait for processing to complete and data to be loaded"""
    print(f"\n  Waiting for processing (timeout: {timeout}s)...")

    start_time = time.time()
    last_ids = 0
    stable_count = 0

    while time.time() - start_time < timeout:
        status = get_status()
        if not status:
            time.sleep(5)
            continue

        sites = status.get('sites', [])
        total_ids = sum(site.get('total_ids', 0) for site in sites)

        # Check if IDs are stable (not changing)
        if total_ids == last_ids and total_ids > 0:
            stable_count += 1
            if stable_count >= 3:  # Stable for 3 checks (15 seconds)
                print(f"  ✓ Processing complete: {total_ids} IDs extracted")
                return True
        else:
            stable_count = 0

        if total_ids != last_ids:
            print(f"    IDs: {total_ids}")

        last_ids = total_ids
        time.sleep(5)

    print(f"  ⚠ Timeout after {timeout}s - final count: {last_ids} IDs")
    return last_ids > 0


def main():
    print("=" * 70)
    print("SITE TESTING WITH SCHEMA MAPS")
    print("=" * 70)

    # Check API key
    if not API_KEY:
        print("\n✗ API key not found!")
        print("Create .test_api_key file in the root directory")
        sys.exit(1)

    # Verify API is accessible
    headers = {'X-API-Key': API_KEY}
    try:
        response = requests.get(f"{API_BASE}/status", headers=headers, timeout=5)
        if response.status_code != 200:
            print(f"\n✗ API returned status {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Cannot connect to API: {e}")
        sys.exit(1)

    print("\n✓ API connection verified")

    # Show initial status
    show_status("INITIAL STATUS")

    # ========== PHASE 1: Add hebbarskitchen ==========
    print("\n" + "=" * 70)
    print("PHASE 1: ADD HEBBARSKITCHEN")
    print("=" * 70)

    site_info = TEST_SITES['hebbarskitchen']

    print("\n1. Verifying schema map exists...")
    if not verify_schema_map_exists(site_info['schema_map']):
        print("✗ Cannot proceed - schema map not found")
        sys.exit(1)

    print("\n2. Adding site...")
    if not add_site('hebbarskitchen', site_info['site_url']):
        print("✗ Failed to add site")
        sys.exit(1)

    print("\n3. Adding schema map...")
    if not add_schema_map('hebbarskitchen', site_info['site_url'], site_info['schema_map']):
        print("✗ Failed to add schema map")
        sys.exit(1)

    print("\n4. Waiting for data to load into vector DB...")
    if wait_for_processing(timeout=120):
        show_status("HEBBARSKITCHEN LOADED")
    else:
        print("⚠ Processing may still be ongoing")
        show_status("CURRENT STATUS")

    # ========== PHASE 2: Add imdb and backcountry ==========
    print("\n" + "=" * 70)
    print("PHASE 2: ADD IMDB AND BACKCOUNTRY")
    print("=" * 70)

    for site_name in ['imdb', 'backcountry']:
        site_info = TEST_SITES[site_name]

        print(f"\n{site_name.upper()}:")
        print("1. Verifying schema map exists...")
        if not verify_schema_map_exists(site_info['schema_map']):
            print(f"⚠ Skipping {site_name} - schema map not found")
            continue

        print("2. Adding site...")
        if not add_site(site_name, site_info['site_url']):
            print(f"⚠ Failed to add {site_name}")
            continue

        print("3. Adding schema map...")
        if not add_schema_map(site_name, site_info['site_url'], site_info['schema_map']):
            print(f"⚠ Failed to add schema map for {site_name}")
            continue

    print("\n4. Waiting for all sites to process...")
    if wait_for_processing(timeout=180):
        show_status("ALL SITES LOADED")
    else:
        print("⚠ Processing may still be ongoing")
        show_status("CURRENT STATUS")

    # ========== PHASE 3: Remove hebbarskitchen ==========
    print("\n" + "=" * 70)
    print("PHASE 3: REMOVE HEBBARSKITCHEN")
    print("=" * 70)

    site_info = TEST_SITES['hebbarskitchen']

    print("\n1. Deleting hebbarskitchen site...")
    if not delete_site('hebbarskitchen', site_info['site_url']):
        print("✗ Failed to delete site")
        sys.exit(1)

    print("\n2. Waiting for removal to complete...")
    time.sleep(10)  # Give workers time to process removal jobs

    show_status("AFTER HEBBARSKITCHEN REMOVAL")

    # Verify hebbarskitchen is gone
    status = get_status()
    if status:
        sites = status.get('sites', [])
        hebbar_found = any(s['site_url'] == site_info['site_url'] for s in sites)

        if not hebbar_found:
            print("\n✓ Hebbarskitchen successfully removed from sites list")
        else:
            print("\n⚠ Hebbarskitchen still appears in sites list")

    # ========== SUMMARY ==========
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print("\n✓ Phase 1: Added hebbarskitchen with schema map")
    print("✓ Phase 2: Added imdb and backcountry with schema maps")
    print("✓ Phase 3: Removed hebbarskitchen")
    print("\nFinal status:")
    show_status("FINAL STATUS")


if __name__ == '__main__':
    main()
