#!/usr/bin/env python3
"""
Script to add IMDB data to production using the schema-files API.
Uses the finer-grained API endpoint to add schema_map.xml files from guha.com/data/
"""

import requests
import sys
from urllib.parse import quote

# Production configuration
API_BASE = "https://testing.nlweb.ai/api"
API_KEY = "tqYiq7xqxSb-iC5WPHi1ek341XCKhl4HLhN5OK9mPaUPBZfQykMoTbQX4jrQddr4"

# IMDB data
SITE_URL = "https://www.imdb.com"
SCHEMA_MAP_URL = "https://guha.com/data/imdb_com/schema_map.xml"

def add_schema_map_to_site(site_url, schema_map_url, api_key):
    """Add a schema map to a site using the schema-files API"""

    # URL encode the site_url for the API path
    encoded_site = quote(site_url, safe='')

    # Construct the API endpoint
    endpoint = f"{API_BASE}/sites/{encoded_site}/schema-files"

    print(f"Adding schema map to site...")
    print(f"  Site: {site_url}")
    print(f"  Schema Map: {schema_map_url}")
    print(f"  Endpoint: {endpoint}")

    try:
        response = requests.post(
            endpoint,
            headers={
                'X-API-Key': api_key,
                'Content-Type': 'application/json'
            },
            json={
                'schema_map_url': schema_map_url
            }
        )

        if response.status_code == 200:
            data = response.json()
            print(f"\n✓ Success!")
            print(f"  Files added: {data.get('files_added', 0)}")
            print(f"  Files queued for processing: {data.get('files_queued', 0)}")
            return True
        else:
            print(f"\n✗ Failed: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

def check_site_status(site_url, api_key):
    """Check the status of a site"""
    encoded_site = quote(site_url, safe='')
    endpoint = f"{API_BASE}/sites/{encoded_site}"

    print(f"\nChecking site status...")

    try:
        response = requests.get(
            endpoint,
            headers={'X-API-Key': api_key}
        )

        if response.status_code == 200:
            data = response.json()
            print(f"\nSite Status:")
            print(f"  Site URL: {data.get('site_url')}")
            print(f"  Schema Maps: {data.get('schema_map_count', 0)}")
            print(f"  Total Files: {data.get('total_files', 0)}")
            print(f"  Total IDs: {data.get('total_ids', 0)}")
            print(f"  Last Processed: {data.get('last_processed', 'Never')}")
            return True
        else:
            print(f"Failed to get status: HTTP {response.status_code}")
            return False

    except Exception as e:
        print(f"Error checking status: {e}")
        return False

def main():
    print("=" * 60)
    print("ADD IMDB DATA TO PRODUCTION")
    print("=" * 60)

    # Add the schema map
    success = add_schema_map_to_site(SITE_URL, SCHEMA_MAP_URL, API_KEY)

    if success:
        # Check the site status
        check_site_status(SITE_URL, API_KEY)

        print("\n" + "=" * 60)
        print("MONITORING")
        print("=" * 60)
        print("\nYou can monitor progress at:")
        print(f"  Web UI: https://testing.nlweb.ai/")
        print(f"  Profile: https://testing.nlweb.ai/profile")
        print(f"  Site Details: https://testing.nlweb.ai/site-details.html?site={quote(SITE_URL, safe='')}")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
