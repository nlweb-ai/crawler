#!/usr/bin/env python3
"""
Script to add three sites to production: IMDB, Hebbarskitchen, and Backcountry
"""

import requests
import sys
from urllib.parse import quote

# Production configuration
API_BASE = "https://testing.nlweb.ai/api"
API_KEY = "tqYiq7xqxSb-iC5WPHi1ek341XCKhl4HLhN5OK9mPaUPBZfQykMoTbQX4jrQddr4"

# Sites to add
SITES = [
    {
        'name': 'IMDB',
        'site_url': 'imdb.com',
        'schema_map_url': 'https://guha.com/data/imdb_com/schema_map.xml'
    },
    {
        'name': 'Hebbarskitchen',
        'site_url': 'hebbarskitchen.com',
        'schema_map_url': 'https://guha.com/data/hebbarskitchen_com/schema_map.xml'
    },
    {
        'name': 'Backcountry',
        'site_url': 'backcountry.com',
        'schema_map_url': 'https://guha.com/data/backcountry_com/schema_map.xml'
    }
]

def add_schema_map_to_site(site_name, site_url, schema_map_url, api_key):
    """Add a schema map to a site using the schema-files API"""

    # URL encode the site_url for the API path
    encoded_site = quote(site_url, safe='')

    # Construct the API endpoint
    endpoint = f"{API_BASE}/sites/{encoded_site}/schema-files"

    print(f"\nAdding {site_name}...")
    print(f"  Site: {site_url}")
    print(f"  Schema Map: {schema_map_url}")

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
            print(f"  ✓ Success!")
            print(f"    Files added: {data.get('files_added', 0)}")
            print(f"    Files queued: {data.get('files_queued', 0)}")
            return True
        else:
            print(f"  ✗ Failed: HTTP {response.status_code}")
            print(f"    Response: {response.text}")
            return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def main():
    print("=" * 60)
    print("ADD THREE SITES TO PRODUCTION")
    print("=" * 60)

    success_count = 0
    for site in SITES:
        if add_schema_map_to_site(site['name'], site['site_url'], site['schema_map_url'], API_KEY):
            success_count += 1

    print("\n" + "=" * 60)
    print(f"SUMMARY: {success_count}/{len(SITES)} sites added successfully")
    print("=" * 60)
    print("\nMonitor progress at:")
    print("  https://testing.nlweb.ai/profile")

    return 0 if success_count == len(SITES) else 1

if __name__ == "__main__":
    sys.exit(main())
