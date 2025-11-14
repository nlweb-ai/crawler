#!/usr/bin/env python3
"""Clear all database tables"""

import sys
import os
import requests
from dotenv import load_dotenv

# Configuration
API_URL = 'https://testing.nlweb.ai'
api_key = 'tqYiq7xqxSb-iC5WPHi1ek341XCKhl4HLhN5OK9mPaUPBZfQykMoTbQX4jrQddr4'

headers = {
    'X-API-Key': api_key,
    'Content-Type': 'application/json'
}

# Delete all sites (which should cascade delete files and ids)
print("Getting all sites...")
response = requests.get(f'{API_URL}/api/sites', headers=headers)
sites = response.json()

print(f"Found {len(sites)} sites")
for site in sites:
    site_url = site['site']
    print(f"Deleting site: {site_url}")
    response = requests.delete(f'{API_URL}/api/sites/{site_url}', headers=headers)
    if response.status_code == 200:
        print(f"  ✓ Deleted {site_url}")
    else:
        print(f"  ✗ Failed to delete {site_url}: {response.status_code} - {response.text}")

print("\nDone clearing databases")
