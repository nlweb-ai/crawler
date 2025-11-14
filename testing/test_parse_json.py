#!/usr/bin/env python3
"""
Test script to fetch and parse a schema.org JSON file
"""

import requests
import json

URL = "https://guha.com/data/backcountry_com/1.json"

print(f"Fetching: {URL}")
print("=" * 80)

response = requests.get(URL)
print(f"Status Code: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type')}")
print()

if response.status_code == 200:
    try:
        data = response.json()

        print("JSON Structure:")
        print(f"Type: {type(data)}")

        if isinstance(data, list):
            print(f"Array with {len(data)} items")
            print()

            # Extract @id values
            ids_found = []
            for i, item in enumerate(data):
                if isinstance(item, dict) and '@id' in item:
                    ids_found.append(item['@id'])
                    if i < 5:  # Show first 5
                        print(f"Item {i}: @id = {item['@id']}")
                        print(f"  @type = {item.get('@type', 'N/A')}")

            print()
            print(f"Total @id values found: {len(ids_found)}")

            if ids_found:
                print()
                print("First 10 @id values:")
                for id_val in ids_found[:10]:
                    print(f"  - {id_val}")

        elif isinstance(data, dict):
            print("Single object")
            if '@id' in data:
                print(f"@id: {data['@id']}")
            if '@graph' in data:
                print(f"Has @graph with {len(data['@graph'])} items")
                ids_found = [item.get('@id') for item in data['@graph'] if '@id' in item]
                print(f"Total @id values in @graph: {len(ids_found)}")
                if ids_found:
                    print()
                    print("First 10 @id values:")
                    for id_val in ids_found[:10]:
                        print(f"  - {id_val}")

        # Show raw JSON (first 1000 chars)
        print()
        print("Raw JSON (first 1000 characters):")
        print("=" * 80)
        print(json.dumps(data, indent=2)[:1000])

    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        print()
        print("Raw content (first 1000 characters):")
        print(response.text[:1000])
else:
    print(f"Failed to fetch: {response.status_code}")
