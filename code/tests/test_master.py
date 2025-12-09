#!/usr/bin/env python3
"""Test script for master.py with local test data"""

import sys
import os

from ..core import master

def test_schema_map_parsing():
    """Test parsing of our generated schema_map.xml files"""

    # Test with a local server URL
    test_sites = [
        "http://localhost:8000/hebbarskitchen_com",
        "http://localhost:8000/hebbarskitchen_com/schema_map.xml",
        "http://localhost:8000/imdb_com"
    ]

    print("Testing schema URL extraction:\n")

    for site_url in test_sites:
        print(f"Testing: {site_url}")
        urls = master.get_schema_urls_from_robots(site_url)

        if urls:
            print(f"  Found {len(urls)} schema files:")
            for url in urls:
                print(f"    - {url}")
        else:
            print(f"  No schema files found")
        print()

def test_xml_parsing_directly():
    """Test XML parsing with actual content from our files"""

    xml_content = """<?xml version="1.0" encoding="utf-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url contentType="structuredData/schema.org">
    <loc>http://localhost:8000/test_site/1.json</loc>
  </url>
  <url contentType="structuredData/schema.org">
    <loc>http://localhost:8000/test_site/2.json</loc>
  </url>
</urlset>"""

    print("Testing direct XML parsing:")
    urls = master.parse_schema_map_xml(xml_content, "http://localhost:8000/test_site/")
    print(f"Found {len(urls)} URLs:")
    for url in urls:
        print(f"  - {url}")
    print()

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Master.py with Schema Map Support")
    print("=" * 60)
    print()

    # Test direct XML parsing
    test_xml_parsing_directly()

    print("\n" + "=" * 60)
    print("To test with actual local data:")
    print("1. In another terminal: cd data/ && python3 -m http.server 8000")
    print("2. Then run this script again")
    print("=" * 60)

    # Uncomment to test with local server
    # test_schema_map_parsing()