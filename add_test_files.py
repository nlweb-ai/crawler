#!/usr/bin/env python3
"""
Manually add test files to the database and queue for processing.
This bypasses the schema_map requirement for testing purposes.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code', 'core'))

import db
from datetime import datetime
import json

def add_test_files():
    """Add test files directly to database for testing"""

    user_id = "test:user001"

    # Test pages with schema.org markup
    test_files = [
        ("https://www.hebbarskitchen.com", "https://hebbarskitchen.com/eggless-bread-omelette-recipe-vegetarian/"),
        ("https://www.hebbarskitchen.com", "https://hebbarskitchen.com/paneer-chingari-recipe-dhaba-style/"),
        ("https://www.imdb.com", "https://www.imdb.com/title/tt0111161/"),  # Shawshank Redemption
        ("https://www.backcountry.com", "https://www.backcountry.com/patagonia-nano-puff-jacket-mens"),
    ]

    conn = db.get_connection()
    added_count = 0

    try:
        cursor = conn.cursor()

        for site_url, file_url in test_files:
            schema_map = "manual_test"

            # Check if file already exists
            cursor.execute("""
                SELECT file_url FROM files
                WHERE site_url = %s AND user_id = %s AND file_url = %s
            """, (site_url, user_id, file_url))

            if cursor.fetchone():
                print(f"✓ File already exists: {file_url}")
                continue

            # Add file to database
            cursor.execute("""
                INSERT INTO files (site_url, user_id, schema_map, file_url, created_at)
                VALUES (%s, %s, %s, %s, GETUTCDATE())
            """, (site_url, user_id, schema_map, file_url))
            conn.commit()
            added_count += 1
            print(f"✓ Added file to DB: {file_url}")

        print(f"\n{'='*80}")
        print(f"Summary: {added_count} files added to database")
        print(f"{'='*80}")
        print("\nNote: Files added to database. Master scheduler will queue them for processing.")
        print("Check status with: curl -H 'X-API-Key: <key>' http://172.193.209.48/api/status")

    finally:
        conn.close()

if __name__ == '__main__':
    add_test_files()
