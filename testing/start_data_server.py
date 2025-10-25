#!/usr/bin/env python3
"""
Start the data server to serve test schema files
Run in terminal 1
"""

import subprocess
import os
import sys
from pathlib import Path

DATA_SERVER_PORT = 8000

def main():
    print("=" * 60)
    print("DATA SERVER")
    print("=" * 60)
    print(f"Starting data server on port {DATA_SERVER_PORT}")
    print(f"Serving files from: data/")
    print(f"URL: http://localhost:{DATA_SERVER_PORT}/")
    print("=" * 60)
    print()

    data_dir = Path("data")
    if not data_dir.exists():
        print("ERROR: data/ directory not found!")
        print("Please run: python3 code/tests/mockdata/generate_test_data.py")
        sys.exit(1)

    # List available sites
    sites = [d.name for d in data_dir.iterdir() if d.is_dir() and (d / "schema_map.xml").exists()]
    if sites:
        print(f"Available sites ({len(sites)}):")
        for site in sites:
            print(f"  - http://localhost:{DATA_SERVER_PORT}/{site}/schema_map.xml")
        print()

    print("Press Ctrl+C to stop")
    print("-" * 60)

    try:
        subprocess.run(
            ["python3", "-m", "http.server", str(DATA_SERVER_PORT)],
            cwd="data"
        )
    except KeyboardInterrupt:
        print("\nData server stopped")

if __name__ == "__main__":
    main()