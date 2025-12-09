#!/usr/bin/env python3
"""
Start the API server
Run in terminal 2
"""

import subprocess
import sys
import os
from pathlib import Path

API_PORT = 5001

def main():
    print("=" * 60)
    print("API SERVER")
    print("=" * 60)
    print(f"Starting API server on port {API_PORT}")
    print(f"Web UI: http://localhost:{API_PORT}/")
    print(f"API Status: http://localhost:{API_PORT}/api/status")
    print(f"Queue Status: http://localhost:{API_PORT}/api/queue/status")
    print("=" * 60)

    # Check dependencies
    try:
        import flask
        import flask_cors
        try:
            import pymssql
        except Exception as e:
            raise ImportError(f"pymssql import failed: {e!r}")
        print("✓ All dependencies installed")
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("\nInstall with:")
        print("  pip install flask flask-cors")
        print("  pip install pymssql")
        sys.exit(1)

    # Set environment for API port
    os.environ['API_PORT'] = str(API_PORT)

    print("\nStarting Flask app...")
    print("Press Ctrl+C to stop")
    print("-" * 60)

    try:
        subprocess.run(["python3", "code/core/api.py"])
    except KeyboardInterrupt:
        print("\nAPI server stopped")

if __name__ == "__main__":
    main()