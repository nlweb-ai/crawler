#!/usr/bin/env python3
"""
Start a worker process
Run in terminal 3 (can run multiple workers in separate terminals)
"""

import subprocess
import sys
import os
from pathlib import Path
import time

def main():
    print("=" * 60)
    print("WORKER PROCESS")
    print("=" * 60)
    print("Starting worker to process queue jobs")
    print(f"Queue directory: {os.getenv('QUEUE_DIR', 'queue')}")
    print("=" * 60)

    # Check dependencies
    try:
        import requests
        try:
            import pymssql
        except Exception as e:
            raise ImportError(f"pymssql import failed: {e!r}")
        print("✓ All dependencies installed")
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("\nInstall with:")
        print("  uv add requests")
        print("  uv add pymssql")
        sys.exit(1)

    # Check queue status
    queue_dir = Path(os.getenv('QUEUE_DIR', 'queue'))
    if queue_dir.exists():
        pending = len(list(queue_dir.glob('job-*.json')))
        processing = len(list(queue_dir.glob('*.processing')))
        print(f"\nQueue status:")
        print(f"  Pending jobs: {pending}")
        print(f"  Processing: {processing}")
    else:
        queue_dir.mkdir(exist_ok=True)
        print(f"\nCreated queue directory: {queue_dir}")

    print("\nStarting worker...")
    print("Worker will display detailed logs for each job processed")
    print("Press Ctrl+C to stop")
    print("-" * 60)
    print()

    try:
        subprocess.run(["python3", "code/core/worker.py"])
    except KeyboardInterrupt:
        print("\nWorker stopped")

if __name__ == "__main__":
    main()