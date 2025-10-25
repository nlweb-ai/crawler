#!/usr/bin/env python3
"""
Launch script to start all services and add test sites
"""

import subprocess
import time
import sys
import os
import signal
from pathlib import Path

# Check and install requirements first
def check_requirements():
    """Check if requirements are installed, install if needed"""
    print("Checking Python dependencies...")

    # Check what's missing
    missing = []
    optional_missing = []

    try:
        import flask
    except ImportError:
        missing.append("flask")

    try:
        import flask_cors
    except ImportError:
        missing.append("flask-cors")

    try:
        import requests
    except ImportError:
        missing.append("requests")

    try:
        import pyodbc
        print("  ✓ pyodbc installed (database connections enabled)")
    except ImportError:
        optional_missing.append("pyodbc")
        print("  ⚠ pyodbc not installed (database connections disabled)")
        print("    To enable database, run: ./install_pyodbc_mac.sh (macOS)")

    # Install required packages if missing
    if missing:
        print(f"  Installing required packages: {', '.join(missing)}")
        req_file = Path("code/requirements-test.txt")

        if req_file.exists():
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(req_file)])
                print("  ✓ Required dependencies installed")
            except subprocess.CalledProcessError as e:
                print(f"  ✗ Failed to install dependencies: {e}")
                print("\nPlease install manually:")
                print(f"  pip install flask flask-cors requests")
                sys.exit(1)
        else:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "flask", "flask-cors", "requests"])
                print("  ✓ Required dependencies installed")
            except subprocess.CalledProcessError as e:
                print(f"  ✗ Failed to install dependencies: {e}")
                sys.exit(1)
    else:
        print("  ✓ All required dependencies installed")

    if optional_missing:
        print("\n  Note: Running in test mode without database connections")
        print("  The system will use file queue but won't persist to SQL")

# Check requirements before importing
check_requirements()

# Now import the modules we need
import requests
import json

# Configuration
API_SERVER_PORT = 5001  # Changed from 5000 to avoid macOS AirPlay conflict
API_BASE = f"http://localhost:{API_SERVER_PORT}/api"
DATA_SERVER_PORT = 8000

# Track processes for cleanup
processes = []

def cleanup(signum=None, frame=None):
    """Clean up all processes on exit"""
    print("\n\nShutting down all services...")
    for proc in processes:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except:
            proc.kill()
    sys.exit(0)

# Register cleanup handlers
signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

def start_data_server():
    """Start the HTTP server to serve test data"""
    print("Starting data server on port 8000...")
    proc = subprocess.Popen(
        ["python3", "-m", "http.server", str(DATA_SERVER_PORT)],
        cwd="data",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    processes.append(proc)
    print(f"  Data server started (PID: {proc.pid})")
    return proc

def start_api_server():
    """Start the Flask API server"""
    print("Starting API server on port 5000...")
    proc = subprocess.Popen(
        ["python3", "code/core/api.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, 'FLASK_ENV': 'development'}
    )
    processes.append(proc)
    print(f"  API server started (PID: {proc.pid})")
    return proc

def start_worker():
    """Start a worker process"""
    print("Starting worker process...")
    proc = subprocess.Popen(
        ["python3", "code/core/worker.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    processes.append(proc)
    print(f"  Worker started (PID: {proc.pid})")
    return proc

def wait_for_server(url, name, max_attempts=30):
    """Wait for a server to be ready"""
    print(f"Waiting for {name} to be ready...")
    for i in range(max_attempts):
        try:
            response = requests.get(url, timeout=1)
            if response.status_code < 500:
                print(f"  {name} is ready!")
                return True
        except:
            pass
        time.sleep(1)
        if i % 5 == 0 and i > 0:
            print(f"  Still waiting for {name}... ({i}s)")

    print(f"  ERROR: {name} failed to start after {max_attempts} seconds")
    return False

def add_test_site(site_name, base_url="http://localhost:8000"):
    """Add a test site using the API"""
    site_url = f"{base_url}/{site_name}"

    print(f"\nAdding site: {site_name}")

    # First add the site
    try:
        response = requests.post(
            f"{API_BASE}/sites",
            json={
                "site_url": site_url,
                "interval_hours": 24
            }
        )
        if response.status_code == 200:
            print(f"  ✓ Site added: {site_url}")
        else:
            print(f"  ✗ Failed to add site: {response.text}")
            return False
    except Exception as e:
        print(f"  ✗ Error adding site: {e}")
        return False

    # Then trigger processing
    try:
        response = requests.post(
            f"{API_BASE}/process/{requests.utils.quote(site_url, safe='')}"
        )
        if response.status_code == 200:
            print(f"  ✓ Processing triggered for {site_name}")
            return True
        else:
            print(f"  ✗ Failed to trigger processing: {response.text}")
            return False
    except Exception as e:
        print(f"  ✗ Error triggering processing: {e}")
        return False

def check_status():
    """Check the current system status"""
    try:
        # Get queue status
        response = requests.get(f"{API_BASE}/queue/status")
        if response.status_code == 200:
            data = response.json()
            print("\n=== Queue Status ===")
            print(f"  Pending jobs:     {data['pending_jobs']}")
            print(f"  Processing jobs:  {data['processing_jobs']}")
            print(f"  Failed jobs:      {data['failed_jobs']}")
            print(f"  Total jobs:       {data['total_jobs']}")

            if data['jobs']:
                print("\n  Recent jobs:")
                for job in data['jobs'][:5]:  # Show first 5
                    print(f"    - [{job['status']}] {job['type']} for {job.get('site', 'N/A')}")

        # Get site status
        response = requests.get(f"{API_BASE}/status")
        if response.status_code == 200:
            data = response.json()
            print("\n=== Site Status ===")
            for site in data:
                print(f"  {site['site_url']}:")
                print(f"    Files: {site['total_files']}, IDs: {site['total_ids']}")
                print(f"    Last processed: {site['last_processed'] or 'Never'}")

    except Exception as e:
        print(f"Error checking status: {e}")

def run_worker_mode():
    """Run in worker mode - just the worker process"""
    print("=" * 60)
    print("CRAWLER WORKER")
    print("=" * 60)
    print("Starting worker process...")
    print("Worker will show detailed logs for each job")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()

    # Run worker directly without capturing output
    import subprocess
    try:
        subprocess.run(["python3", "code/core/worker.py"])
    except KeyboardInterrupt:
        print("\nWorker stopped")

def run_master_mode(clear_db=False, add_sites=True):
    """Run in master mode - starts servers and optionally adds sites"""
    print("=" * 60)
    print("CRAWLER MASTER (SERVERS)")
    print("=" * 60)

    # Check if test data exists
    data_dir = Path("data")
    if not data_dir.exists():
        print("ERROR: data/ directory not found!")
        print("Please run: python3 code/tests/mockdata/generate_test_data.py")
        sys.exit(1)

    # Find available test sites
    test_sites = []
    for site_dir in data_dir.iterdir():
        if site_dir.is_dir() and (site_dir / "schema_map.xml").exists():
            test_sites.append(site_dir.name)

    if not test_sites:
        print("ERROR: No test sites found in data/ directory!")
        print("Please run: python3 code/tests/mockdata/generate_test_data.py")
        sys.exit(1)

    print(f"\nFound {len(test_sites)} test sites:")
    for site in test_sites:
        print(f"  - {site}")

    # Optionally clear database for fresh start
    if clear_db:
        print("\n" + "=" * 60)
        print("CLEARING DATABASE")
        print("=" * 60)

        try:
            # Import db module
            import sys
            sys.path.insert(0, 'code/core')
            import db

            conn = db.get_connection()
            db.clear_all_data(conn)
            conn.close()
        except Exception as e:
            print(f"Warning: Could not clear database: {e}")
            response = input("\nContinue anyway? (y/n): ")
            if response.lower() != 'y':
                print("Exiting...")
                sys.exit(0)

    # Start services
    print("\n" + "=" * 60)
    print("STARTING SERVICES")
    print("=" * 60)

    # Start data server
    data_proc = start_data_server()
    time.sleep(2)

    # Start API server
    api_proc = start_api_server()

    # Wait for API server to be ready
    if not wait_for_server(f"http://localhost:{API_SERVER_PORT}/api/status", "API server"):
        cleanup()

    # Don't start worker in master mode - it will run separately

    # Optionally add test sites
    if add_sites:
        print("\n" + "=" * 60)
        print("ADDING TEST SITES")
        print("=" * 60)

        for site in test_sites[:2]:  # Add first 2 sites for testing
            add_test_site(site)
            time.sleep(1)

    # Monitor progress
    print("\n" + "=" * 60)
    print("MONITORING PROGRESS")
    print("=" * 60)
    print("\nURLs for monitoring:")
    print(f"  Web UI:        http://localhost:{API_SERVER_PORT}/")
    print(f"  API Status:    http://localhost:{API_SERVER_PORT}/api/status")
    print(f"  Queue Status:  http://localhost:{API_SERVER_PORT}/api/queue/status")
    print(f"  Data Server:   http://localhost:{DATA_SERVER_PORT}/")

    print("\nPress Ctrl+C to stop all services\n")

    # Monitor loop
    try:
        while True:
            time.sleep(10)
            check_status()
    except KeyboardInterrupt:
        pass

    cleanup()

def main():
    """Main entry point with argument parsing"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Launch crawler components for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Terminal 1 (Master):  python3 launch_test.py master --clear-db
  Terminal 2 (Worker):  python3 launch_test.py worker

Options for master mode:
  --clear-db     Clear database before starting
  --no-add-sites Don't automatically add test sites
        """
    )

    parser.add_argument('mode', choices=['master', 'worker'],
                        help='Run as master (servers) or worker')
    parser.add_argument('--clear-db', action='store_true',
                        help='Clear database before starting (master only)')
    parser.add_argument('--no-add-sites', action='store_true',
                        help="Don't automatically add test sites (master only)")

    args = parser.parse_args()

    if args.mode == 'worker':
        run_worker_mode()
    else:  # master mode
        run_master_mode(
            clear_db=args.clear_db,
            add_sites=not args.no_add_sites
        )

if __name__ == "__main__":
    main()