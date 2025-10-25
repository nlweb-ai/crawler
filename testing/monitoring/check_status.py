#!/usr/bin/env python3
"""
Check the status of the crawler system to verify if testing succeeded
"""

import sys
import os
import requests
import json
from datetime import datetime

sys.path.insert(0, 'code/core')

def check_api_status():
    """Check if API server is running"""
    try:
        response = requests.get("http://localhost:5001/api/status", timeout=2)
        return response.status_code == 200
    except:
        return False

def check_data_server():
    """Check if data server is running"""
    try:
        response = requests.get("http://localhost:8000/", timeout=2)
        return response.status_code in [200, 301, 302]
    except:
        return False

def check_database_status():
    """Check database for processing results"""
    import config
    import db

    conn = db.get_connection()
    cursor = conn.cursor()

    # Get site statistics
    cursor.execute("""
        SELECT
            s.site_url,
            s.last_processed,
            COUNT(DISTINCT f.file_url) as file_count,
            COUNT(DISTINCT i.id) as id_count
        FROM sites s
        LEFT JOIN files f ON s.site_url = f.site_url
        LEFT JOIN ids i ON f.file_url = i.file_url
        GROUP BY s.site_url, s.last_processed
        ORDER BY s.site_url
    """)

    results = cursor.fetchall()
    conn.close()

    return results

def check_queue_status():
    """Check queue directory status"""
    queue_dir = os.getenv('QUEUE_DIR', 'queue')

    if not os.path.exists(queue_dir):
        return 0, 0, 0

    pending = len([f for f in os.listdir(queue_dir) if f.endswith('.json') and '.processing' not in f])
    processing = len([f for f in os.listdir(queue_dir) if f.endswith('.processing')])

    error_dir = os.path.join(queue_dir, 'errors')
    failed = 0
    if os.path.exists(error_dir):
        failed = len([f for f in os.listdir(error_dir) if os.path.isfile(os.path.join(error_dir, f))])

    return pending, processing, failed

def check_worker_processes():
    """Check if worker processes are running"""
    import subprocess

    try:
        result = subprocess.run("ps aux | grep -E 'python.*worker.py' | grep -v grep",
                              shell=True, capture_output=True, text=True)
        return len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
    except:
        return 0

def main():
    print("=" * 70)
    print("CRAWLER SYSTEM STATUS CHECK")
    print("=" * 70)

    # Check services
    print("\n1. SERVICE STATUS")
    print("-" * 40)

    api_running = check_api_status()
    data_running = check_data_server()
    worker_count = check_worker_processes()

    print(f"   API Server (port 5001):  {'✓ Running' if api_running else '✗ Not running'}")
    print(f"   Data Server (port 8000): {'✓ Running' if data_running else '✗ Not running'}")
    print(f"   Worker Processes:        {worker_count} running")

    # Check queue
    print("\n2. QUEUE STATUS")
    print("-" * 40)

    pending, processing, failed = check_queue_status()
    print(f"   Pending Jobs:     {pending}")
    print(f"   Processing Jobs:  {processing}")
    print(f"   Failed Jobs:      {failed}")

    # Check database
    print("\n3. DATABASE STATUS")
    print("-" * 40)

    try:
        results = check_database_status()

        if not results:
            print("   No sites in database")
        else:
            total_files = 0
            total_ids = 0
            all_processed = True

            for site_url, last_processed, file_count, id_count in results:
                site_name = site_url.split('/')[-1] if site_url else "Unknown"
                total_files += file_count or 0
                total_ids += id_count or 0

                if last_processed:
                    last_proc_str = datetime.fromisoformat(str(last_processed)).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    last_proc_str = "Never"
                    all_processed = False

                print(f"\n   {site_name}:")
                print(f"      Files:          {file_count or 0}")
                print(f"      IDs:            {id_count or 0}")
                print(f"      Last Processed: {last_proc_str}")

            print(f"\n   TOTALS:")
            print(f"      Sites:     {len(results)}")
            print(f"      Files:     {total_files}")
            print(f"      IDs:       {total_ids}")

    except Exception as e:
        print(f"   Error accessing database: {e}")
        all_processed = False

    # Overall status
    print("\n" + "=" * 70)
    print("TEST RESULT")
    print("=" * 70)

    # Determine success criteria
    success_criteria = []
    failures = []

    # Check if services were started
    if api_running or data_running or worker_count > 0:
        success_criteria.append("✓ Services were started")
    else:
        failures.append("✗ No services running")

    # Check if data was processed
    if 'results' in locals() and results:
        if total_ids > 0:
            success_criteria.append(f"✓ Successfully processed {total_ids} IDs from {total_files} files")
        else:
            failures.append("✗ No IDs were extracted")

        if all_processed:
            success_criteria.append("✓ All sites have been processed")
        else:
            failures.append("✗ Some sites have not been processed")
    else:
        failures.append("✗ No data in database")

    # Check queue status
    if pending == 0 and processing == 0:
        success_criteria.append("✓ Queue is empty (all jobs completed)")
    else:
        failures.append(f"✗ Queue has {pending + processing} unprocessed jobs")

    if failed > 0:
        failures.append(f"✗ {failed} jobs failed")

    # Print results
    if success_criteria:
        print("\nSUCCESS INDICATORS:")
        for criteria in success_criteria:
            print(f"  {criteria}")

    if failures:
        print("\nISSUES FOUND:")
        for failure in failures:
            print(f"  {failure}")

    # Overall verdict
    print("\n" + "-" * 70)
    if not failures and success_criteria and len(success_criteria) >= 3:
        print("VERDICT: ✓ TESTING SUCCEEDED")
        print("\nThe crawler system successfully:")
        print("  • Processed all test sites")
        print("  • Extracted and stored schema.org data")
        print("  • Completed all queued jobs")
        return 0
    elif success_criteria and total_ids > 0:
        print("VERDICT: ⚠ PARTIAL SUCCESS")
        print("\nSome components worked but there are issues to address.")
        return 1
    else:
        print("VERDICT: ✗ TESTING FAILED")
        print("\nThe system did not process data successfully.")
        return 2

if __name__ == '__main__':
    sys.exit(main())