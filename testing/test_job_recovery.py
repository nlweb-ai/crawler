#!/usr/bin/env python3
"""
Test script to demonstrate job recovery mechanism
"""

import sys
import os
import json
import time
from datetime import datetime

sys.path.insert(0, 'code/core')

def create_test_job(queue_dir, site_name, will_hang=False):
    """Create a test job that optionally simulates a hang"""
    job = {
        'type': 'process_file',
        'site': f'http://localhost:8000/{site_name}',
        'file_url': f'http://localhost:8000/{site_name}/test.json',
        'queued_at': datetime.utcnow().isoformat(),
        'test_hang': will_hang  # Special flag for testing
    }

    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')
    job_file = os.path.join(queue_dir, f'job-{timestamp}.json')

    with open(job_file, 'w') as f:
        json.dump(job, f)

    print(f"Created {'hanging' if will_hang else 'normal'} job: {os.path.basename(job_file)}")
    return job_file

def simulate_stuck_job(queue_dir):
    """Create a .processing file to simulate a stuck job"""
    job = {
        'type': 'process_file',
        'site': 'http://localhost:8000/stuck_site',
        'file_url': 'http://localhost:8000/stuck_site/stuck.json',
        'queued_at': datetime.utcnow().isoformat()
    }

    # Create a .processing file directly
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')
    processing_file = os.path.join(queue_dir, f'job-{timestamp}.json.processing')

    with open(processing_file, 'w') as f:
        json.dump(job, f)

    # Modify the file time to make it look old (6 minutes ago)
    old_time = time.time() - 360  # 6 minutes ago
    os.utime(processing_file, (old_time, old_time))

    print(f"Created stuck job: {os.path.basename(processing_file)}")
    print(f"  Modified time: {datetime.fromtimestamp(old_time)}")
    return processing_file

def check_queue_status(queue_dir):
    """Check current queue status"""
    pending = [f for f in os.listdir(queue_dir) if f.endswith('.json') and not '.processing' in f]
    processing = [f for f in os.listdir(queue_dir) if f.endswith('.processing')]
    retry = [f for f in os.listdir(queue_dir) if '.retry' in f]

    print(f"\n=== Queue Status ===")
    print(f"Pending jobs: {len(pending)}")
    print(f"Processing jobs: {len(processing)}")
    print(f"Retry jobs: {len(retry)}")

    if processing:
        print("\nProcessing files:")
        for f in processing:
            file_path = os.path.join(queue_dir, f)
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            age = datetime.now() - mtime
            print(f"  - {f} (age: {age})")

    if retry:
        print("\nRetry files:")
        for f in retry:
            print(f"  - {f}")

def main():
    queue_dir = os.getenv('QUEUE_DIR', 'queue')
    os.makedirs(queue_dir, exist_ok=True)

    print("=" * 60)
    print("JOB RECOVERY TEST")
    print("=" * 60)

    # Check initial status
    check_queue_status(queue_dir)

    print("\n1. Creating a stuck job (simulated 6 minutes old)...")
    stuck_file = simulate_stuck_job(queue_dir)

    print("\n2. Creating normal test jobs...")
    create_test_job(queue_dir, 'test_site_1')
    create_test_job(queue_dir, 'test_site_2')

    check_queue_status(queue_dir)

    print("\n3. Starting JobManager cleanup...")
    from job_manager import JobManager

    job_manager = JobManager(queue_dir, job_timeout_minutes=5)

    print("\n4. Running cleanup (should recover stuck job)...")
    stale_count = job_manager.cleanup_stale_jobs()
    print(f"   Cleaned up {stale_count} stale jobs")

    check_queue_status(queue_dir)

    print("\n5. Testing cleanup daemon...")
    print("   Starting daemon (will check every 2 minutes)...")
    job_manager.start_cleanup_daemon()

    print("\n6. Creating another stuck job...")
    simulate_stuck_job(queue_dir)

    check_queue_status(queue_dir)

    print("\n   Waiting 10 seconds for daemon to clean up...")
    # Override cleanup interval for testing
    job_manager.cleanup_interval = 5  # 5 seconds for testing
    job_manager.stop_cleanup_daemon()
    job_manager.start_cleanup_daemon()

    time.sleep(10)

    check_queue_status(queue_dir)

    print("\n7. Stopping cleanup daemon...")
    job_manager.stop_cleanup_daemon()

    print("\n✓ Test complete!")
    print("\nThe JobManager provides:")
    print("  • Automatic recovery of stuck jobs (timeout: 5 minutes)")
    print("  • Retry tracking (up to 3 retries)")
    print("  • Background cleanup daemon")
    print("  • Heartbeat mechanism for long-running jobs")

if __name__ == '__main__':
    main()