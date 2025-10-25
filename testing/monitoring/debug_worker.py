#!/usr/bin/env python3
"""Debug worker to understand what's failing"""

import sys
import os
import time
import json
import traceback

sys.path.insert(0, 'code/core')
import config
import db
from worker import process_job

def simple_worker():
    """Simple worker without JobManager to debug issues"""
    print("[DEBUG] Starting debug worker...")

    conn = db.get_connection()
    print("[DEBUG] Connected to database")

    queue_dir = os.getenv('QUEUE_DIR', 'queue')

    while True:
        # Look for jobs
        found_job = False

        for filename in sorted(os.listdir(queue_dir)):
            if not filename.startswith('job-') or not filename.endswith('.json'):
                continue

            job_path = os.path.join(queue_dir, filename)
            processing_path = job_path + '.processing'

            try:
                # Claim job
                os.rename(job_path, processing_path)
                print(f"\n[DEBUG] Claimed job: {filename}")

                # Read job
                with open(processing_path) as f:
                    job = json.load(f)

                print(f"[DEBUG] Job type: {job.get('type')}")
                print(f"[DEBUG] File URL: {job.get('file_url')}")

                # Process job
                print("[DEBUG] Calling process_job...")
                success = process_job(conn, processing_path, job)
                print(f"[DEBUG] process_job returned: {success}")

                if success:
                    os.remove(processing_path)
                    print("[DEBUG] Job completed successfully")
                else:
                    # Move to errors
                    print("[DEBUG] Job failed, moving to errors")
                    error_dir = os.path.join(queue_dir, 'errors')
                    os.makedirs(error_dir, exist_ok=True)

                    # Add error info
                    job['last_error'] = "Processing failed (debug)"
                    error_file = os.path.join(error_dir, f"debug-{filename}")

                    with open(error_file, 'w') as f:
                        json.dump(job, f)

                    os.remove(processing_path)

                found_job = True
                break

            except Exception as e:
                print(f"[DEBUG] Exception in worker: {e}")
                traceback.print_exc()

                # Try to clean up
                if os.path.exists(processing_path):
                    os.remove(processing_path)
                break

        if not found_job:
            print("[DEBUG] No jobs found, sleeping...")
            time.sleep(5)

if __name__ == '__main__':
    try:
        simple_worker()
    except KeyboardInterrupt:
        print("\n[DEBUG] Stopped by user")
    except Exception as e:
        print(f"[DEBUG] Fatal error: {e}")
        traceback.print_exc()