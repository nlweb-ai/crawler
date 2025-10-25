"""
Job queue management with timeout and recovery mechanisms
"""
import os
import json
import time
from datetime import datetime, timedelta
import glob

class JobManager:
    """Manages job queue with timeout and recovery capabilities"""

    def __init__(self, queue_dir='queue', job_timeout_minutes=5, cleanup_interval_minutes=2):
        self.queue_dir = queue_dir
        self.job_timeout = timedelta(minutes=job_timeout_minutes)
        self.cleanup_interval = cleanup_interval_minutes * 60  # Convert to seconds
        self.last_cleanup = time.time()

    def start_cleanup_daemon(self):
        """No-op for compatibility - cleanup happens during job claims"""
        print(f"[JobManager] Cleanup will run inline (checking every {self.cleanup_interval}s)")

    def stop_cleanup_daemon(self):
        """No-op for compatibility"""
        pass

    def maybe_cleanup(self):
        """Periodically clean up stale jobs when claiming jobs"""
        current_time = time.time()
        if current_time - self.last_cleanup >= self.cleanup_interval:
            self.last_cleanup = current_time
            try:
                stale_count = self.cleanup_stale_jobs()
                if stale_count > 0:
                    print(f"[JobManager] Cleaned up {stale_count} stale jobs")
            except Exception as e:
                print(f"[JobManager] Error in cleanup: {e}")

    def cleanup_stale_jobs(self):
        """Find and reset stale processing jobs"""
        stale_count = 0
        current_time = datetime.utcnow()

        # Find all .processing files
        processing_files = glob.glob(os.path.join(self.queue_dir, '*.processing'))

        for processing_file in processing_files:
            try:
                # Check file modification time
                mtime = datetime.utcfromtimestamp(os.path.getmtime(processing_file))
                age = current_time - mtime

                if age > self.job_timeout:
                    print(f"[JobManager] Found stale job (age: {age}): {os.path.basename(processing_file)}")

                    # Read job content to log what's being reset
                    try:
                        with open(processing_file, 'r') as f:
                            job = json.load(f)
                        print(f"[JobManager]   Type: {job.get('type')}, Site: {job.get('site')}, File: {job.get('file_url')}")
                    except:
                        pass

                    # Reset job by removing .processing extension
                    original_path = processing_file.rsplit('.processing', 1)[0]

                    # If job failed multiple times, move to error queue
                    if '.retry' in original_path:
                        retry_count = int(original_path.split('.retry')[1].split('.')[0])
                        if retry_count >= 3:
                            # Move to errors after 3 retries
                            error_dir = os.path.join(self.queue_dir, 'errors')
                            os.makedirs(error_dir, exist_ok=True)
                            error_path = os.path.join(error_dir, os.path.basename(original_path))
                            os.rename(processing_file, error_path)
                            print(f"[JobManager]   Moved to errors after {retry_count} retries")
                            stale_count += 1
                            continue

                    # Add retry count to filename
                    base_name = original_path.rsplit('.retry', 1)[0] if '.retry' in original_path else original_path
                    retry_count = 0
                    if '.retry' in original_path:
                        retry_count = int(original_path.split('.retry')[1].split('.')[0])
                    retry_count += 1

                    new_path = f"{base_name}.retry{retry_count}.json"
                    os.rename(processing_file, new_path)
                    print(f"[JobManager]   Reset as: {os.path.basename(new_path)}")
                    stale_count += 1

            except Exception as e:
                print(f"[JobManager] Error processing {processing_file}: {e}")

        return stale_count

    def claim_job_with_heartbeat(self, queue_dir):
        """
        Claim a job and return both the path and a heartbeat function.
        The heartbeat should be called periodically during processing.
        """
        # Check for cleanup periodically
        self.maybe_cleanup()

        for filename in sorted(os.listdir(queue_dir)):
            if not filename.startswith('job-'):
                continue

            job_path = os.path.join(queue_dir, filename)
            processing_path = job_path + '.processing'

            try:
                # Atomic claim via rename
                os.rename(job_path, processing_path)

                # Read job
                with open(processing_path) as f:
                    job = json.load(f)

                # Create heartbeat function that updates file mtime
                def heartbeat():
                    try:
                        # Touch the file to update modification time
                        os.utime(processing_path, None)
                        return True
                    except:
                        return False

                return processing_path, job, heartbeat

            except (OSError, FileNotFoundError):
                # Job was already claimed by another worker
                continue
            except Exception as e:
                print(f"[JobManager] Error claiming job {filename}: {e}")
                continue

        return None, None, None

    def cleanup_on_startup(self):
        """
        Clean up any stale jobs on startup.
        This handles cases where workers crashed or were killed.
        """
        print(f"[JobManager] Checking for stale jobs on startup...")
        stale_count = self.cleanup_stale_jobs()
        if stale_count > 0:
            print(f"[JobManager] Cleaned up {stale_count} stale jobs from previous run")
        else:
            print(f"[JobManager] No stale jobs found")

    def mark_job_failed(self, processing_path, error_msg):
        """Mark a job as failed with error information"""
        try:
            # Read job
            with open(processing_path, 'r') as f:
                job = json.load(f)

            # Add error information
            job['last_error'] = error_msg
            job['failed_at'] = datetime.utcnow().isoformat()

            # Write to errors directory
            error_dir = os.path.join(self.queue_dir, 'errors')
            os.makedirs(error_dir, exist_ok=True)

            # Generate error filename
            timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
            error_filename = f"failed-{timestamp}-{os.path.basename(processing_path).replace('.processing', '')}"
            error_path = os.path.join(error_dir, error_filename)

            # Write job with error info
            with open(error_path, 'w') as f:
                json.dump(job, f, indent=2)

            # Remove processing file
            os.remove(processing_path)

            print(f"[JobManager] Job marked as failed: {error_filename}")

        except Exception as e:
            print(f"[JobManager] Error marking job as failed: {e}")
            # Try to at least move the file
            try:
                error_dir = os.path.join(self.queue_dir, 'errors')
                os.makedirs(error_dir, exist_ok=True)
                error_path = os.path.join(error_dir, os.path.basename(processing_path))
                os.rename(processing_path, error_path)
            except:
                pass