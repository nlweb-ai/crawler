from datetime import datetime
import json
import os
import time
from typing import Any, Dict, Optional

from .queue_interface import QueueInterface, QueueMessage


class FileQueue(QueueInterface):
    """File-based queue implementation for local development"""

    def __init__(self, queue_dir: str = 'queue'):
        self.queue_dir = queue_dir
        self.provision()

    def provision(self):
        """Assert that the queue directory exists and is accessible"""
        os.makedirs(self.queue_dir, exist_ok=True)
    
    def status(self) -> Dict[str, Any]:
        status = {
            'queue_type': 'file',
            'pending_jobs': 0,
            'processing_jobs': 0,
            'failed_jobs': 0,
            'jobs': [],
            'error': None
        }

        # File-based queue status
        queue_dir = os.getenv('QUEUE_DIR', 'queue')
        status['queue_dir'] = queue_dir

        if not os.path.exists(queue_dir):
            return status

        # Count pending jobs
        for filename in sorted(os.listdir(queue_dir), reverse=True):
            if filename.startswith('job-') and filename.endswith('.json'):
                status['pending_jobs'] += 1
                # Read job details (limit to 20 most recent)
                if len([j for j in status['jobs'] if j['status'] == 'pending']) < 20:
                    try:
                        with open(os.path.join(queue_dir, filename)) as f:
                            job = json.load(f)
                            status['jobs'].append({
                                'id': filename,
                                'status': 'pending',
                                'type': job.get('type'),
                                'site': job.get('site'),
                                'file_url': job.get('file_url'),
                                'queued_at': job.get('queued_at')
                            })
                    except:
                        pass
            elif filename.endswith('.processing'):
                status['processing_jobs'] += 1
                # Read job details
                try:
                    filepath = os.path.join(queue_dir, filename)
                    mtime = os.path.getmtime(filepath)
                    age_seconds = int(time.time() - mtime)

                    with open(filepath) as f:
                        job = json.load(f)
                        status['jobs'].append({
                            'id': filename,
                            'status': 'processing',
                            'type': job.get('type'),
                            'site': job.get('site'),
                            'file_url': job.get('file_url'),
                            'queued_at': job.get('queued_at'),
                            'processing_time': age_seconds
                        })
                except:
                    pass

        # Count failed jobs
        error_dir = os.path.join(queue_dir, 'errors')
        if os.path.exists(error_dir):
            for filename in os.listdir(error_dir):
                if filename.startswith('job-') or filename.startswith('failed-'):
                    status['failed_jobs'] += 1

        return status

    def send_message(self, message: Dict[Any, Any]) -> bool:
        """Write a job file to the queue directory"""
        try:
            job_id = f"job-{datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')}.json"
            temp_path = os.path.join(self.queue_dir, f".tmp-{job_id}")
            final_path = os.path.join(self.queue_dir, job_id)

            with open(temp_path, 'w') as f:
                json.dump(message, f)
            os.rename(temp_path, final_path)  # Atomic write
            return True
        except Exception as e:
            print(f"[FileQueue] Error sending message: {e}")
            return False

    def receive_message(self, visibility_timeout: int = 300) -> Optional[QueueMessage]:
        """Claim a job from the file system"""
        try:
            for filename in sorted(os.listdir(self.queue_dir)):
                if not filename.startswith('job-') or not filename.endswith('.json'):
                    continue

                job_path = os.path.join(self.queue_dir, filename)
                processing_path = job_path + '.processing'

                try:
                    # Atomic claim via rename
                    os.rename(job_path, processing_path)

                    # Read job
                    with open(processing_path) as f:
                        content = json.load(f)

                    return QueueMessage(
                        id=filename,
                        content=content,
                        receipt_handle=processing_path
                    )
                except (OSError, FileNotFoundError):
                    continue
        except Exception as e:
            print(f"[FileQueue] Error receiving message: {e}")

        return None

    def delete_message(self, message: QueueMessage) -> bool:
        """Remove the processing file"""
        try:
            if os.path.exists(message.receipt_handle):
                os.remove(message.receipt_handle)
            return True
        except Exception as e:
            print(f"[FileQueue] Error deleting message: {e}")
            return False

    def return_message(self, message: QueueMessage) -> bool:
        """Return job to queue by removing .processing extension"""
        try:
            if os.path.exists(message.receipt_handle):
                original_path = message.receipt_handle.replace('.processing', '')
                os.rename(message.receipt_handle, original_path)
            return True
        except Exception as e:
            print(f"[FileQueue] Error returning message: {e}")
            return False

