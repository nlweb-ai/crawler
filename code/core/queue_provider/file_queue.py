from datetime import datetime
import json
import os
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

