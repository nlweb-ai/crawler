
import json
from typing import Any, Dict, Optional

from .queue_interface import QueueMessage, QueueInterface


class AzureStorageQueue(QueueInterface):
    """Azure Storage Queue implementation (works with Azurite)"""

    DEFAULT_QUEUE_NAME = 'jobs'

    def __init__(self, connection_string: str, queue_name: str = DEFAULT_QUEUE_NAME):
        from azure.storage.queue import QueueServiceClient
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.queue_client = QueueServiceClient.from_connection_string(
            connection_string
        ).get_queue_client(queue_name)

        self.provision()

    def provision(self):
        '''Check if the queue is available by fetching its properties'''
        try:
            self.queue_client.get_queue_properties()
        except Exception:
            pass

        # Create queue if it doesn't exist
        try:
            self.queue_client.create_queue()
        except:
            pass  # Queue already exists

    def send_message(self, message: Dict[Any, Any]) -> bool:
        """Send message to Storage Queue"""
        try:
            self.queue_client.send_message(json.dumps(message))
            return True
        except Exception as e:
            print(f"[StorageQueue] Error sending message: {e}")
            return False

    def receive_message(self, visibility_timeout: int = 300) -> Optional[QueueMessage]:
        """Receive message from Storage Queue"""
        try:
            messages = self.queue_client.receive_messages(
                visibility_timeout=visibility_timeout,
                max_messages=1
            )
            for msg in messages:
                content = json.loads(msg.content)
                return QueueMessage(
                    id=msg.id,
                    content=content,
                    receipt_handle=(msg.id, msg.pop_receipt)
                )
        except Exception as e:
            print(f"[StorageQueue] Error receiving message: {e}")
        return None

    def delete_message(self, message: QueueMessage) -> bool:
        """Delete message from Storage Queue"""
        try:
            msg_id, pop_receipt = message.receipt_handle
            self.queue_client.delete_message(msg_id, pop_receipt)
            return True
        except Exception as e:
            print(f"[StorageQueue] Error deleting message: {e}")
            return False

    def return_message(self, message: QueueMessage) -> bool:
        """Update message visibility to 0 to return it"""
        try:
            msg_id, pop_receipt = message.receipt_handle
            self.queue_client.update_message(
                msg_id,
                pop_receipt,
                visibility_timeout=0
            )
            return True
        except Exception as e:
            print(f"[StorageQueue] Error returning message: {e}")
            return False