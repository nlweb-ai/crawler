"""
Azure Storage Queue with Azure AD Authentication
This version uses Workload Identity / Managed Identity instead of connection strings
"""
import json
from typing import Optional, Dict, Any
from azure.core.exceptions import ResourceExistsError

from .queue_interface import QueueInterface, QueueMessage


class AzureStorageQueueAAD(QueueInterface):
    """Azure Storage Queue implementation using Azure AD authentication"""

    def __init__(self, storage_account_name: str, queue_name: str = 'crawler-jobs'):
        from azure.identity import DefaultAzureCredential
        from azure.storage.queue import QueueServiceClient

        self.storage_account_name = storage_account_name
        self.queue_name = queue_name
        self.account_url = f"https://{storage_account_name}.queue.core.windows.net"

        # Use DefaultAzureCredential which automatically handles:
        # - Workload Identity (when AZURE_FEDERATED_TOKEN_FILE is set)
        # - Managed Identity (when running in Azure)
        # - Azure CLI (when running locally)
        self.credential = DefaultAzureCredential()

        # Create queue service client
        self.service_client = QueueServiceClient(account_url=self.account_url, credential=self.credential)
        self.queue_client = self.service_client.get_queue_client(queue_name)

    def provision(self):
        """Ensure that the queue exists. If not, create it."""
        self.queue_client.get_queue_properties()

        try:
            self.queue_client.create_queue()
            print(f"[Queue] Created queue: {self.queue_name}")
        except ResourceExistsError:
            print(f"[Queue] Queue already exists: {self.queue_name}")
        except Exception as e:
            print(f"[Queue] Error creating queue: {e}")
            raise

    def send_message(self, message: Dict[str, Any]) -> bool:
        """Send message to Storage Queue"""
        try:
            content = json.dumps(message)
            self.queue_client.send_message(content)
            return True
        except Exception as e:
            print(f"[Storage Queue AAD] Error sending message: {e}")
            return False

    def receive_message(self, visibility_timeout: int = 300) -> Optional[QueueMessage]:
        """Receive message from Storage Queue"""
        try:
            # Get messages (max 1)
            messages = self.queue_client.receive_messages(
                messages_per_page=1,
                visibility_timeout=visibility_timeout
            )

            for msg in messages:
                content = json.loads(msg.content)
                return QueueMessage(
                    id=msg.id,
                    content=content,
                    receipt_handle=msg  # Store entire message for deletion
                )
        except Exception as e:
            import traceback
            print(f"[Storage Queue AAD] Error receiving message: {e}")
            print(f"[Storage Queue AAD] Error details: {traceback.format_exc()}")
        return None

    def delete_message(self, message: QueueMessage) -> bool:
        """Delete the message from Storage Queue"""
        try:
            msg = message.receipt_handle
            self.queue_client.delete_message(msg.id, msg.pop_receipt)
            return True
        except Exception as e:
            print(f"[Storage Queue AAD] Error deleting message: {e}")
            return False

    def return_message(self, message: QueueMessage) -> bool:
        """Return message to queue by updating visibility timeout to 0"""
        try:
            msg = message.receipt_handle
            # Update visibility timeout to 0 to make message immediately available
            self.queue_client.update_message(
                msg.id,
                msg.pop_receipt,
                visibility_timeout=0
            )
            return True
        except Exception as e:
            print(f"[Storage Queue AAD] Error returning message: {e}")
            return False

    def get_message_count(self) -> int:
        """Get approximate number of messages in queue"""
        try:
            properties = self.queue_client.get_queue_properties()
            return properties.approximate_message_count  # type: ignore
        except Exception as e:
            print(f"[Storage Queue AAD] Error getting message count: {e}")
            return -1


# def get_queue_with_aad():
#     """Factory function to create queue with AAD authentication"""
#     storage_account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
#     queue_name = os.getenv('AZURE_STORAGE_QUEUE_NAME', 'crawler-jobs')

#     if not storage_account:
#         raise ValueError("AZURE_STORAGE_ACCOUNT_NAME environment variable not set")

#     print(f"[Queue] Using Azure Storage Queue with AAD authentication: {storage_account}")
#     return AzureStorageQueueAAD(storage_account, queue_name)
