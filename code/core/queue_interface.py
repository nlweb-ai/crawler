"""
Queue abstraction layer that supports multiple backends
"""
import config  # Load environment variables
import os
import json
import abc
from datetime import datetime
from typing import Optional, Dict, Any, Callable


class QueueMessage:
    """Represents a queue message"""
    def __init__(self, id: str, content: Dict[Any, Any], receipt_handle: Any = None):
        self.id = id
        self.content = content
        self.receipt_handle = receipt_handle


class QueueInterface(abc.ABC):
    """Abstract base class for queue implementations"""

    @abc.abstractmethod
    def send_message(self, message: Dict[Any, Any]) -> bool:
        """Send a message to the queue"""
        pass

    @abc.abstractmethod
    def receive_message(self, visibility_timeout: int = 300) -> Optional[QueueMessage]:
        """Receive a message from the queue"""
        pass

    @abc.abstractmethod
    def delete_message(self, message: QueueMessage) -> bool:
        """Delete a message from the queue"""
        pass

    @abc.abstractmethod
    def return_message(self, message: QueueMessage) -> bool:
        """Return a message to the queue (make visible again)"""
        pass


class FileQueue(QueueInterface):
    """File-based queue implementation for local development"""

    def __init__(self, queue_dir: str = 'queue'):
        self.queue_dir = queue_dir
        os.makedirs(queue_dir, exist_ok=True)

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


class AzureServiceBusQueue(QueueInterface):
    """Azure Service Bus queue implementation"""

    def __init__(self, connection_string: str, queue_name: str = 'jobs'):
        from azure.servicebus import ServiceBusClient
        self.connection_string = connection_string
        self.queue_name = queue_name
        self._client = None

    def _get_client(self):
        if not self._client:
            from azure.servicebus import ServiceBusClient
            self._client = ServiceBusClient.from_connection_string(self.connection_string)
        return self._client

    def send_message(self, message: Dict[Any, Any]) -> bool:
        """Send message to Service Bus"""
        try:
            from azure.servicebus import ServiceBusMessage
            client = self._get_client()
            with client.get_queue_sender(queue_name=self.queue_name) as sender:
                sb_message = ServiceBusMessage(json.dumps(message))
                sender.send_messages(sb_message)
            return True
        except Exception as e:
            print(f"[ServiceBus] Error sending message: {e}")
            return False

    def receive_message(self, visibility_timeout: int = 300) -> Optional[QueueMessage]:
        """Receive message from Service Bus"""
        try:
            client = self._get_client()
            with client.get_queue_receiver(
                queue_name=self.queue_name,
                max_wait_time=5
            ) as receiver:
                messages = receiver.receive_messages(max_message_count=1, max_wait_time=5)
                if messages:
                    msg = messages[0]
                    content = json.loads(str(msg))
                    return QueueMessage(
                        id=msg.message_id,
                        content=content,
                        receipt_handle=msg
                    )
        except Exception as e:
            print(f"[ServiceBus] Error receiving message: {e}")
        return None

    def delete_message(self, message: QueueMessage) -> bool:
        """Complete the message in Service Bus"""
        try:
            client = self._get_client()
            with client.get_queue_receiver(queue_name=self.queue_name) as receiver:
                receiver.complete_message(message.receipt_handle)
            return True
        except Exception as e:
            print(f"[ServiceBus] Error completing message: {e}")
            return False

    def return_message(self, message: QueueMessage) -> bool:
        """Abandon the message to return it to queue"""
        try:
            client = self._get_client()
            with client.get_queue_receiver(queue_name=self.queue_name) as receiver:
                receiver.abandon_message(message.receipt_handle)
            return True
        except Exception as e:
            print(f"[ServiceBus] Error abandoning message: {e}")
            return False


class AzureStorageQueue(QueueInterface):
    """Azure Storage Queue implementation (works with Azurite)"""

    def __init__(self, connection_string: str, queue_name: str = 'jobs'):
        from azure.storage.queue import QueueServiceClient
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.queue_client = QueueServiceClient.from_connection_string(
            connection_string
        ).get_queue_client(queue_name)

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


def get_queue() -> QueueInterface:
    """Factory function to get appropriate queue implementation"""

    queue_type = os.getenv('QUEUE_TYPE', 'file').lower()

    if queue_type == 'file':
        return FileQueue(os.getenv('QUEUE_DIR', 'queue'))

    elif queue_type == 'servicebus':
        conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
        if not conn_str:
            raise ValueError("AZURE_SERVICEBUS_CONNECTION_STRING not set")
        return AzureServiceBusQueue(conn_str)

    elif queue_type == 'storage':
        conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not conn_str:
            # Use Azurite default connection string for local dev
            conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
        return AzureStorageQueue(conn_str)

    else:
        raise ValueError(f"Unknown queue type: {queue_type}")