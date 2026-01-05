"""
Azure Service Bus Queue with Azure AD Authentication
This version uses managed identity or Azure CLI credentials instead of connection strings
"""
import os
import json
from typing import Optional, Dict, Any
from queue_interface import QueueInterface, QueueMessage
from azure.identity import AzureCliCredential


class AzureServiceBusQueueAAD(QueueInterface):
    """Azure Service Bus queue implementation using Azure AD authentication"""

    def __init__(self, namespace: str, queue_name: str):
        from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
        from azure.servicebus import ServiceBusClient
        import os

        self.namespace = namespace
        self.queue_name = queue_name
        # Handle both formats: "namespace" or "namespace.servicebus.windows.net"
        if '.servicebus.windows.net' in namespace:
            self.fully_qualified_namespace = namespace
        else:
            self.fully_qualified_namespace = f"{namespace}.servicebus.windows.net"

        # Use DefaultUsing Azure Service Bus with AAD authenticationAzureCredential which automatically handles:
        # - Workload Identity (when AZURE_FEDERATED_TOKEN_FILE is set)
        # - Managed Identity (when running in Azure)
        # - Azure CLI (when running locally)
        print("[Queue] Using DefaultAzureCredential (supports Workload Identity)")
        self.credential = AzureCliCredential()
        self._client = None

    def _get_client(self):
        if not self._client:
            from azure.servicebus import ServiceBusClient
            self._client = ServiceBusClient(
                fully_qualified_namespace=self.fully_qualified_namespace,
                credential=self.credential
            )
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
            print(f"[ServiceBus AAD] Error sending message: {e}")
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
            import traceback
            print(f"[ServiceBus AAD] Error receiving message: {e}")
            print(f"[ServiceBus AAD] Error details: {traceback.format_exc()}")
        return None

    def delete_message(self, message: QueueMessage) -> bool:
        """Complete the message in Service Bus"""
        try:
            client = self._get_client()
            with client.get_queue_receiver(queue_name=self.queue_name) as receiver:
                receiver.complete_message(message.receipt_handle)
            return True
        except Exception as e:
            print(f"[ServiceBus AAD] Error completing message: {e}")
            return False

    def return_message(self, message: QueueMessage) -> bool:
        """Abandon the message to return it to queue"""
        try:
            client = self._get_client()
            with client.get_queue_receiver(queue_name=self.queue_name) as receiver:
                receiver.abandon_message(message.receipt_handle)
            return True
        except Exception as e:
            print(f"[ServiceBus AAD] Error abandoning message: {e}")
            return False


def get_queue_with_aad() -> QueueInterface:
    """
    Factory function to get queue implementation with Azure AD support
    """
    from queue_interface import FileQueue
    from queue_interface_storage import AzureStorageQueueAAD

    queue_type = os.getenv('QUEUE_TYPE', 'file').lower()

    if queue_type == 'file':
        return FileQueue(os.getenv('QUEUE_DIR', 'queue'))

    elif queue_type == 'servicebus':
        # Try AAD authentication first
        namespace = os.getenv('AZURE_SERVICEBUS_NAMESPACE')
        queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'crawler-jobs')
        if namespace:
            print(f"[Queue] Using Azure Service Bus with AAD authentication: {namespace}/{queue_name}")
            return AzureServiceBusQueueAAD(namespace, queue_name)

        # Fall back to connection string if available
        conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
        if conn_str:
            print("[Queue] Using Azure Service Bus with connection string")
            from queue_interface import AzureServiceBusQueue
            return AzureServiceBusQueue(conn_str)

        raise ValueError("Neither AZURE_SERVICEBUS_NAMESPACE nor AZURE_SERVICEBUS_CONNECTION_STRING is set")

    elif queue_type == 'storage':
        # Use AAD authentication for Storage Queue
        storage_account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        queue_name = os.getenv('AZURE_STORAGE_QUEUE_NAME', 'crawler-jobs')

        if not storage_account:
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME environment variable not set")

        print(f"[Queue] Using Azure Storage Queue with AAD authentication: {storage_account}")
        return AzureStorageQueueAAD(storage_account, queue_name)

    else:
        raise ValueError(f"Unknown queue type: {queue_type}")