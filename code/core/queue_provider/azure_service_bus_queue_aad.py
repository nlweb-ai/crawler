"""
Azure Service Bus Queue with Azure AD Authentication
This version uses managed identity or Azure CLI credentials instead of connection strings
"""
import os
import json
from typing import Optional, Dict, Any
from azure.identity import DefaultAzureCredential

from .queue_interface import QueueInterface, QueueMessage


class AzureServiceBusQueueAAD(QueueInterface):
    """Azure Service Bus queue implementation using Azure AD authentication"""

    def __init__(self, namespace: str, queue_name: str):
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
        self.credential = DefaultAzureCredential()
        self._client = None

    def _get_client(self):
        if not self._client:
            from azure.servicebus import ServiceBusClient
            self._client = ServiceBusClient(
                fully_qualified_namespace=self.fully_qualified_namespace,
                credential=self.credential
            )
        return self._client

    def provision(self):
        """Establish a connection to Service Bus, peeking the queue to verify."""
        client = self._get_client()
        with client.get_queue_receiver(self.queue_name, max_wait_time=5) as receiver:
            receiver.peek_messages(max_message_count=1)

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
                        id=msg.message_id,  # type: ignore
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
