import json
from typing import Any, Dict, Optional

from .queue_interface import QueueInterface, QueueMessage

class AzureServiceBusQueue(QueueInterface):
    """Azure Service Bus queue implementation"""

    def __init__(self, connection_string: str, queue_name: str):
        from azure.servicebus import ServiceBusClient
        self.connection_string = connection_string
        self.queue_name = queue_name
        self._client: Optional[ServiceBusClient] = None

    def provision(self):
        """Establish a connection to Service Bus, peeking the queue to verify."""
        client = self._get_client()
        # Test connection by peeking at queue
        with client.get_queue_receiver(self.queue_name, max_wait_time=5) as receiver:
            receiver.peek_messages(max_message_count=1)

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
                        id=msg.message_id, # type: ignore
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

