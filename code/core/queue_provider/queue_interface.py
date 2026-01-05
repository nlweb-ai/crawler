import abc
from typing import Optional, Dict, Any


class QueueMessage:
    """Represents a queue message"""
    def __init__(self, id: str, content: Dict[Any, Any], receipt_handle: Any = None):
        self.id = id
        self.content = content
        self.receipt_handle = receipt_handle


class QueueInterface(abc.ABC):
    """Abstract base class for queue implementations"""

    @abc.abstractmethod
    def provision(self):
        """Ensure that the queue has all resources available and ready to use"""
        pass

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

