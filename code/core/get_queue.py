import os

from queue_interface import QueueInterface
from queue_interface_aad import AzureServiceBusQueueAAD


def get_queue() -> QueueInterface:
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
        if namespace:
            print(f"[Queue] Using Azure Service Bus with AAD authentication: {namespace}")
            return AzureServiceBusQueueAAD(namespace, os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'crawler-queue'))

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