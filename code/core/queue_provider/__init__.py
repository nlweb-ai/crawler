import os

from .queue_interface import QueueInterface, QueueMessage
from .file_queue import FileQueue
from .azure_service_bus_queue import AzureServiceBusQueue
from .azure_storage_queue import AzureStorageQueue
from .azure_service_bus_queue_aad import AzureServiceBusQueueAAD
from .azure_storage_queue_aad import AzureStorageQueueAAD


def get_queue_from_env() -> QueueInterface:
    """
    Factory function to get queue implementation based on environment variables
    """
    queue_type = os.getenv('QUEUE_TYPE', 'file').lower()

    match queue_type:
        case 'file':
            return FileQueue(os.getenv('QUEUE_DIR', 'queue'))
        
        case 'servicebus':
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
                return AzureServiceBusQueue(conn_str, queue_name)
            
            raise ValueError("Neither AZURE_SERVICEBUS_NAMESPACE nor AZURE_SERVICEBUS_CONNECTION_STRING is set")
        
        case 'storage':
            # Use AAD authentication for Storage Queue
            storage_account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
            queue_name = os.getenv('AZURE_STORAGE_QUEUE_NAME', 'crawler-jobs')
            if storage_account:
                print(f"[Queue] Using Azure Storage Queue with AAD authentication: {storage_account}")
                return AzureStorageQueueAAD(storage_account, queue_name)

            conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
            if not conn_str:
                # Use Azurite default connection string for local dev
                conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"

            return AzureStorageQueue(conn_str)
        
    raise ValueError(f"Unsupported QUEUE_TYPE: {queue_type}")


__all__ = [
    'get_queue_from_env',
    'QueueInterface',
    'QueueMessage',
    'FileQueue',
    'AzureServiceBusQueue',
    'AzureServiceBusQueueAAD',
    'AzureStorageQueue',
    'AzureStorageQueueAAD',
]