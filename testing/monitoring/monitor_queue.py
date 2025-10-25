#!/usr/bin/env python3
"""
Queue monitoring tool for the crawler system
Supports both file-based and Azure Service Bus queues
"""
import os
import sys
import time
import json
from datetime import datetime
sys.path.insert(0, 'code/core')
import config  # Load environment variables

def monitor_file_queue():
    """Monitor file-based queue"""
    queue_dir = os.getenv('QUEUE_DIR', 'queue')

    if not os.path.exists(queue_dir):
        print(f"Queue directory {queue_dir} does not exist")
        return

    while True:
        os.system('clear')
        print("=" * 70)
        print(f"FILE QUEUE MONITOR - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 70)
        print()

        # Count different types of files
        pending = []
        processing = []

        for filename in os.listdir(queue_dir):
            filepath = os.path.join(queue_dir, filename)
            if filename.endswith('.processing'):
                processing.append(filename)
            elif filename.startswith('job-') and filename.endswith('.json'):
                pending.append(filename)

        # Show summary
        print(f"📊 SUMMARY")
        print(f"  Pending:    {len(pending)}")
        print(f"  Processing: {len(processing)}")
        print()

        # Show pending jobs
        if pending:
            print("📋 PENDING JOBS (newest first):")
            for filename in sorted(pending, reverse=True)[:10]:
                try:
                    with open(os.path.join(queue_dir, filename)) as f:
                        job = json.load(f)
                        print(f"  • {job.get('type', 'unknown'):20} {job.get('file_url', 'N/A')[:60]}")
                except:
                    print(f"  • Error reading {filename}")
            if len(pending) > 10:
                print(f"  ... and {len(pending) - 10} more")
            print()

        # Show processing jobs
        if processing:
            print("⚙️  PROCESSING JOBS:")
            for filename in sorted(processing)[:5]:
                try:
                    filepath = os.path.join(queue_dir, filename)
                    mtime = os.path.getmtime(filepath)
                    age = int(time.time() - mtime)
                    with open(filepath) as f:
                        job = json.load(f)
                        print(f"  • {job.get('type', 'unknown'):20} {job.get('file_url', 'N/A')[:40]} (age: {age}s)")
                except:
                    print(f"  • Error reading {filename}")
            if len(processing) > 5:
                print(f"  ... and {len(processing) - 5} more")
            print()

        # Show errors directory
        error_dir = os.path.join(queue_dir, 'errors')
        if os.path.exists(error_dir):
            error_count = len([f for f in os.listdir(error_dir) if f.startswith('job-') or f.startswith('failed-')])
            if error_count > 0:
                print(f"❌ ERROR JOBS: {error_count}")
                print()

        print("Press Ctrl+C to exit. Refreshing every 2 seconds...")
        time.sleep(2)


def monitor_azure_servicebus():
    """Monitor Azure Service Bus queue"""
    from azure.servicebus import ServiceBusClient
    from azure.servicebus.exceptions import ServiceBusError

    conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
    if not conn_str:
        print("AZURE_SERVICEBUS_CONNECTION_STRING not set in .env")
        sys.exit(1)

    try:
        client = ServiceBusClient.from_connection_string(conn_str)

        while True:
            os.system('clear')
            print("=" * 70)
            print(f"AZURE SERVICE BUS MONITOR - {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)
            print()

            try:
                # Get queue runtime properties
                with client.get_queue_receiver("jobs", max_wait_time=1) as receiver:
                    # Peek at messages without consuming
                    messages = receiver.peek_messages(max_message_count=10)

                    print(f"📊 QUEUE STATUS")
                    print(f"  Messages in queue: {len(messages)}")
                    print()

                    if messages:
                        print("📋 MESSAGES (peeked, not consumed):")
                        for msg in messages:
                            try:
                                content = json.loads(str(msg))
                                print(f"  • {content.get('type', 'unknown'):20} {content.get('file_url', 'N/A')[:60]}")
                                print(f"    Enqueued: {msg.enqueued_time_utc}")
                            except:
                                print(f"  • Error parsing message")
                        print()

            except ServiceBusError as e:
                print(f"Error accessing queue: {e}")

            print("Press Ctrl+C to exit. Refreshing every 5 seconds...")
            time.sleep(5)

    except Exception as e:
        print(f"Failed to connect to Service Bus: {e}")
        sys.exit(1)


def monitor_azure_storage():
    """Monitor Azure Storage Queue"""
    from azure.storage.queue import QueueServiceClient

    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not conn_str:
        # Use default Azurite connection string
        conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"

    try:
        queue_client = QueueServiceClient.from_connection_string(conn_str).get_queue_client("jobs")

        while True:
            os.system('clear')
            print("=" * 70)
            print(f"AZURE STORAGE QUEUE MONITOR - {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)
            print()

            try:
                # Get queue properties
                properties = queue_client.get_queue_properties()
                print(f"📊 QUEUE STATUS")
                print(f"  Approximate message count: {properties['approximate_message_count']}")
                print()

                # Peek at messages
                messages = queue_client.peek_messages(max_messages=10)

                if messages:
                    print("📋 MESSAGES (peeked, not consumed):")
                    for msg in messages:
                        try:
                            content = json.loads(msg.content)
                            print(f"  • {content.get('type', 'unknown'):20} {content.get('file_url', 'N/A')[:60]}")
                            print(f"    Inserted: {msg.inserted_on}")
                        except:
                            print(f"  • Error parsing message")
                    print()

            except Exception as e:
                print(f"Error accessing queue: {e}")

            print("Press Ctrl+C to exit. Refreshing every 5 seconds...")
            time.sleep(5)

    except Exception as e:
        print(f"Failed to connect to Storage Queue: {e}")
        sys.exit(1)


def main():
    queue_type = os.getenv('QUEUE_TYPE', 'file').lower()

    print(f"Starting monitor for queue type: {queue_type}")
    print()

    try:
        if queue_type == 'file':
            monitor_file_queue()
        elif queue_type == 'servicebus':
            monitor_azure_servicebus()
        elif queue_type == 'storage':
            monitor_azure_storage()
        else:
            print(f"Unknown queue type: {queue_type}")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")


if __name__ == '__main__':
    main()