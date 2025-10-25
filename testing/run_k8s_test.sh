#!/bin/bash

# Script to run test_dynamic_updates.py with all required services

echo "================================================"
echo "Running Dynamic Updates Test"
echo "================================================"
echo ""
echo "This will start all required services and run the test"
echo ""

# Load environment variables from .env if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "ERROR: .env file not found!"
    echo ""
    echo "Please create .env file with your Azure credentials:"
    echo "  cp .env.example .env"
    echo "  # Edit .env with your Azure credentials"
    echo ""
    echo "Required variables for Azure Service Bus:"
    echo "  QUEUE_TYPE=servicebus"
    echo "  AZURE_SERVICEBUS_NAMESPACE=your-namespace"
    echo "  AZURE_SERVICE_BUS_QUEUE_NAME=your-queue-name"
    echo "  (Plus Azure AD credentials or connection string)"
    exit 1
fi

# Verify Azure Service Bus configuration
if [ "$QUEUE_TYPE" != "servicebus" ]; then
    echo "ERROR: QUEUE_TYPE must be set to 'servicebus' in .env"
    echo "Current value: $QUEUE_TYPE"
    exit 1
fi

if [ -z "$AZURE_SERVICEBUS_NAMESPACE" ] && [ -z "$AZURE_SERVICEBUS_CONNECTION_STRING" ]; then
    echo "ERROR: Azure Service Bus not configured!"
    echo "Set either:"
    echo "  AZURE_SERVICEBUS_NAMESPACE (for Azure AD auth)"
    echo "  or"
    echo "  AZURE_SERVICEBUS_CONNECTION_STRING (for connection string auth)"
    exit 1
fi

echo "Using Azure Service Bus queue..."
echo "  Namespace: ${AZURE_SERVICEBUS_NAMESPACE:-from connection string}"
echo "  Queue: ${AZURE_SERVICE_BUS_QUEUE_NAME:-jobs}"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping all services..."

    # Kill all background processes
    jobs -p | xargs -I {} kill {} 2>/dev/null

    # Give processes time to shut down
    sleep 2

    # Force kill if still running
    jobs -p | xargs -I {} kill -9 {} 2>/dev/null

    echo "All services stopped"
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Verify test data exists
if [ ! -d "data/backcountry_com" ]; then
    echo "ERROR: Test data not found in data/backcountry_com"
    echo "Please ensure test data is available"
    exit 1
fi

# Kill any existing processes on required ports
echo "Checking for existing processes on required ports..."
for port in 8000 5001; do
    PID=$(lsof -ti :$port 2>/dev/null)
    if [ ! -z "$PID" ]; then
        echo "  Killing process on port $port (PID: $PID)"
        kill -9 $PID 2>/dev/null
        sleep 1
    fi
done

# Clear Azure Service Bus queue
echo "Clearing Azure Service Bus queue..."
python3 -c "
import sys
sys.path.insert(0, 'code/core')
import config
import os

queue_type = os.getenv('QUEUE_TYPE', 'file').lower()
print(f'Queue type: {queue_type}')

if queue_type == 'servicebus':
    try:
        # Try Azure AD auth first
        namespace = os.getenv('AZURE_SERVICEBUS_NAMESPACE')
        if namespace:
            print(f'Using Azure AD auth with namespace: {namespace}')
            from azure.identity import DefaultAzureCredential
            from azure.servicebus import ServiceBusClient

            credential = DefaultAzureCredential()
            client = ServiceBusClient(
                fully_qualified_namespace=f'{namespace}.servicebus.windows.net',
                credential=credential
            )
            queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'jobs')

            with client.get_queue_receiver(queue_name=queue_name, max_wait_time=1) as receiver:
                messages = receiver.receive_messages(max_message_count=1000, max_wait_time=1)
                count = 0
                while messages:
                    for message in messages:
                        receiver.complete_message(message)
                        count += 1
                    messages = receiver.receive_messages(max_message_count=1000, max_wait_time=1)
                print(f'Cleared {count} messages from Service Bus queue: {queue_name}')
        else:
            # Try connection string
            conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
            if conn_str:
                print('Using connection string auth')
                from azure.servicebus import ServiceBusClient

                client = ServiceBusClient.from_connection_string(conn_str)
                queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'jobs')

                with client.get_queue_receiver(queue_name=queue_name, max_wait_time=1) as receiver:
                    messages = receiver.receive_messages(max_message_count=1000, max_wait_time=1)
                    count = 0
                    while messages:
                        for message in messages:
                            receiver.complete_message(message)
                            count += 1
                        messages = receiver.receive_messages(max_message_count=1000, max_wait_time=1)
                    print(f'Cleared {count} messages from Service Bus queue: {queue_name}')
    except Exception as e:
        print(f'Warning: Could not clear Service Bus queue: {e}')
        print('Queue may already be empty or not accessible')

# Clear database
print('Clearing database...')
import db
conn = db.get_connection()
db.clear_all_data(conn)
conn.close()
print('Database cleared')
"

echo ""
echo "Starting services..."
echo ""

# Start test data server in background
echo "Starting test data server on port 8000..."
python3 test_data_server.py &
DATA_SERVER_PID=$!
sleep 2

# Start master service (API + scheduler) in background
echo "Starting master service (API + scheduler) on port 5001..."
python3 code/core/api.py &
MASTER_PID=$!
sleep 3

# Start worker in background
echo "Starting worker service..."
python3 code/core/worker.py &
WORKER_PID=$!
sleep 2

echo ""
echo "All services started!"
echo "  Test data server: PID $DATA_SERVER_PID"
echo "  Master service: PID $MASTER_PID"
echo "  Worker service: PID $WORKER_PID"
echo ""

# Wait a bit for services to fully initialize
echo "Waiting for services to initialize..."
sleep 5

# Check if API is responding
echo "Checking API health..."
curl -s http://localhost:5001/api/status > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: API is not responding!"
    echo "Check the logs above for errors"
    exit 1
fi

echo ""
echo "================================================"
echo "Running test..."
echo "================================================"
echo ""

# Run the test
python3 test_dynamic_updates.py

# Test exit code
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo ""
    echo "✅ Test completed successfully!"
else
    echo ""
    echo "❌ Test failed!"
fi

echo ""
echo "Services will be stopped automatically..."
exit $TEST_RESULT