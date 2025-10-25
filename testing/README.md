# Testing and Monitoring Scripts

This directory contains all testing, monitoring, and development utilities for the crawler.

## Directory Structure

```
testing/
├── monitoring/          # Scripts for monitoring running system
├── setup/              # Setup and configuration utilities
├── data/               # Test data server and utilities
├── *.py                # Main test scripts
└── run_k8s_test.sh     # Main test runner
```

## Quick Start

### Run Complete Test Suite
```bash
./testing/run_k8s_test.sh
```

### Start Test Data Server
```bash
./testing/data/start_test_data_server.sh
```

## Monitoring Scripts (`monitoring/`)

- **`check_status.py`** - Check crawler system status
- **`monitor_progress.py`** - Monitor crawling progress
- **`monitor_queue.py`** - Monitor queue status
- **`debug_worker.py`** - Debug worker processes

## Test Scripts

- **`test_dynamic_updates.py`** - Test dynamic file updates
- **`test_file_removal.py`** - Test file removal handling
- **`test_job_recovery.py`** - Test job recovery mechanisms
- **`launch_test.py`** - Launch test scenarios

## Setup Utilities (`setup/`)

- **`setup_azure_servicebus.sh`** - Set up Azure Service Bus
- **`setup_azure_servicebus_aad.sh`** - Set up Service Bus with AAD
- **`setup_env.sh`** - Configure environment variables
- **`install_pyodbc_mac.sh`** - Install ODBC drivers on Mac
- **`remove_fk_constraint.py`** - Database migration utility

## Data Server (`data/`)

- **`test_data_server.py`** - Python server for test data
- **`start_test_data_server.sh`** - Start the test data server

## Local Testing

- **`local-setup.sh`** - Set up local testing environment
- **`run_k8s_test.sh`** - Run tests with Kubernetes-like setup

## Helper Scripts

- **`start_api_server.py`** - Start API server for testing
- **`start_data_server.py`** - Start data server for testing
- **`start_worker.py`** - Start worker for testing
- **`run.py`** - Legacy launcher (deprecated)

## Running Tests

### Full Test with Azure Services
```bash
# Ensure .env is configured with Azure credentials
./testing/run_k8s_test.sh
```

### Local Testing (No Azure)
```bash
# Set up local environment
./testing/local-setup.sh

# Start test data server
./testing/data/start_test_data_server.sh

# Run specific test
python3 testing/test_dynamic_updates.py
```

### Monitor Running System
```bash
# Check overall status
python3 testing/monitoring/check_status.py

# Monitor queue
python3 testing/monitoring/monitor_queue.py

# Watch progress
python3 testing/monitoring/monitor_progress.py
```

## Test Data

Test data is stored in `/data` directory with sample schema.org files for:
- backcountry_com
- hebbarskitchen_com
- imdb_com
- tripadvisor_com

## Environment Setup

Tests require environment variables configured in `.env`:
- Azure Service Bus credentials
- SQL Database connection
- Storage account details
- Azure AD credentials (if using AAD auth)

See `.env.example` for required variables.