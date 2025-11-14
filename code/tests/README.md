# Unit Tests

Unit tests for the core crawler application.

## Files

### `test_master.py`
Tests for master service logic:
- Schema map discovery from robots.txt
- XML parsing for schema maps
- Job creation and queueing
- Site processing flows

### `mockdata/generate_test_data.py`
Utility to generate test data files for unit tests.

## Running Tests

```bash
# Run all tests
python3 -m pytest code/tests/

# Run specific test file
python3 -m pytest code/tests/test_master.py

# Run with verbose output
python3 -m pytest code/tests/ -v

# Run with coverage
python3 -m pytest code/tests/ --cov=code/core --cov-report=html
```

## Test Data

Test data is stored in the `mockdata/` subdirectory.

## Adding New Tests

1. Create a new test file: `test_<module>.py`
2. Import the module you want to test
3. Write test functions starting with `test_`
4. Use pytest fixtures for setup/teardown

Example:
```python
import pytest
from code.core import db

def test_add_site():
    conn = db.get_connection()
    db.add_site(conn, 'https://example.com', 'test_user')
    # Assert expected behavior
    conn.close()
```

## Integration Tests

For integration tests that require Azure services, see the `/testing` directory at the project root.
