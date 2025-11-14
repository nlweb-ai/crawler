# Core Crawler Application

This directory contains the core business logic for the distributed schema.org crawler.

## Architecture Overview

The system consists of two main components:
- **Master Service** - API server, scheduler, and job creator
- **Worker Service(s)** - Distributed job processors

## File Descriptions

### Main Services

#### `api.py` (977 lines)
Flask REST API server - the Master service entry point.

**Key Features:**
- OAuth authentication (GitHub, Microsoft) + API key support
- Site management endpoints (add, delete, list)
- Schema file management (add/remove schema maps)
- Queue and worker monitoring
- Background scheduler integration
- Web UI hosting

**Main Routes:**
- `GET /` - Web dashboard
- `GET /login` - OAuth login page
- `POST /api/sites` - Add site to monitor
- `DELETE /api/sites/<url>` - Remove site
- `POST /api/sites/<url>/schema-files` - Add schema map manually
- `GET /api/status` - System status
- `GET /api/queue/status` - Queue statistics
- `GET /api/workers` - Worker pod status (via Kubernetes API)

**Authentication:**
- Session-based (OAuth flow)
- Header-based (`X-API-Key` header)

**Startup:**
```bash
python3 api.py
# or
./start_master.sh
```

---

#### `worker.py` (469 lines)
Distributed job processor - the Worker service.

**Job Types:**
1. `process_file` - Fetch JSON-LD file, extract IDs, update DB and vector DB
2. `process_removed_file` - Clean up deleted files from DB and vector DB

**Processing Flow:**
```
1. Receive job from queue (visibility timeout: 5 min)
2. Check if file still exists in DB (skip if deleted)
3. Fetch JSON-LD from URL
4. Extract @id values (handles arrays, single objects, @graph)
5. Update database with new IDs
6. Reference counting:
   - First occurrence → Add to vector DB with embedding
   - Last occurrence removed → Delete from vector DB
7. Delete message from queue (or return on error)
```

**Logging:**
- `/app/data/fetch_log.jsonl` - All URL fetches with status
- `/app/data/vector_db_additions.jsonl` - Items added to vector DB

**Status Endpoint:**
- Port 8080: `/status` - Worker status, current job, stats
- Port 8080: `/health` - Health check

**Startup:**
```bash
python3 worker.py
# or
./start_worker.sh
```

---

#### `master.py` (264 lines)
Site discovery and job creation logic.

**Two-Level Processing:**

**Level 1: `process_site(site_url, user_id)`**
- Discovers schema maps from robots.txt (looks for `schemaMap:` directives)
- Falls back to `/schema_map.xml` if not in robots.txt
- Calls Level 2 for each discovered schema map

**Level 2: `add_schema_map_to_site(site_url, user_id, schema_map_url)`**
- Fetches and parses schema_map.xml
- Extracts all JSON file URLs from XML
- Adds files to database
- Queues `process_file` jobs for new files
- Queues `process_removed_file` jobs for deleted files

**Schema Map XML Parsing:**
- Handles sitemap.org namespace (http://www.sitemaps.org/schemas/sitemap/0.9)
- Also works without namespace
- Looks for `<url>` elements with `contentType` containing "schema.org"

**Usage:**
```python
from master import process_site, add_schema_map_to_site

# Discover and process automatically
process_site('https://example.com', user_id='github:12345')

# Add specific schema map
add_schema_map_to_site(
    'https://example.com',
    'github:12345',
    'https://example.com/schema_map.xml'
)
```

---

### Supporting Modules

#### `auth.py` (197 lines)
Hybrid authentication system.

**Features:**
- Flask-Login for session management
- OAuth integration (GitHub, Microsoft via Authlib)
- API key generation and validation
- User management (auto-creates users on first login)

**Usage:**
```python
from auth import require_auth, get_current_user

@app.route('/api/protected')
@require_auth
def protected_route():
    user_id = get_current_user()  # Returns user_id string
    # ... your code
```

**Authentication Methods:**
1. **OAuth Session** - User logged in via GitHub/Microsoft
2. **API Key** - Header: `X-API-Key: <key>`

**Environment Variables:**
- `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET` - GitHub OAuth
- `MICROSOFT_CLIENT_ID`, `MICROSOFT_CLIENT_SECRET` - Microsoft OAuth
- `MICROSOFT_TENANT_ID` - Optional, defaults to 'common'

---

#### `db.py` (443 lines)
Database abstraction layer for Azure SQL.

**Connection:**
- Uses `pymssql` (simpler than ODBC)
- Environment: `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`

**Tables:**
```sql
users (
    user_id VARCHAR(255) PRIMARY KEY,  -- Format: "github:12345" or "microsoft:abc-def"
    email VARCHAR(255),
    name VARCHAR(255),
    provider VARCHAR(50),              -- 'github' or 'microsoft'
    api_key VARCHAR(64) UNIQUE,        -- Auto-generated on user creation
    created_at DATETIME,
    last_login DATETIME
)

sites (
    site_url VARCHAR(500),
    user_id VARCHAR(255),
    process_interval_hours INT DEFAULT 24,
    last_processed DATETIME,
    is_active BIT DEFAULT 1,
    created_at DATETIME,
    PRIMARY KEY (site_url, user_id)
)

files (
    site_url VARCHAR(500),
    user_id VARCHAR(255),
    file_url VARCHAR(500),             -- JSON-LD file URL
    schema_map VARCHAR(500),           -- Schema map that contains this file
    last_read_time DATETIME,
    number_of_items INT,
    is_manual BIT DEFAULT 0,           -- Manually added vs auto-discovered
    is_active BIT DEFAULT 1,           -- Soft delete
    PRIMARY KEY (file_url, user_id)
)

ids (
    file_url VARCHAR(500),
    user_id VARCHAR(255),
    id VARCHAR(500)                    -- @id value from JSON-LD
    -- No PK: allows duplicates for reference counting
)
```

**Key Functions:**
- `get_connection()` - Get database connection
- `create_tables(conn)` - Initialize schema
- `add_site(conn, site_url, user_id, interval_hours)` - Add site
- `update_site_files(conn, site_url, user_id, files)` - Update files (returns added/removed)
- `update_file_ids(conn, file_url, user_id, ids)` - Update IDs (returns added/removed)
- `count_id_references(conn, id, user_id)` - Count references for deduplication

**Concurrency:**
- Per-site semaphores prevent concurrent modifications to same site
- MERGE statements for atomic upserts

---

#### `scheduler.py` (61 lines)
Scheduled site reprocessing.

**Logic:**
- Queries sites where `last_processed + interval_hours <= now`
- Processes each site via `master.process_site()`
- Updates `last_processed` timestamp

**Note:** This is a standalone version. Production uses the async scheduler in `api.py`.

---

#### `config.py` (35 lines)
Environment variable loader.

**Behavior:**
- Searches for `.env` file in project root (`/Users/rvguha/code/crawler/.env`)
- Parses `KEY=VALUE` format
- Only sets variables NOT already in environment
- Auto-loads on module import

**Usage:**
```python
import config  # Automatically loads .env
import os
value = os.getenv('MY_VAR')
```

---

### Queue Implementations

#### `queue_interface.py` (281 lines)
Abstract queue interface with multiple backends.

**Interface:**
```python
class QueueInterface(abc.ABC):
    def send_message(message: dict) -> bool
    def receive_message(visibility_timeout: int) -> QueueMessage
    def delete_message(message: QueueMessage) -> bool
    def return_message(message: QueueMessage) -> bool
```

**Implementations:**

1. **FileQueue** - Local development
   - Uses filesystem with atomic renames
   - Queue directory: `./queue/`
   - Files: `job-TIMESTAMP.json` → `job-TIMESTAMP.json.processing`

2. **AzureServiceBusQueue** - Production (connection string auth)
   - Env: `AZURE_SERVICEBUS_CONNECTION_STRING`
   - Queue name: `AZURE_SERVICE_BUS_QUEUE_NAME`

3. **AzureStorageQueue** - Alternative (connection string auth)
   - Env: `AZURE_STORAGE_CONNECTION_STRING`
   - Queue name: `AZURE_STORAGE_QUEUE_NAME`

**Factory:**
```python
from queue_interface import get_queue

queue = get_queue()  # Returns appropriate implementation based on QUEUE_TYPE
queue.send_message({'type': 'process_file', 'file_url': '...'})
```

**Environment:**
- `QUEUE_TYPE` - 'file', 'servicebus', or 'storage' (default: 'file')

---

#### `queue_interface_aad.py` (100+ lines)
Azure Service Bus with Azure AD authentication.

**Preferred for Production Kubernetes:**
- Uses `DefaultAzureCredential` (no connection strings needed)
- Supports: Workload Identity, Managed Identity, Azure CLI
- Env: `AZURE_SERVICEBUS_NAMESPACE`, `AZURE_SERVICE_BUS_QUEUE_NAME`

**Usage:**
```python
from queue_interface_aad import get_queue_with_aad

queue = get_queue_with_aad()
```

---

#### `queue_interface_storage.py`
Azure Storage Queue with Azure AD authentication.

**Alternative to Service Bus:**
- Uses `DefaultAzureCredential`
- Works with Azurite for local development
- Env: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_QUEUE_NAME`

---

### Vector Database

#### `vector_db.py` (281 lines)
Azure Cognitive Search vector database integration.

**Features:**
- Auto-creates search index with HNSW vector search
- Generates embeddings via Azure OpenAI
- Batch operations (100 items per batch)
- Hash URLs for Azure Search document keys (SHA-256)

**Environment:**
- `AZURE_SEARCH_ENDPOINT` - Search service endpoint
- `AZURE_SEARCH_KEY` - Admin key
- `AZURE_SEARCH_INDEX_NAME` - Index name (default: 'crawler-vectors')
- `AZURE_OPENAI_ENDPOINT` - OpenAI endpoint
- `AZURE_OPENAI_KEY` - OpenAI key
- `AZURE_OPENAI_EMBEDDING_DEPLOYMENT` - Deployment name (default: 'text-embedding-3-small')

**Public API (synchronous):**
```python
from vector_db import vector_db_add, vector_db_delete, vector_db_batch_add, vector_db_batch_delete

# Add single item
vector_db_add(id='https://example.com/thing/1', site='https://example.com', json_obj={...})

# Delete single item
vector_db_delete(id='https://example.com/thing/1')

# Batch operations
vector_db_batch_add([(id1, site1, obj1), (id2, site2, obj2), ...])
vector_db_batch_delete([id1, id2, ...])
```

**Index Schema:**
- `id` - SHA-256 hash of URL (document key)
- `url` - Original @id from JSON-LD
- `site` - Site URL
- `type` - @type from JSON-LD
- `content` - Searchable text representation
- `timestamp` - When indexed
- `embedding` - 1536-dimension vector

---

#### `embedding_provider/azure_oai_embedding.py` (66 lines)
Azure OpenAI embedding generation.

**Features:**
- Async Azure OpenAI client
- Single and batch embedding generation
- Uses `text-embedding-3-small` by default

**Usage:**
```python
from embedding_provider.azure_oai_embedding import AzureOpenAIEmbedding

provider = AzureOpenAIEmbedding(
    endpoint='https://....openai.azure.com/',
    api_key='...',
    deployment='text-embedding-3-small'
)

# Single
embedding = await provider.get_embedding('some text')  # Returns List[float]

# Batch
embeddings = await provider.get_batch_embeddings(['text1', 'text2'])  # Returns List[List[float]]
```

---

### Other Files

#### `job_manager.py`
Job management utilities (likely deprecated - not used in current architecture).

---

## Data Flow

### Adding a New Site

```
1. User: POST /api/sites {"site_url": "https://example.com"}
   ↓
2. api.py: db.add_site() → master.process_site() (async)
   ↓
3. master.py:
   - Fetch robots.txt
   - Extract schemaMap URLs
   - For each schema map: add_schema_map_to_site()
   ↓
4. master.py: add_schema_map_to_site()
   - Fetch schema_map.xml
   - Parse → Extract JSON file URLs
   - db.update_site_files() → Returns added/removed files
   - For each added file: queue.send_message({type: 'process_file', ...})
   ↓
5. worker.py:
   - queue.receive_message()
   - Fetch JSON-LD file
   - Extract @id values
   - db.update_file_ids() → Returns added/removed IDs
   - For each added ID: Check ref_count
     - If ref_count == 1: vector_db_add() (with embedding)
   - For each removed ID: Check ref_count
     - If ref_count == 0: vector_db_delete()
   - queue.delete_message()
```

### Updating a Site

```
1. Scheduler: Process sites where last_processed + interval > now
   ↓
2. master.process_site() → Same flow as above
   ↓
3. db.update_site_files() detects changes:
   - New files → Queue 'process_file' jobs
   - Removed files → Queue 'process_removed_file' jobs
   ↓
4. Workers process both types:
   - process_file: Add new IDs, update vector DB
   - process_removed_file: Remove IDs, clean vector DB
```

## Environment Variables

### Required
```bash
# Database
DB_SERVER=yourserver.database.windows.net
DB_DATABASE=crawler
DB_USERNAME=admin
DB_PASSWORD=SecurePassword123!

# Queue (choose one)
QUEUE_TYPE=file|servicebus|storage

# If using Service Bus
AZURE_SERVICEBUS_NAMESPACE=yournamespace
AZURE_SERVICE_BUS_QUEUE_NAME=crawler-queue

# Or Storage Queue
AZURE_STORAGE_ACCOUNT_NAME=youraccount
AZURE_STORAGE_QUEUE_NAME=crawler-jobs
```

### Optional
```bash
# OAuth
GITHUB_CLIENT_ID=...
GITHUB_CLIENT_SECRET=...
MICROSOFT_CLIENT_ID=...
MICROSOFT_CLIENT_SECRET=...

# Vector DB
AZURE_SEARCH_ENDPOINT=https://yoursearch.search.windows.net
AZURE_SEARCH_KEY=...
AZURE_SEARCH_INDEX_NAME=crawler-vectors

# Embeddings
AZURE_OPENAI_ENDPOINT=https://....openai.azure.com/
AZURE_OPENAI_KEY=...
AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-3-small

# Flask
FLASK_SECRET_KEY=your-secret-key-for-sessions
API_PORT=5001

# Worker
WORKER_STATUS_PORT=8080
```

## Development

### Run Master Locally
```bash
python3 code/core/api.py
# Access: http://localhost:5001
```

### Run Worker Locally
```bash
python3 code/core/worker.py
```

### Run Tests
```bash
python3 -m pytest code/tests/
```

## Design Patterns

### Multi-Tenancy
All data is scoped to `user_id`. Every database query includes `user_id` in WHERE clause.

### Idempotency
- MERGE statements for upserts (files table)
- Reference counting prevents duplicate vector DB adds
- Jobs can be retried safely

### Concurrency Control
- Per-site semaphores prevent concurrent modifications
- Queue visibility timeout (5 min) for crash recovery
- Database connection pooling and recovery

### Error Handling
- Jobs return to queue on failure (via `return_message()`)
- Database connection recovery in workers
- Comprehensive logging to JSONL files
