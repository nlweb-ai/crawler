# Web Crawler System Documentation

## Overview
This is a multi-site web crawling system with a Flask-based frontend and an async crawler backend. The system crawls websites based on their sitemaps, stores HTML content and structured data (schema.org JSON-LD), generates embeddings using the NLWeb submodule, and inserts vectors into a vector database for semantic search capabilities.

## Architecture Components

### 1. Frontend (app.py)
- Flask web application for managing crawl operations
- Accepts website URLs or direct sitemap URLs
- Extracts URLs from robots.txt and sitemaps
- Provides status monitoring and pause/resume functionality

### 2. Crawler Backend (crawler.py)
- Asynchronous crawler using aiohttp
- Multi-threaded architecture:
  - URL reader thread: Monitors URL files for new sites
  - Worker thread: Performs actual crawling with max 10 concurrent requests
  - Embeddings monitor thread: Monitors JSON files for embedding generation
  - Database monitor thread: Monitors embedding files for database insertion
- Avoids consecutive calls to same website
- Extracts and stores schema.org JSON-LD data
- Generates embeddings using NLWeb submodule
- Uploads vectors to configured vector database

### 3. NLWeb Submodule Integration
- Reuses database loading utilities from adjacent NLWeb project
- Imports embedding libraries for consistent vectorization
- Provides `setup_submodule_path.py` for automatic path configuration
- Enables seamless integration with NLWeb AI platform

## Directory Structure

```
crawler/
├── data/
│   ├── urls/           # URL lists per site
│   │   └── {site_name}.txt
│   ├── docs/           # Crawled HTML content
│   │   └── {site_name}/
│   │       └── {page_files}
│   ├── json/           # Schema.org JSON-LD data
│   │   └── {site_name}.json
│   ├── embeddings/     # Generated embeddings
│   │   └── {site_name}.json
│   ├── keys/           # Processed item tracking
│   │   └── {site_name}.json
│   └── status/         # Crawl status per site
│       └── {site_name}.json
├── nlweb-submodule/    # NLWeb library integration
├── config/             # Configuration files
├── logs/               # Application logs
└── templates/          # Flask HTML templates
```

### Directory Details:

1. **data/urls/** - Contains text files with one URL per line for each site
2. **data/docs/** - Stores crawled HTML pages organized by site
3. **data/json/** - Aggregates all schema.org JSON-LD data found on each site
4. **data/embeddings/** - Contains generated embeddings for each site's content
5. **data/keys/** - Tracks processed items to avoid duplicate processing
6. **data/status/** - JSON files tracking crawl progress:
   ```json
   {
     "total_urls": 147708,
     "crawled_urls": 0,
     "paused": false,
     "last_updated": "2025-07-23T15:49:26.033938",
     "json_stats": {
       "total_objects": 1250,
       "type_counts": {"Article": 800, "WebPage": 450}
     }
   }
   ```
7. **nlweb-submodule/** - Git submodule containing NLWeb library code
8. **config/** - YAML configuration files for various components
9. **logs/** - Application logs for debugging and monitoring

## Frontend Features

### URL Processing
1. **Website Input**: 
   - Fetches robots.txt
   - Extracts sitemap URLs
   - Recursively processes sitemap index files
   
2. **Sitemap Input**:
   - Directly processes sitemap XML
   - Handles both regular sitemaps and sitemap indexes

3. **URL Filtering**:
   - Optional filter parameter to only include URLs containing specific text

### Web Endpoints
- `/` - Main input form
- `/process` - POST endpoint for processing URLs
- `/status/<site_name>` - Get crawl status for specific site
- `/sites` - List all sites being crawled
- `/toggle_pause/<site_name>` - Pause/resume crawling for a site

## Crawler Implementation

### Key Features:
1. **Concurrent Crawling**: Uses asyncio with max 10 parallel requests
2. **Duplicate Detection**: Checks if pages already crawled before fetching
3. **Schema.org Extraction**: Parses JSON-LD structured data from pages
4. **Site Isolation**: Avoids hammering single site with consecutive requests
5. **Pause/Resume**: Respects pause status from status files

### Crawl Process:
1. **URL Processing**: Read URLs from all files in data/urls/ directory
2. **Content Extraction**: For each URL:
   - Check if already crawled
   - Check if site is paused
   - Fetch page content
   - Extract schema.org JSON-LD
   - Save HTML to data/docs/{site_name}/
   - Append JSON-LD to data/json/{site_name}.json
   - Update status file

3. **Embedding Generation** (Embeddings Worker):
   - Monitor data/json/ files for new content
   - Generate embeddings using NLWeb submodule
   - Save embeddings to data/embeddings/{site_name}.json
   - Track processed items

4. **Database Insertion** (Database Worker):
   - Monitor data/embeddings/ files for new vectors
   - Upload documents with embeddings to vector database
   - Track processed keys in data/keys/{site_name}.json
   - Handle batch processing for efficiency

## Current Implementation Status

### Completed:
- ✅ Directory setup and data organization
- ✅ Flask frontend with URL/sitemap processing
- ✅ Sitemap parsing with recursive handling
- ✅ Status file management with enhanced tracking
- ✅ URL collection and storage
- ✅ Asynchronous crawler with worker threads
- ✅ Schema.org extraction (and synthesizing from meta tags)
- ✅ URL reader thread monitoring for new files
- ✅ **Embeddings worker** - generates embeddings using NLWeb submodule
- ✅ **Database worker** - uploads vectors to configured database
- ✅ **NLWeb submodule integration** - reuses database and embedding libraries
- ✅ Pause/resume functionality for individual sites
- ✅ **Enhanced site deletion** - removes from both local storage and vector database
- ✅ Multi-threaded monitoring system
- ✅ Comprehensive error tracking and logging
- ✅ Docker containerization with Qdrant integration

### Architecture Improvements:
- ✅ **Embeddings Monitor Thread**: Watches JSON files and queues embedding generation
- ✅ **Database Monitor Thread**: Watches embedding files and queues database insertion
- ✅ **Submodule Path Setup**: Automatic configuration for NLWeb library imports
- ✅ **Vector Database Integration**: Full CRUD operations through NLWeb submodule
- ✅ **Batch Processing**: Efficient handling of large datasets

### Known Limitations:
- ⚠️ **Database Locking**: Cannot use Qdrant local simultaneously with NLWeb (use Qdrant server)
- ⚠️ **Duplicate Detection**: Basic filename-based detection (could be enhanced)
- ⚠️ **Rate Limiting**: Basic domain-based throttling (could be more sophisticated)

## Usage Instructions

### Prerequisites
- Python 3.8+
- Git with submodule support
- Docker and Docker Compose (for containerized deployment)
- Vector database (Qdrant recommended)

### Installation

1. **Clone the repository with submodules**:
   ```bash
   git clone --recurse-submodules https://github.com/nlweb-ai/crawler.git
   cd crawler
   ```

   Or if already cloned:
   ```bash
   git clone https://github.com/nlweb-ai/crawler.git
   cd crawler
   git submodule update --init --recursive
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your Qdrant URL, API keys, etc.
   ```

3. **Choose deployment method**:

   **Option A: Docker (Recommended)**
   ```bash
   # With Qdrant container
   docker-compose -f docker-compose.yml -f docker-compose.qdrant.yml up --build

   # Or with external Qdrant
   docker-compose up --build
   ```

   **Option B: Local Development**
   ```bash
   pip install -r requirements.txt
   python run.py
   ```

### Using the Crawler

1. **Access the web interface**: Navigate to http://localhost:5000

2. **Add sites to crawl**:
   - Enter website URL or direct sitemap URL
   - Optionally add filter text to include only specific URLs
   - Click "Process" to start URL collection and crawling

3. **Monitor progress**:
   - View the main status page for all active crawls
   - Check individual site progress and statistics
   - Monitor real-time logs for detailed crawling information

4. **Manage sites**:
   - Pause/resume individual site crawls
   - Delete sites (removes all data including vectors from database)
   - Restart crawls from scratch
   - View extracted schema.org data


## NLWeb Submodule Integration

### Purpose
The crawler integrates with the NLWeb project through a Git submodule to:
- Reuse proven database loading utilities
- Leverage consistent embedding generation
- Maintain compatibility with the NLWeb AI platform
- Avoid code duplication

### How It Works
The `setup_submodule_path.py` module automatically configures Python imports:

```python
import setup_submodule_path  # This automatically sets up the submodule path
from core.embedding import get_embedding
from core.retriever import upload_documents, delete_documents_by_site
```

### Key Integrations
1. **Embedding Generation**: Uses `core.embedding.get_embedding()` for consistent vectorization
2. **Database Operations**: Uses `core.retriever` for upload/delete operations
3. **Configuration**: Shares configuration patterns with NLWeb

### Updating the Submodule
```bash
# Get latest updates from NLWeb
git submodule update --remote nlweb-submodule

# Or update to specific commit
cd nlweb-submodule
git checkout <commit-hash>
cd ..
git add nlweb-submodule
git commit -m "Update NLWeb submodule"
```

## Database Considerations

### Qdrant Integration
- **Shared Database**: Both crawler and NLWeb can use the same Qdrant instance
- **Network Configuration**: Uses `qdrant-net` Docker network for communication
- **Authentication**: Shared API key through environment variables

### Important Limitations
⚠️ **Single Connection Limit**: Qdrant local database only supports one connection at a time. For concurrent use with NLWeb:
- Use Qdrant server (Docker container) instead of local
- Configure both applications to use the same Qdrant URL
- Ensure consistent API key configuration

### Recommended Setup
```yaml
# docker-compose.yml
services:
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    environment:
      QDRANT__SERVICE__API_KEY: "your_secret_key"
    networks:
      - qdrant-net

networks:
  qdrant-net:
    external: true
```

## Technical Notes
- The system is designed for content analysis and semantic search
- URLs are collected first, then crawled asynchronously with embedding generation
- Each site maintains independent status and can be paused/resumed
- The crawler respects robots.txt by using sitemaps for URL discovery
- Vector database operations are handled through the NLWeb submodule for consistency