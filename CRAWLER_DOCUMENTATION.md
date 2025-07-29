# Web Crawler System Documentation

## Overview
This is a multi-site web crawling system with a Flask-based frontend and an async crawler backend. The system crawls websites based on their sitemaps and stores both HTML content and structured data (schema.org JSON-LD).

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
- Avoids consecutive calls to same website
- Extracts and stores schema.org JSON-LD data

## Directory Structure

```
crawler/
├── urls/           # URL lists per site
│   └── {site_name}.txt
├── docs/           # Crawled HTML content
│   └── {site_name}/
│       └── {page_files}
├── json/           # Schema.org JSON-LD data
│   └── {site_name}.json
├── status/         # Crawl status per site
│   └── {site_name}.json
└── templates/      # Flask HTML templates
```

### Directory Details:

1. **urls/** - Contains text files with one URL per line for each site
2. **docs/** - Stores crawled HTML pages organized by site
3. **json/** - Aggregates all schema.org JSON-LD data found on each site
4. **status/** - JSON files tracking crawl progress:
   ```json
   {
     "total_urls": 147708,
     "crawled_urls": 0,
     "paused": false,
     "last_updated": "2025-07-23T15:49:26.033938"
   }
   ```

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
1. Read URLs from all files in urls/ directory
2. For each URL:
   - Check if already crawled
   - Check if site is paused
   - Fetch page content
   - Extract schema.org JSON-LD
   - Save HTML to docs/{site_name}/
   - Append JSON-LD to json/{site_name}.json
   - Update status file

## Current Implementation Status

### Completed:
- ✅ Directory setup (setup_directories.py)
- ✅ Flask frontend with URL/sitemap processing
- ✅ Sitemap parsing with recursive handling
- ✅ Status file management
- ✅ URL collection and storage
- ✅ Basic crawler structure

### Missing/Incomplete:
- ❌ URL reader thread not monitoring for new files
- ❌ Worker thread pool not fully implemented
- ❌ Schema.org extraction not implemented
- ❌ Duplicate detection not working
- ❌ Site-based request throttling not implemented
- ❌ Pause/resume functionality in crawler not connected

## Usage Instructions

1. **Start the Flask app**:
   ```bash
   python app.py
   ```

2. **Add sites to crawl**:
   - Navigate to http://localhost:5000
   - Enter website URL or sitemap URL
   - Optionally add filter text
   - Submit to collect URLs

3. **Monitor status**:
   - Check /status page for all sites
   - View individual site status at /status/<site_name>

4. **Run the crawler**:
   ```bash
   python crawler.py
   ```

## Notes for Resume
- The system is designed for defensive security analysis and content monitoring
- URLs are collected first, then crawled asynchronously
- Each site maintains independent status and can be paused
- The crawler respects robots.txt by using sitemaps for URL discovery