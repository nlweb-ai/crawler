# Docker Setup for NLWeb Crawler

This directory contains Docker configuration files to run the NLWeb Crawler application with Qdrant vector database integration. The crawler now includes embeddings generation and vector database insertion capabilities through the NLWeb submodule.

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Git with submodule support
- At least 4GB RAM available for containers

### Option 1: With Qdrant Container (Recommended)

1. **Create the external network:**
   ```bash
   docker network create qdrant-net
   ```

2. **Build and start all services:**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.qdrant.yml up --build
   ```

3. **Access the applications:**
   - Crawler Web Interface: http://localhost:5000
   - Qdrant API: http://localhost:6333
   - Qdrant Web UI: http://localhost:6333/dashboard

### Option 2: With External Qdrant

1. **Configure external Qdrant URL in .env:**
   ```bash
   QDRANT_URL=http://your-qdrant-server:6333
   QDRANT_API_KEY=your_secret_key
   ```

2. **Start only the crawler:**
   ```bash
   docker-compose up --build
   ```

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Vector Database Configuration
QDRANT_URL=http://qdrant:6333
QDRANT_API_KEY=supersecret123

# Embedding Provider (choose one)
OPENAI_API_KEY=your_openai_api_key
AZURE_OPENAI_API_KEY=your_azure_openai_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/

# Application Settings
FLASK_ENV=development
PYTHONPATH=/app:/app

# Provider Selection
PREFERRED_RETRIEVAL_ENDPOINT=qdrant
PREFERRED_EMBEDDING_PROVIDER=openai
```

### Config Files

The `config/` directory is mounted to the container for easy configuration management:

- `config/config_retrieval.yaml` - Vector database endpoints and settings
- `config/config_embedding.yaml` - Embedding provider configurations
- `config/config_llm.yaml` - Language model settings
- `config/config_logging.yaml` - Logging configuration
- `config/config_webserver.yaml` - Web server settings

### Data Persistence

The following directories are mounted to the host for data persistence:

- `data/` - All crawler data including:
  - `data/urls/` - URL lists for each site
  - `data/docs/` - Crawled HTML content
  - `data/json/` - Extracted schema.org data
  - `data/embeddings/` - Generated embeddings
  - `data/keys/` - Processed item tracking
  - `data/status/` - Crawl status files
- `logs/` - Application logs for debugging
- `config/` - Configuration files

## Services

### crawler-app
- **Image:** Built from local Dockerfile with NLWeb submodule
- **Ports:** 5000:5000 (Web interface)
- **Volumes:**
  - `./config:/app/config` - Configuration files
  - `./data:/app/data` - Persistent data storage
  - `./logs:/app/logs` - Application logs
- **Environment:**
  - Points to Qdrant container via `QDRANT_URL=http://qdrant:6333`
  - Includes embedding provider API keys
- **Networks:** `qdrant-net`
- **Features:**
  - Multi-threaded web crawler
  - Embeddings generation worker
  - Database insertion worker
  - Real-time monitoring interface

### qdrant (Optional)
- **Image:** qdrant/qdrant:latest
- **Ports:**
  - 6333:6333 (REST API)
  - 6334:6334 (gRPC API)
- **Volume:** `qdrant_data:/qdrant/storage` (Persistent vector storage)
- **Environment:** `QDRANT__SERVICE__API_KEY` for authentication
- **Network:** `qdrant-net`
- **Features:**
  - Vector similarity search
  - Web dashboard at http://localhost:6333/dashboard
  - Persistent storage for embeddings

### Network Configuration

⚠️ **Important**: The `qdrant-net` network should be created as external so that both the crawler and NLWeb services can connect to the same Qdrant instance:

```bash
docker network create qdrant-net
```

This allows:
- Crawler and NLWeb to share the same vector database
- Consistent data access across applications
- Simplified deployment and management

## Development Mode

For development with live code reloading:

1. **Use the development override:**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.override.yml up --build
   ```

1.1 **Use the qdrant override:**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.qdrant.yml up --build
   ```

2. **Or simply use the default (override is loaded automatically):**
   ```bash
   docker-compose up --build
   ```

## Production Deployment

For production, you may want to:

1. **Disable the development override:**
   ```bash
   docker-compose -f docker-compose.yml up -d
   ```

2. **Use specific environment file:**
   ```bash
   docker-compose --env-file .env.production up -d
   ```

## Useful Commands

- **View logs:** `docker-compose logs -f crawler-app`
- **Stop services:** `docker-compose down`
- **Rebuild:** `docker-compose up --build`
- **Reset Qdrant data:** `docker-compose down -v` (removes volumes)

## Multi-Application Coordination

### Running with NLWeb

The crawler is designed to work alongside the main NLWeb application:

1. **Shared Qdrant Instance:**
   ```bash
   # Create shared network
   docker network create qdrant-net

   # Start Qdrant
   docker-compose -f docker-compose.qdrant.yml up -d qdrant

   # Start crawler
   docker-compose up crawler-app

   # Start NLWeb (in separate project)
   cd ../nlweb
   docker-compose up nlweb-app
   ```

2. **Environment Consistency:**
   Ensure both applications use the same:
   - `QDRANT_URL=http://qdrant:6333`
   - `QDRANT_API_KEY=supersecret123`
   - Embedding provider settings

### Database Compatibility Notes

⚠️ **Critical Limitation**:
- **Qdrant Local**: Only supports single connections - cannot be used simultaneously by crawler and NLWeb
- **Qdrant Server** (Docker): Supports multiple connections - recommended for production

**Recommended Architecture:**
```
┌─────────────┐    ┌─────────────┐
│   Crawler   │    │    NLWeb    │
│  Container  │    │  Container  │
└─────┬───────┘    └─────┬───────┘
      │                  │
      └────────┬─────────┘
               │
    ┌──────────▼──────────┐
    │   Qdrant Server     │
    │    (Docker)         │
    └─────────────────────┘
```

## Troubleshooting

### Common Issues

1. **Qdrant Connection Failed:**
   ```
   Error: Connection to Qdrant failed
   ```
   **Solutions:**
   - Ensure `QDRANT_URL=http://qdrant:6333` (not localhost)
   - Check that Qdrant container is running: `docker ps`
   - Verify network connectivity: `docker network ls`

2. **Permission Errors:**
   ```
   PermissionError: [Errno 13] Permission denied
   ```
   **Solution:**
   ```bash
   chmod -R 755 data/ logs/ config/
   chown -R $USER:$USER data/ logs/ config/
   ```

3. **Submodule Not Found:**
   ```
   ModuleNotFoundError: No module named 'core'
   ```
   **Solutions:**
   ```bash
   git submodule update --init --recursive
   docker-compose build --no-cache
   ```

4. **Database Lock Error:**
   ```
   Error: Database is locked
   ```
   **Solution:** Switch from Qdrant local to Qdrant server:
   ```bash
   # Stop local Qdrant instances
   docker-compose -f docker-compose.qdrant.yml up qdrant
   ```

5. **Embedding Generation Fails:**
   ```
   Error: OpenAI API key not found
   ```
   **Solution:** Check environment variables:
   ```bash
   docker-compose exec crawler-app env | grep -i openai
   ```

### Debugging Commands

```bash
# Check container status
docker-compose ps

# View crawler logs
docker-compose logs -f crawler-app

# View Qdrant logs
docker-compose logs -f qdrant

# Check network connectivity
docker-compose exec crawler-app ping qdrant

# Verify submodule
docker-compose exec crawler-app ls -la nlweb-submodule/

# Test database connection
docker-compose exec crawler-app python -c "
import setup_submodule_path
from core.retriever import get_vector_db_client
client = get_vector_db_client()
print('Database connection successful')
"
```

### Performance Optimization

1. **Resource Allocation:**
   ```yaml
   # docker-compose.yml
   services:
     crawler-app:
       deploy:
         resources:
           limits:
             memory: 2G
             cpus: '1.0'
   ```

2. **Concurrent Processing:**
   - Adjust `MAX_CONCURRENT` in crawler settings
   - Monitor CPU and memory usage
   - Scale Qdrant resources as needed

3. **Storage Optimization:**
   ```bash
   # Regular cleanup of old logs
   find logs/ -name "*.log" -mtime +7 -delete

   # Monitor disk usage
   du -sh data/
   ```