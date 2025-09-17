# Docker Setup for NLWeb Crawler

This directory contains Docker configuration files to run the NLWeb Crawler application with Qdrant vector database.

## Quick Start

1. **Build and start the services:**
   ```bash
   docker-compose up --build
   ```

2. **Access the application:**
   - Crawler Web Interface: http://localhost:5000
   - Qdrant API: http://localhost:6333

## Configuration

### Environment Variables
Copy the environment template and customize as needed:
```bash
cp .env.docker.template .env.docker
```

Edit `.env.docker` with your specific configuration values.

### Config Files
The `config/` directory is mounted to the container, so you can modify configuration files directly on the host:
- `config/config_retrieval.yaml` - Vector database configuration
- `config/config_embedding.yaml` - Embedding provider configuration
- `config/config_llm.yaml` - LLM provider configuration
- Other config files as needed

### Data Persistence
The following directories are mounted to the host for data persistence:
- `data/` - Contains embeddings, JSON files, crawled documents
- `logs/` - Application logs
- `config/` - Configuration files

## Services

### crawler-app
- **Image:** Built from local Dockerfile
- **Ports:** 5000:5000
- **Volumes:** 
  - `./config:/app/config`
  - `./data:/app/data`
  - `./logs:/app/logs`

### qdrant
- **Image:** qdrant/qdrant:latest
- **Ports:** 6333:6333, 6334:6334
- **Volume:** `qdrant_data:/qdrant/storage`

## Development Mode

For development with live code reloading:

1. **Use the development override:**
   ```bash
   docker-compose -f docker-compose.yml -f docker-compose.override.yml up --build
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

## Troubleshooting

1. **Qdrant connection issues:** Ensure the `QDRANT_URL` environment variable points to `http://qdrant:6333` (not localhost)

2. **Permission issues:** Make sure the `data/` and `logs/` directories are writable:
   ```bash
   chmod -R 755 data/ logs/
   ```

3. **Submodule issues:** If the nlweb-submodule is not properly initialized:
   ```bash
   git submodule update --init --recursive
   docker-compose build --no-cache
   ```
