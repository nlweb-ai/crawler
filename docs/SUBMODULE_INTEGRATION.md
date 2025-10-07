# NLWeb Submodule Integration Guide

## Overview

The NLWeb Crawler integrates with the main NLWeb project through a Git submodule to reuse critical libraries and maintain consistency across the platform. This integration allows the crawler to leverage proven database loading utilities and embedding libraries without code duplication.

## What is Included

The `nlweb-submodule` provides access to:

### Core Libraries
- **Embedding Generation**: `core.embedding` module for consistent vectorization
- **Database Operations**: `core.retriever` module for vector database CRUD operations
- **Configuration Management**: Shared configuration patterns and utilities
- **Storage Providers**: Support for multiple vector databases (Qdrant, Elasticsearch, PostgreSQL)

### Key Components Used
1. `core.embedding.get_embedding()` - Generates embeddings for text content
2. `core.retriever.upload_documents()` - Uploads documents with embeddings to vector database
3. `core.retriever.delete_documents_by_site()` - Removes all documents for a specific site
4. Configuration classes for database and embedding provider settings

## Setup Process

### 1. Initial Submodule Setup

When cloning the crawler repository:

```bash
# Clone with submodules (recommended)
git clone --recurse-submodules https://github.com/nlweb-ai/crawler.git

# Or clone and initialize submodules separately
git clone https://github.com/nlweb-ai/crawler.git
cd crawler
git submodule update --init --recursive
```

### 2. Path Configuration

The crawler includes `setup_submodule_path.py` which automatically configures Python imports:

```python
"""
Utility module to set up the NLWeb submodule path.
Import this module before importing any core modules to ensure the path is set up correctly.
"""

import os
import sys

def setup_nlweb_submodule_path():
    """
    Add the NLWeb submodule to the Python path so we can import directly from it.
    This function can be called multiple times safely - it won't add duplicate paths.
    """
    # Calculate the path to the submodule
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    
    # If we're in the code directory, go up one level
    if os.path.basename(current_file_dir) == 'code':
        project_root = os.path.dirname(current_file_dir)
    else:
        project_root = current_file_dir
    
    nlweb_submodule_path = os.path.join(project_root, 'nlweb-submodule', 'code', 'python')
    
    # Only add the path if it exists and isn't already in sys.path
    if os.path.exists(nlweb_submodule_path) and nlweb_submodule_path not in sys.path:
        sys.path.insert(0, nlweb_submodule_path)
        return True
    
    return False

# Automatically set up the path when this module is imported
setup_nlweb_submodule_path()
```

### 3. Usage in Crawler Code

In any crawler module that needs NLWeb functionality:

```python
import setup_submodule_path  # This automatically sets up the submodule path

# Now you can import from the NLWeb submodule
from core.embedding import get_embedding
from core.retriever import upload_documents, delete_documents_by_site
```

## Integration Points

### Embeddings Worker

The embeddings worker uses the NLWeb embedding library:

```python
async def embeddings_worker(self):
    """Worker that processes embeddings queue."""
    while self.running:
        try:
            site_name, batch = await self.embeddings_queue.get()
            
            # Prepare texts for embedding
            texts = [self.prepare_text_for_embedding(obj) for obj in batch]
            
            # Import get_embedding function from the submodule
            from core.embedding import get_embedding
            
            # Generate embeddings
            embeddings = []
            for text in texts:
                embedding = await get_embedding(text)
                embeddings.append(embedding)
            
            # Save embeddings to file
            await self.save_embeddings(site_name, keys, embeddings, batch)
            
        except Exception as e:
            self.error_logger.error(f"Embeddings worker error | {str(e)}")
```

### Database Worker

The database worker uses NLWeb's retriever module:

```python
async def database_worker(self):
    """Worker that processes database queue."""
    while self.running:
        try:
            from core.retriever import upload_documents
            
            # Get batch from queue
            site_name, batch = await self.database_queue.get()
            
            # Prepare documents for upload
            documents = []
            for item in batch:
                doc = {
                    "url": item["url"],
                    "site": site_name,
                    "schema_json": item["schema_json"],
                    "embedding": item["embedding"]
                }
                documents.append(doc)
            
            # Upload to vector database
            await upload_documents(documents)
            
        except Exception as e:
            self.error_logger.error(f"Database worker error | {str(e)}")
```

### Site Deletion

Site deletion removes data from both local storage and vector database:

```python
def delete_site(self, site_name):
    """Mark a site as deleted and remove from vector database."""
    # Mark for deletion in crawler
    self.deleted_sites.add(site_name)
    
    # Delete documents from database using NLWeb submodule
    from core.retriever import delete_documents_by_site
    
    future = asyncio.run_coroutine_threadsafe(
        delete_documents_by_site(site_name),
        self.loop,
    )
    result = future.result()
```

## Configuration

### Shared Configuration Files

The crawler uses configuration files compatible with NLWeb:

- `config/config_retrieval.yaml` - Vector database settings
- `config/config_embedding.yaml` - Embedding provider settings
- `config/config_llm.yaml` - Language model settings

### Environment Variables

Key environment variables that should be consistent between crawler and NLWeb:

```bash
# Vector Database
QDRANT_URL=http://localhost:6333
QDRANT_API_KEY=your_secret_key

# Embedding Provider
OPENAI_API_KEY=your_openai_key
AZURE_OPENAI_API_KEY=your_azure_key
AZURE_OPENAI_ENDPOINT=your_azure_endpoint

# Database Selection
PREFERRED_RETRIEVAL_ENDPOINT=qdrant
PREFERRED_EMBEDDING_PROVIDER=openai
```

## Updating the Submodule

### Getting Latest Updates

```bash
# Update to latest commit on the tracked branch
git submodule update --remote nlweb-submodule

# Commit the update
git add nlweb-submodule
git commit -m "Update NLWeb submodule to latest"
```

### Updating to Specific Version

```bash
# Navigate to submodule directory
cd nlweb-submodule

# Checkout specific commit or tag
git checkout v1.2.3  # or specific commit hash

# Return to main project
cd ..

# Commit the change
git add nlweb-submodule
git commit -m "Update NLWeb submodule to v1.2.3"
```

### Checking Submodule Status

```bash
# Check submodule status
git submodule status

# See what commit the submodule is on
cd nlweb-submodule
git log --oneline -1
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```
   ModuleNotFoundError: No module named 'core'
   ```
   **Solution**: Ensure `setup_submodule_path` is imported before any core imports

2. **Submodule Not Initialized**
   ```
   fatal: not a git repository (or any of the parent directories): .git
   ```
   **Solution**: Initialize the submodule
   ```bash
   git submodule update --init --recursive
   ```

3. **Path Issues**
   ```
   ImportError: attempted relative import with no known parent package
   ```
   **Solution**: Check that the submodule path is correctly added to sys.path

4. **Configuration Conflicts**
   ```
   ConfigurationError: No retrieval endpoint configured
   ```
   **Solution**: Ensure configuration files are properly set up and environment variables are defined

### Debugging Steps

1. **Verify Submodule Status**:
   ```bash
   git submodule status
   ls -la nlweb-submodule/code/python/
   ```

2. **Check Python Path**:
   ```python
   import sys
   print(sys.path)
   # Should include path to nlweb-submodule/code/python
   ```

3. **Test Imports**:
   ```python
   import setup_submodule_path
   from core.embedding import get_embedding
   print("Imports successful")
   ```

## Best Practices

1. **Always Import setup_submodule_path First**: This ensures the Python path is configured before any other imports

2. **Keep Submodule Updated**: Regularly update to get bug fixes and new features from NLWeb

3. **Use Consistent Configuration**: Ensure both crawler and NLWeb use the same database and embedding settings

4. **Test Integration**: After submodule updates, test that embedding generation and database operations still work

5. **Document Dependencies**: If you add new imports from the submodule, document them for other developers

## Benefits of This Approach

- **Code Reuse**: Leverages proven, tested libraries from NLWeb
- **Consistency**: Ensures compatible embeddings and database operations
- **Maintainability**: Updates to core libraries benefit both projects
- **Reduced Complexity**: Avoids duplicating complex database and embedding logic
- **Platform Integration**: Seamless compatibility with the broader NLWeb ecosystem
