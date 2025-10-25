"""
Vector Database implementation with Azure Cognitive Search and embeddings support.
"""

import config  # Load environment variables
import os
import json
import asyncio
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    VectorSearch,
    VectorSearchProfile,
    HnswAlgorithmConfiguration
)
from azure.core.credentials import AzureKeyCredential

# Import embedding provider
import sys
sys.path.insert(0, os.path.dirname(__file__))
from embedding_provider.azure_oai_embedding import AzureOpenAIEmbedding


class EmbeddingWrapper:
    """Wrapper for handling embeddings with different providers"""

    def __init__(self):
        # Initialize Azure OpenAI embedding provider from environment variables
        self.azure_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        self.azure_api_key = os.getenv('AZURE_OPENAI_KEY')
        self.azure_deployment = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT', 'text-embedding-3-small')

        if self.azure_endpoint and self.azure_api_key:
            self.azure_provider = AzureOpenAIEmbedding(
                endpoint=self.azure_endpoint,
                api_key=self.azure_api_key,
                deployment=self.azure_deployment
            )
        else:
            self.azure_provider = None

    async def get_embedding(self, text: str, provider: str = "azure_openai") -> List[float]:
        """Generate embedding for text"""
        # Truncate text to prevent excessive token usage
        MAX_CHARS = 20000
        if len(text) > MAX_CHARS:
            text = text[:MAX_CHARS]

        if provider == "azure_openai" and self.azure_provider:
            return await self.azure_provider.get_embedding(text)
        else:
            # Return a dummy embedding for testing if no provider configured
            return [0.0] * 1536  # Standard embedding dimension

    async def batch_get_embeddings(self, texts: List[str], provider: str = "azure_openai") -> List[List[float]]:
        """Generate embeddings for multiple texts"""
        if provider == "azure_openai" and self.azure_provider:
            # Truncate texts
            MAX_CHARS = 20000
            texts = [t[:MAX_CHARS] if len(t) > MAX_CHARS else t for t in texts]
            return await self.azure_provider.get_batch_embeddings(texts)
        else:
            # Return dummy embeddings for testing
            return [[0.0] * 1536 for _ in texts]


class VectorDB:
    """Azure Cognitive Search vector database implementation"""

    def __init__(self):
        # Azure Search configuration from environment variables
        self.search_endpoint = os.getenv('AZURE_SEARCH_ENDPOINT')
        self.search_key = os.getenv('AZURE_SEARCH_KEY')
        self.index_name = os.getenv('AZURE_SEARCH_INDEX_NAME', 'crawler-vectors')

        # Initialize embedding wrapper
        self.embedding_wrapper = EmbeddingWrapper()

        # Initialize Azure Search clients if credentials available
        if self.search_endpoint and self.search_key:
            credential = AzureKeyCredential(self.search_key)
            self.index_client = SearchIndexClient(self.search_endpoint, credential)
            self.search_client = SearchClient(self.search_endpoint, self.index_name, credential)
            self._ensure_index_exists()
        else:
            self.index_client = None
            self.search_client = None

    def _ensure_index_exists(self):
        """Create the search index if it doesn't exist"""
        try:
            # Check if index exists
            self.index_client.get_index(self.index_name)
        except:
            # Create index with vector search configuration
            fields = [
                SimpleField(name="id", type=SearchFieldDataType.String, key=True),  # Hash of URL for Azure Search key
                SearchableField(name="url", type=SearchFieldDataType.String),  # Original URL (was @id in JSON-LD)
                SearchableField(name="site", type=SearchFieldDataType.String),
                SearchableField(name="type", type=SearchFieldDataType.String),
                SearchableField(name="content", type=SearchFieldDataType.String),
                SimpleField(name="timestamp", type=SearchFieldDataType.DateTimeOffset),
                SearchField(
                    name="embedding",
                    type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    searchable=True,
                    vector_search_dimensions=1536,
                    vector_search_profile_name="default"
                ),
            ]

            vector_search = VectorSearch(
                profiles=[
                    VectorSearchProfile(
                        name="default",
                        algorithm_configuration_name="hnsw"
                    )
                ],
                algorithms=[
                    HnswAlgorithmConfiguration(name="hnsw")
                ]
            )

            index = SearchIndex(
                name=self.index_name,
                fields=fields,
                vector_search=vector_search
            )

            self.index_client.create_index(index)

    def _prepare_document(self, id: str, site: str, json_obj: dict, embedding: List[float]) -> dict:
        """Prepare document for indexing"""
        # Extract type information
        obj_type = json_obj.get('@type', 'Unknown')
        if isinstance(obj_type, list):
            obj_type = ', '.join(obj_type)

        # Create searchable content from JSON object
        content_parts = []
        for key, value in json_obj.items():
            if isinstance(value, str):
                content_parts.append(f"{key}: {value}")
            elif isinstance(value, (list, dict)):
                content_parts.append(f"{key}: {json.dumps(value)[:500]}")

        content = " ".join(content_parts)[:10000]  # Limit content size

        # Generate hash of URL for Azure Search key field
        # Using SHA-256 and taking first 32 hex chars (128 bits) for reasonable key length
        url_hash = hashlib.sha256(id.encode('utf-8')).hexdigest()[:32]

        return {
            "id": url_hash,  # Hash of URL for Azure Search key
            "url": id,  # Original URL unmodified
            "site": site,
            "type": obj_type,
            "content": content,
            "timestamp": datetime.utcnow().isoformat() + 'Z',  # Add Z for UTC timezone
            "embedding": embedding
        }

    async def add(self, id: str, site: str, json_obj: dict):
        """Add or update an item in the vector database"""
        try:
            # Generate text representation for embedding
            text = json.dumps(json_obj)

            # Get embedding
            embedding = await self.embedding_wrapper.get_embedding(text)

            if self.search_client:
                # Prepare and upload document
                document = self._prepare_document(id, site, json_obj, embedding)
                self.search_client.upload_documents(documents=[document])

        except Exception as e:
            print(f"Error adding to vector DB: {e}")

    async def delete(self, id: str):
        """Remove an item from the vector database"""
        try:
            if self.search_client:
                # Hash the URL to match the stored key
                url_hash = hashlib.sha256(id.encode('utf-8')).hexdigest()[:32]
                self.search_client.delete_documents(documents=[{"id": url_hash}])
        except Exception as e:
            print(f"Error deleting from vector DB: {e}")

    async def batch_add(self, items: List[Tuple[str, str, dict]]):
        """Batch add items to the vector database"""
        try:
            # Generate embeddings for all items
            texts = [json.dumps(obj) for _, _, obj in items]
            embeddings = await self.embedding_wrapper.batch_get_embeddings(texts)

            if self.search_client:
                # Prepare documents
                documents = []
                for (id, site, json_obj), embedding in zip(items, embeddings):
                    document = self._prepare_document(id, site, json_obj, embedding)
                    documents.append(document)

                # Upload in batches of 100
                batch_size = 100
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i + batch_size]
                    self.search_client.upload_documents(documents=batch)

        except Exception as e:
            print(f"Error in batch add to vector DB: {e}")

    async def batch_delete(self, ids: List[str]):
        """Batch delete items from the vector database"""
        try:
            if self.search_client:
                # Hash URLs to match stored keys
                documents = [{"id": hashlib.sha256(id.encode('utf-8')).hexdigest()[:32]} for id in ids]

                # Delete in batches of 100
                batch_size = 100
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i + batch_size]
                    self.search_client.delete_documents(documents=batch)

        except Exception as e:
            print(f"Error in batch delete from vector DB: {e}")


# Global vector DB instance
_vector_db = None

def _get_vector_db():
    """Get or create the global vector DB instance"""
    global _vector_db
    if _vector_db is None:
        _vector_db = VectorDB()
    return _vector_db


# Public synchronous API (called by worker.py)
def vector_db_add(id: str, site: str, json_obj: dict):
    """
    Add/update an item in the vector database (synchronous wrapper)
    """
    db = _get_vector_db()
    asyncio.run(db.add(id, site, json_obj))


def vector_db_delete(id: str):
    """
    Remove an item from the vector database (synchronous wrapper)
    """
    db = _get_vector_db()
    asyncio.run(db.delete(id))


def vector_db_batch_add(items: list):
    """
    Batch add items to the vector database (synchronous wrapper)
    Args:
        items: List of (id, site, json_obj) tuples
    """
    db = _get_vector_db()
    asyncio.run(db.batch_add(items))


def vector_db_batch_delete(ids: list):
    """
    Batch delete items from the vector database (synchronous wrapper)
    """
    db = _get_vector_db()
    asyncio.run(db.batch_delete(ids))