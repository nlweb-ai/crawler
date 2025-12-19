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


# Maximum number of IDs Azure AI Search allows you to delete in a single batch
MAX_BATCH_DELETE_IDS = 1000


def extract_essential_fields(json_obj: dict) -> str:
    """
    Extract only essential fields from a schema.org object for embedding.
    This reduces token usage while preserving searchable content.
    """
    essential_fields = {}

    # Always include type and ID
    if '@type' in json_obj:
        essential_fields['@type'] = json_obj['@type']
    if '@id' in json_obj:
        essential_fields['@id'] = json_obj['@id']

    # Common essential fields across all schema.org types
    common_fields = ['name', 'description', 'headline', 'text', 'abstract', 'summary']
    for field in common_fields:
        if field in json_obj:
            essential_fields[field] = json_obj[field]

    # Type-specific essential fields
    obj_type = json_obj.get('@type', '')
    if isinstance(obj_type, list):
        obj_type = obj_type[0] if obj_type else ''

    if 'Recipe' in obj_type:
        # For recipes: include ingredients and basic info, skip detailed instructions
        recipe_fields = ['recipeIngredient', 'recipeYield', 'totalTime', 'cookTime', 'prepTime',
                        'recipeCategory', 'recipeCuisine', 'keywords']
        for field in recipe_fields:
            if field in json_obj:
                essential_fields[field] = json_obj[field]

    elif 'Movie' in obj_type or 'TVSeries' in obj_type:
        # For movies/TV: include basic metadata
        media_fields = ['genre', 'datePublished', 'director', 'actor', 'duration', 'contentRating']
        for field in media_fields:
            if field in json_obj:
                value = json_obj[field]
                # For nested objects, just keep the name
                if isinstance(value, dict) and 'name' in value:
                    essential_fields[field] = value['name']
                elif isinstance(value, list):
                    # For arrays of objects, extract names
                    essential_fields[field] = [v['name'] if isinstance(v, dict) and 'name' in v else v for v in value[:5]]  # Limit to 5
                else:
                    essential_fields[field] = value

    elif 'Product' in obj_type:
        # For products: include basic product info
        product_fields = ['brand', 'model', 'offers', 'aggregateRating', 'category']
        for field in product_fields:
            if field in json_obj:
                value = json_obj[field]
                # Simplify offers and ratings
                if field == 'offers' and isinstance(value, dict):
                    essential_fields[field] = {'price': value.get('price'), 'availability': value.get('availability')}
                elif field == 'aggregateRating' and isinstance(value, dict):
                    essential_fields[field] = {'ratingValue': value.get('ratingValue'), 'ratingCount': value.get('ratingCount')}
                else:
                    essential_fields[field] = value

    elif 'Article' in obj_type or 'NewsArticle' in obj_type:
        # For articles: include metadata and abstract
        article_fields = ['author', 'datePublished', 'publisher', 'articleSection']
        for field in article_fields:
            if field in json_obj:
                value = json_obj[field]
                if isinstance(value, dict) and 'name' in value:
                    essential_fields[field] = value['name']
                else:
                    essential_fields[field] = value

    # Convert to JSON string
    essential_json = json.dumps(essential_fields)

    # If still too large (>6000 chars ~= ~6000 tokens with overhead), truncate
    MAX_CHARS = 6000
    if len(essential_json) > MAX_CHARS:
        # Try with just the most basic fields
        minimal_fields = {
            '@type': essential_fields.get('@type'),
            '@id': essential_fields.get('@id'),
            'name': essential_fields.get('name', '')[:500],  # Truncate name
            'description': essential_fields.get('description', '')[:1000]  # Truncate description
        }
        essential_json = json.dumps(minimal_fields)

        # Final truncation if still too large
        if len(essential_json) > MAX_CHARS:
            essential_json = essential_json[:MAX_CHARS]

    return essential_json


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
                SearchField(name="site", type=SearchFieldDataType.String, searchable=True, filterable=True),  # Make site searchable and filterable
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

        # Store the original JSON object as a JSON string
        # This ensures the content field contains valid JSON
        content = json.dumps(json_obj)

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
            print(f"[Vector DB] batch_add called with {len(items)} items")
            if len(items) > 0:
                print(f"[Vector DB] Sample sites: {[site for _, site, _ in items[:3]]}")
                print(f"[Vector DB] Sample IDs: {[id for id, _, _ in items[:3]]}")

            # Process in batches to avoid token limits
            # Azure OpenAI embedding API has limits on:
            # - Max 2048 items per request
            # - Max tokens per request (varies by model, ~8191 for text-embedding-3-small)
            # We'll use a conservative batch size of 50 items
            embedding_batch_size = 50

            print(f"[Vector DB] Starting embedding generation for {len(items)} items in batches of {embedding_batch_size}")
            all_embeddings = []
            for i in range(0, len(items), embedding_batch_size):
                batch_items = items[i:i + embedding_batch_size]
                # Extract essential fields instead of using full JSON
                texts = [extract_essential_fields(obj) for _, _, obj in batch_items]
                print(f"[Vector DB] Batch {i//embedding_batch_size + 1}: Generating embeddings for {len(batch_items)} items...")
                batch_embeddings = await self.embedding_wrapper.batch_get_embeddings(texts)
                all_embeddings.extend(batch_embeddings)
                print(f"[Vector DB] Batch {i//embedding_batch_size + 1}: Successfully generated {len(batch_embeddings)} embeddings")
                print(f"[Vector DB] Generated embeddings for batch {i//embedding_batch_size + 1}/{(len(items) + embedding_batch_size - 1)//embedding_batch_size} ({len(batch_items)} items)")

                # Add small delay between batches to avoid rate limits
                if i + embedding_batch_size < len(items):
                    import asyncio
                    await asyncio.sleep(1)  # 1 second delay between batches

            print(f"[Vector DB] Total embeddings generated: {len(all_embeddings)}")

            if self.search_client:
                # Prepare documents
                print(f"[Vector DB] Preparing {len(items)} documents for upload...")
                documents = []
                for (id, site, json_obj), embedding in zip(items, all_embeddings):
                    document = self._prepare_document(id, site, json_obj, embedding)
                    documents.append(document)

                print(f"[Vector DB] Prepared {len(documents)} documents")
                if len(documents) > 0:
                    print(f"[Vector DB] Sample document keys: {list(documents[0].keys())}")
                    print(f"[Vector DB] Sample document site: {documents[0].get('site')}")

                # Upload to search index in batches of 100
                upload_batch_size = 100
                print(f"[Vector DB] Starting upload to search index in batches of {upload_batch_size}...")
                for i in range(0, len(documents), upload_batch_size):
                    batch = documents[i:i + upload_batch_size]
                    print(f"[Vector DB] Uploading batch {i//upload_batch_size + 1}/{(len(documents) + upload_batch_size - 1)//upload_batch_size} ({len(batch)} documents)...")
                    result = self.search_client.upload_documents(documents=batch)
                    print(f"[Vector DB] Upload result: {result}")
                    print(f"[Vector DB] Uploaded batch {i//upload_batch_size + 1}/{(len(documents) + upload_batch_size - 1)//upload_batch_size} ({len(batch)} documents)")

                print(f"[Vector DB] Successfully completed batch_add for {len(items)} items")
            else:
                print(f"[Vector DB] WARNING: search_client is None, cannot upload documents")

        except Exception as e:
            print(f"[Vector DB] ERROR in batch add to vector DB: {e}")
            import traceback
            traceback.print_exc()

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

    async def count_by_site(self, site: str) -> int:
        """Count documents for a specific site"""
        try:
            if self.search_client:
                # Use search API with filter to count
                results = self.search_client.search(
                    search_text="*",
                    filter=f"site eq '{site}'",
                    select="id",
                    include_total_count=True,
                    top=0  # We only want the count, not the results
                )
                return results.get_count()
            return 0
        except Exception as e:
            print(f"Error counting documents in vector DB: {e}")
            return 0


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


def vector_db_count_by_site(site: str) -> int:
    """
    Count documents for a specific site (synchronous wrapper)
    """
    db = _get_vector_db()
    return asyncio.run(db.count_by_site(site))