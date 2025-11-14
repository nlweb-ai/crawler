# Embedding Providers

Pluggable embedding generation for vector database.

## Architecture

The embedding provider system allows swapping between different embedding APIs while maintaining a consistent interface.

## Files

### `__init__.py`
Package initializer.

### `azure_oai_embedding.py`
Azure OpenAI embedding provider implementation.

## Azure OpenAI Provider

### Usage

```python
from embedding_provider.azure_oai_embedding import AzureOpenAIEmbedding

provider = AzureOpenAIEmbedding(
    endpoint='https://your-resource.openai.azure.com/',
    api_key='your-api-key',
    deployment='text-embedding-3-small'
)

# Single embedding
embedding = await provider.get_embedding('Some text to embed')
# Returns: List[float] with 1536 dimensions

# Batch embeddings (more efficient)
embeddings = await provider.get_batch_embeddings([
    'First text',
    'Second text',
    'Third text'
])
# Returns: List[List[float]]
```

### Configuration

**Environment Variables:**
```bash
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_KEY=your-api-key-here
AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-3-small
```

### Features

- **Async API** - Uses `AsyncAzureOpenAI` for non-blocking operations
- **Batch Support** - Process multiple texts in one request (cost-effective)
- **Standard Dimensions** - Returns 1536-dimension vectors (text-embedding-3-small)
- **Error Handling** - Raises exceptions with detailed error messages

### API Version

Uses Azure OpenAI API version `2024-02-01` (stable).

## Supported Models

### text-embedding-3-small (Recommended)
- **Dimensions:** 1536
- **Max Tokens:** 8,191
- **Cost:** Lower than ada-002
- **Performance:** Better than ada-002

### text-embedding-3-large
- **Dimensions:** 3072
- **Max Tokens:** 8,191
- **Cost:** Higher than small
- **Performance:** Best available

### text-embedding-ada-002 (Legacy)
- **Dimensions:** 1536
- **Max Tokens:** 8,191
- **Cost:** Higher than v3 models
- **Status:** Still supported but v3 preferred

## Adding New Providers

To add a new embedding provider (e.g., OpenAI, Cohere, HuggingFace):

1. **Create provider file:** `new_provider_embedding.py`
2. **Implement methods:**
   ```python
   class NewProviderEmbedding:
       def __init__(self, **config):
           # Initialize client
           pass

       async def get_embedding(self, text: str) -> List[float]:
           # Generate single embedding
           pass

       async def get_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
           # Generate batch embeddings
           pass
   ```
3. **Update vector_db.py:** Add provider selection logic

Example for OpenAI (non-Azure):
```python
from openai import AsyncOpenAI

class OpenAIEmbedding:
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def get_embedding(self, text: str) -> List[float]:
        response = await self.client.embeddings.create(
            input=text,
            model=self.model
        )
        return response.data[0].embedding

    async def get_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        response = await self.client.embeddings.create(
            input=texts,
            model=self.model
        )
        return [data.embedding for data in response.data]
```

## Integration with Vector DB

The vector database (`vector_db.py`) uses embedding providers through the `EmbeddingWrapper` class:

```python
# In vector_db.py
class EmbeddingWrapper:
    def __init__(self):
        # Initialize provider based on config
        self.azure_provider = AzureOpenAIEmbedding(...)

    async def get_embedding(self, text: str, provider: str = "azure_openai"):
        if provider == "azure_openai":
            return await self.azure_provider.get_embedding(text)
        # Add other providers here
```

## Performance Considerations

### Batch Processing
Always prefer `get_batch_embeddings()` over multiple `get_embedding()` calls:

**Good:**
```python
texts = ['text1', 'text2', 'text3']
embeddings = await provider.get_batch_embeddings(texts)  # 1 API call
```

**Bad:**
```python
embeddings = []
for text in texts:
    emb = await provider.get_embedding(text)  # N API calls!
    embeddings.append(emb)
```

### Text Truncation
The vector DB automatically truncates text to 20,000 characters before embedding to prevent token limit errors.

### Rate Limiting
Azure OpenAI has rate limits. Consider:
- Batch operations (up to 2048 inputs per request)
- Retry logic with exponential backoff
- Request throttling in high-volume scenarios

## Cost Optimization

### text-embedding-3-small Pricing (as of 2024)
- **Cost:** ~$0.02 per 1M tokens
- **Batch discount:** No additional discount, but fewer requests = lower latency

### Tips
1. **Deduplicate** - Don't embed the same text twice (handled by crawler's reference counting)
2. **Truncate** - Remove unnecessary content before embedding
3. **Cache** - Store embeddings in vector DB, don't regenerate
4. **Batch** - Always use batch operations for multiple texts

## Testing

Test the embedding provider:

```python
import asyncio
from embedding_provider.azure_oai_embedding import AzureOpenAIEmbedding

async def test():
    provider = AzureOpenAIEmbedding(
        endpoint='https://....openai.azure.com/',
        api_key='your-key',
        deployment='text-embedding-3-small'
    )

    # Test single
    emb = await provider.get_embedding('Hello world')
    print(f'Dimensions: {len(emb)}')  # Should be 1536

    # Test batch
    embs = await provider.get_batch_embeddings(['Hello', 'World'])
    print(f'Batch count: {len(embs)}')  # Should be 2

asyncio.run(test())
```

## Troubleshooting

### "Resource not found" error
- Verify `AZURE_OPENAI_ENDPOINT` is correct
- Check deployment name matches your Azure OpenAI resource
- Ensure API key is valid

### "Invalid API version" error
- Update API version in `azure_oai_embedding.py`
- Check Azure OpenAI changelog for breaking changes

### "Rate limit exceeded" error
- Implement retry logic with backoff
- Reduce batch size
- Check Azure OpenAI quota limits

### Dimension mismatch
- Ensure vector DB index dimension matches embedding dimension
- text-embedding-3-small = 1536 dimensions
- text-embedding-3-large = 3072 dimensions
