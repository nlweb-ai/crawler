"""
Azure OpenAI Embedding Provider
Minimal implementation adapted from NLWeb
"""

from typing import List, Optional
from openai import AsyncAzureOpenAI
import asyncio
import time


class AzureOpenAIEmbedding:
    """Azure OpenAI embedding provider"""

    def __init__(self, endpoint: str, api_key: str, deployment: str = "text-embedding-3-small"):
        """
        Initialize Azure OpenAI embedding client

        Args:
            endpoint: Azure OpenAI endpoint URL
            api_key: Azure OpenAI API key
            deployment: Deployment name for the embedding model
        """
        self.client = AsyncAzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-02-01"  # Use stable API version
        )
        self.deployment = deployment

    async def get_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text

        Args:
            text: Text to embed

        Returns:
            List of floating point numbers representing the embedding
        """
        try:
            response = await self.client.embeddings.create(
                input=text,
                model=self.deployment
            )
            return response.data[0].embedding
        except Exception as e:
            raise Exception(f"Error generating embedding: {str(e)}")

    async def get_batch_embeddings(self, texts: List[str], retry_count: int = 0, max_retries: int = 8) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in a single request with retry logic

        Args:
            texts: List of texts to embed
            retry_count: Current retry attempt (for internal use)
            max_retries: Maximum number of retries for rate limit errors

        Returns:
            List of embeddings (each embedding is a list of floats)
        """
        try:
            # Azure OpenAI can handle batch inputs
            response = await self.client.embeddings.create(
                input=texts,
                model=self.deployment
            )
            return [data.embedding for data in response.data]
        except Exception as e:
            error_msg = str(e)

            # Check if error is due to rate limit (429)
            if "429" in error_msg or "RateLimitReached" in error_msg or "rate limit" in error_msg.lower():
                if retry_count < max_retries:
                    # Exponential backoff: 2, 4, 8, 16, 32 seconds
                    wait_time = 2 ** (retry_count + 1)
                    print(f"[Embedding] Rate limit hit. Retry {retry_count + 1}/{max_retries} after {wait_time}s. Error: {error_msg[:200]}")
                    await asyncio.sleep(wait_time)
                    return await self.get_batch_embeddings(texts, retry_count + 1, max_retries)
                else:
                    print(f"[Embedding] Max retries ({max_retries}) reached for rate limit. Failing batch.")
                    raise Exception(f"Rate limit exceeded after {max_retries} retries: {error_msg}")

            # Check if error is due to token limit
            if "maximum context length" in error_msg or "token" in error_msg.lower():
                # If batch is too large, split it and retry
                if len(texts) > 1:
                    print(f"[Embedding] Batch too large ({len(texts)} items), splitting in half and retrying...")
                    mid = len(texts) // 2
                    first_half = await self.get_batch_embeddings(texts[:mid])
                    second_half = await self.get_batch_embeddings(texts[mid:])
                    return first_half + second_half
                else:
                    # Single item is too large - return zero embedding
                    print(f"[Embedding] Single item too large, returning zero embedding. Error: {error_msg}")
                    return [[0.0] * 1536]  # Return zero embedding for oversized item

            raise Exception(f"Error generating batch embeddings: {error_msg}")