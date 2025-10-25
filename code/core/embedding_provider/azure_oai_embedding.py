"""
Azure OpenAI Embedding Provider
Minimal implementation adapted from NLWeb
"""

from typing import List, Optional
from openai import AsyncAzureOpenAI


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

    async def get_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in a single request

        Args:
            texts: List of texts to embed

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
            raise Exception(f"Error generating batch embeddings: {str(e)}")