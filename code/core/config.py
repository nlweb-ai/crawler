# Copyright (c) 2025 Microsoft Corporation.
# Licensed under the MIT License

"""
Basic config services, including loading config from config_llm.yaml, config_embedding.yaml, config_retrieval.yaml, 
config_webserver.yaml, config_nlweb.yaml
WARNING: This code is under development and may undergo changes in future releases.
Backwards compatibility is not guaranteed at this time.
"""

import os
import yaml
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from dotenv import load_dotenv
from typing import Dict, Optional, Any, List

@dataclass
@dataclass
class EmbeddingProviderConfig:
    api_key: Optional[str] = None
    endpoint: Optional[str] = None
    api_version: Optional[str] = None
    model: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

@dataclass
class RetrievalProviderConfig:
    api_key: Optional[str] = None
    api_key_env: Optional[str] = None  # Environment variable name for API key
    api_endpoint: Optional[str] = None
    api_endpoint_env: Optional[str] = None  # Environment variable name for endpoint
    database_path: Optional[str] = None
    index_name: Optional[str] = None
    db_type: Optional[str] = None
    use_knn: Optional[bool] = None
    enabled: bool = False
    vector_type: Optional[Dict[str, Any]] = None

class AppConfig:
    config_paths = ["config_embedding.yaml", "config_retrieval.yaml"]

    def __init__(self):
        load_dotenv()
        # Set config directory - can be overridden by NLWEB_CONFIG_DIR environment variable
        self.config_directory = self._get_config_directory()
        self.base_output_directory = self._get_base_output_directory()
        self.load_embedding_config()
        self.load_retrieval_config()

    def _get_config_directory(self) -> str:
        """
        Get the configuration directory from environment variable or use default.
        Default is NLWeb/config relative to the code directory.
        """
        # Check for environment variable first
        config_dir = os.getenv('NLWEB_CONFIG_DIR')
        if config_dir:
            # Expand any environment variables or ~ in the path
            config_dir = os.path.expanduser(os.path.expandvars(config_dir))
            if not os.path.exists(config_dir):
                print(f"Warning: Configured config directory {config_dir} does not exist. Using default.")
                config_dir = None
        
        if not config_dir:
            # Default: go up one levels from code directory to crawler directory, then to config
            # code -> crawler
            current_dir = os.path.dirname(os.path.abspath(__file__))  # code directory
            crawler_dir = os.path.dirname(current_dir)  # Crawler directory
            config_dir = os.path.join(crawler_dir, 'config')
        
        return os.path.abspath(config_dir)

    def _get_base_output_directory(self) -> Optional[str]:
        """
        Get the base directory for all output files from the environment variable.
        Returns None if the environment variable is not set.
        """
        base_dir = os.getenv('CRAWLER_OUTPUT_DIR')
        if base_dir and not os.path.exists(base_dir):
            try:
                os.makedirs(base_dir, exist_ok=True)
                print(f"Created output directory: {base_dir}")
            except Exception as e:
                print(f"Warning: Failed to create output directory {base_dir}: {e}")
                return None
        return base_dir

    def _resolve_path(self, path: str) -> str:
        """
        Resolves a path, considering the base output directory if set.
        If path is absolute, returns it unchanged.
        If path is relative and base_output_directory is set, resolves against base_output_directory.
        Otherwise, resolves against the config directory.
        """
        if os.path.isabs(path):
            return path
        
        if self.base_output_directory:
            # If base output directory is set, use it for all relative paths
            return os.path.abspath(os.path.join(self.base_output_directory, path))
        else:
            # Otherwise use the config directory
            return os.path.abspath(os.path.join(self.config_directory, path))

    def _get_config_value(self, value: Any, default: Any = None) -> Any:
        """
        Get configuration value. If value is a string, return it directly.
        Otherwise, treat it as an environment variable name and fetch from environment.
        Returns default if environment variable is not set or value is None.
        """
        if value is None:
            return default
            
        if isinstance(value, str):
            # If it's clearly an environment variable name (e.g., "OPENAI_API_KEY_ENV")
            if value.endswith('_ENV') or value.isupper():
                return os.getenv(value, default)
            # Otherwise, treat it as a literal string value
            else:
                return value
        
        # For non-string values, return as-is
        return value


    def load_embedding_config(self, path: str = "config_embedding.yaml"):
        """Load embedding model configuration."""
        # Build the full path to the config file using the config directory
        full_path = os.path.join(self.config_directory, path)
        
        try:
            with open(full_path, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            # If config file doesn't exist, use defaults
            print(f"Warning: {path} not found. Using default embedding configuration.")
            data = {
                "preferred_provider": "openai",
                "providers": {}
            }
        
        self.preferred_embedding_provider: str = data["preferred_provider"]
        self.embedding_providers: Dict[str, EmbeddingProviderConfig] = {}

        for name, cfg in data.get("providers", {}).items():
            # Extract configuration values from the YAML
            api_key = self._get_config_value(cfg.get("api_key_env"))
            api_endpoint = self._get_config_value(cfg.get("api_endpoint_env"))
            api_version = self._get_config_value(cfg.get("api_version_env"))
            model = self._get_config_value(cfg.get("model"))
            config = self._get_config_value(cfg.get("config"))

            # Create the embedding provider config
            self.embedding_providers[name] = EmbeddingProviderConfig(
                api_key=api_key,
                endpoint=api_endpoint,
                api_version=api_version,
                model=model,
                config=config
            )

    def load_retrieval_config(self, path: str = "config_retrieval.yaml"):
        # Build the full path to the config file using the config directory
        full_path = os.path.join(self.config_directory, path)
        
        try:
            with open(full_path, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            # If config file doesn't exist, use defaults
            print(f"Warning: {path} not found. Using default retrieval configuration.")
            data = {
                "preferred_endpoint": "default",
                "endpoints": {}
            }

        # No longer using preferred_endpoint - now using enabled field on each endpoint
        self.retrieval_endpoints: Dict[str, RetrievalProviderConfig] = {}
        
        # Get the write endpoint for database modifications
        self.write_endpoint: str = data.get("write_endpoint", None)

        # Changed from providers to endpoints
        for name, cfg in data.get("endpoints", {}).items():
            # Use the new method for all configuration values
            self.retrieval_endpoints[name] = RetrievalProviderConfig(
                api_key=self._get_config_value(cfg.get("api_key_env")),
                api_key_env=cfg.get("api_key_env"),  # Store the env var name
                api_endpoint=self._get_config_value(cfg.get("api_endpoint_env")),
                api_endpoint_env=cfg.get("api_endpoint_env"),  # Store the env var name
                database_path=self._get_config_value(cfg.get("database_path")),
                index_name=self._get_config_value(cfg.get("index_name")),
                db_type=self._get_config_value(cfg.get("db_type")),  # Add db_type
                enabled=cfg.get("enabled", False),  # Add enabled field
                use_knn=cfg.get("use_knn"),
                vector_type=cfg.get("vector_type")
            )
    
    
    def get_embedding_provider(self, provider_name: Optional[str] = None) -> Optional[EmbeddingProviderConfig]:
        """Get the specified embedding provider config or the preferred one if not specified."""
        if not hasattr(self, 'embedding_providers'):
            return None
            
        if provider_name and provider_name in self.embedding_providers:
            return self.embedding_providers[provider_name]
            
        if hasattr(self, 'preferred_embedding_provider') and self.preferred_embedding_provider in self.embedding_providers:
            return self.embedding_providers[self.preferred_embedding_provider]
            
        return None
            
    def is_production_mode(self) -> bool:
        """Returns True if the system is running in production mode."""
        return getattr(self, 'mode', 'production').lower() == 'production'
    
    def is_development_mode(self) -> bool:
        """Returns True if the system is running in development mode."""
        return getattr(self, 'mode', 'production').lower() == 'development'
    
    def is_testing_mode(self) -> bool:
        """Returns True if the system is running in testing mode."""
        return getattr(self, 'mode', 'production').lower() == 'testing'


# Global singleton
CONFIG = AppConfig()