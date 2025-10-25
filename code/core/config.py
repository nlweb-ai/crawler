"""
Configuration loader for environment variables.
Loads from .env file if present, otherwise uses system environment.
"""

import os
from pathlib import Path

def load_env():
    """Load environment variables from .env file if it exists"""
    # Find the .env file - look in project root
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent  # Go up to crawler/
    env_file = project_root / '.env'

    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    # Parse KEY=VALUE
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Only set if not already in environment
                        if key not in os.environ:
                            os.environ[key] = value
        print(f"Loaded environment from {env_file}")
    else:
        print(f"No .env file found at {env_file}, using system environment")

# Auto-load when module is imported
load_env()