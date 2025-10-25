#!/bin/bash
# Setup script to export environment variables from .env file

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Path to .env file
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    echo "Loading environment variables from $ENV_FILE"

    # Read .env file and export variables
    while IFS= read -r line; do
        # Skip comments and empty lines
        if [[ ! "$line" =~ ^# ]] && [[ -n "$line" ]]; then
            # Export the variable
            export "$line"
            # Extract variable name for display
            var_name="${line%%=*}"
            echo "  Exported: $var_name"
        fi
    done < "$ENV_FILE"

    echo "Environment variables loaded successfully!"
else
    echo "Error: .env file not found at $ENV_FILE"
    echo "Please create a .env file based on .env.example"
    exit 1
fi