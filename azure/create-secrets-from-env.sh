#!/bin/bash

# Create k8s/secrets.yaml from your .env file
# This automates the base64 encoding process

set -e

echo "================================================"
echo "Creating Kubernetes Secrets from .env"
echo "================================================"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo "Please ensure your .env file exists with Azure credentials"
    exit 1
fi

# Load environment variables
source .env

# Function to encode value
encode_value() {
    local value="$1"
    if [ -z "$value" ]; then
        echo ""
    else
        echo -n "$value" | base64
    fi
}

# Create secrets.yaml
cat > k8s/secrets.yaml << EOF
# Generated from .env - DO NOT COMMIT TO GIT
apiVersion: v1
kind: Secret
metadata:
  name: crawler-secrets
  namespace: crawler
type: Opaque
data:
  # Azure Service Bus
  service-bus-namespace: $(encode_value "${AZURE_SERVICEBUS_NAMESPACE}.servicebus.windows.net")

  # Azure Storage Queue
  storage-account-name: $(encode_value "$AZURE_STORAGE_ACCOUNT_NAME")

  # Azure AD Managed Identity / Service Principal
  azure-client-id: $(encode_value "$AZURE_CLIENT_ID")
  azure-tenant-id: $(encode_value "$AZURE_TENANT_ID")
  azure-client-secret: $(encode_value "${AZURE_CLIENT_SECRET:-}")

  # Database Credentials
  db-server: $(encode_value "$DB_SERVER")
  db-username: $(encode_value "$DB_USERNAME")
  db-password: $(encode_value "$DB_PASSWORD")

  # Azure OpenAI (Optional)
  azure-openai-endpoint: $(encode_value "$AZURE_OPENAI_ENDPOINT")
  azure-openai-key: $(encode_value "$AZURE_OPENAI_KEY")

  # Azure Cognitive Search (Optional)
  azure-search-endpoint: $(encode_value "$AZURE_SEARCH_ENDPOINT")
  azure-search-key: $(encode_value "$AZURE_SEARCH_KEY")
EOF

echo "✓ Created k8s/secrets.yaml from .env"
echo ""
echo "Next steps:"
echo "1. Review k8s/configmap.yaml and update if needed"
echo "2. Run: ./azure/deploy-to-aks.sh"
echo ""
echo "IMPORTANT: k8s/secrets.yaml contains sensitive data!"
echo "DO NOT commit it to version control!"