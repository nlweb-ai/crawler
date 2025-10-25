#!/bin/bash

echo "========================================================================"
echo "AZURE SERVICE BUS SETUP FOR CRAWLER"
echo "========================================================================"
echo ""
echo "This script will help you set up Azure Service Bus for the crawler."
echo "You'll need the Azure CLI installed and logged in."
echo ""

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI not found. Please install it first:"
    echo "   brew update && brew install azure-cli"
    echo ""
    exit 1
fi

# Check if logged in
if ! az account show &> /dev/null; then
    echo "❌ Not logged into Azure. Running 'az login'..."
    az login
fi

echo "✓ Azure CLI is installed and authenticated"
echo ""

# Get or set resource group
echo "Enter resource group name (or press Enter for 'crawler-dev-rg'):"
read -r RESOURCE_GROUP
RESOURCE_GROUP=${RESOURCE_GROUP:-crawler-dev-rg}

# Get or set location
echo "Enter Azure region (or press Enter for 'eastus'):"
read -r LOCATION
LOCATION=${LOCATION:-eastus}

# Get or set namespace name
echo "Enter Service Bus namespace name (must be globally unique):"
echo "Suggested: crawler-sb-$(whoami)-$(date +%s)"
read -r NAMESPACE
if [ -z "$NAMESPACE" ]; then
    NAMESPACE="crawler-sb-$(whoami)-$(date +%s)"
    echo "Using: $NAMESPACE"
fi

# Get or set SKU
echo "Enter SKU (Basic/Standard/Premium, or press Enter for 'Standard'):"
echo "Note: Standard supports topics/subscriptions, Basic is queues only"
read -r SKU
SKU=${SKU:-Standard}

echo ""
echo "========================================================================"
echo "Creating Azure Service Bus with:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  Namespace: $NAMESPACE"
echo "  SKU: $SKU"
echo "========================================================================"
echo ""

# Create resource group if it doesn't exist
echo "Creating resource group..."
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" 2>/dev/null || true

# Create Service Bus namespace with organization's security requirements
echo "Creating Service Bus namespace (this may take a minute)..."
echo "Note: Disabling local auth per organization policy (will use Azure AD)"

if az servicebus namespace create \
    --name "$NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku "$SKU" \
    --disable-local-auth true; then
    echo "✓ Service Bus namespace created with Azure AD authentication"
else
    echo "❌ Failed to create namespace. It may already exist or the name may be taken."
    exit 1
fi

# Create the jobs queue
echo "Creating 'jobs' queue..."
if az servicebus queue create \
    --name "jobs" \
    --namespace-name "$NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --max-delivery-count 3 \
    --default-message-time-to-live "P1D" \
    --lock-duration "PT5M"; then
    echo "✓ Queue 'jobs' created with:"
    echo "  - Max delivery count: 3 (retries)"
    echo "  - Message TTL: 1 day"
    echo "  - Lock duration: 5 minutes"
else
    echo "❌ Failed to create queue"
    exit 1
fi

# Get connection string
echo ""
echo "Getting connection string..."
CONNECTION_STRING=$(az servicebus namespace authorization-rule keys list \
    --namespace-name "$NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString \
    --output tsv)

if [ -z "$CONNECTION_STRING" ]; then
    echo "❌ Failed to get connection string"
    exit 1
fi

# Update .env file
echo ""
echo "========================================================================"
echo "CONFIGURATION"
echo "========================================================================"
echo ""
echo "Add these to your .env file:"
echo ""
echo "# Queue Configuration for Azure Service Bus"
echo "QUEUE_TYPE=servicebus"
echo "AZURE_SERVICEBUS_CONNECTION_STRING=$CONNECTION_STRING"
echo ""
echo "========================================================================"
echo ""

# Ask if we should update .env automatically
echo "Do you want to update .env file automatically? (y/n)"
read -r UPDATE_ENV

if [ "$UPDATE_ENV" = "y" ] || [ "$UPDATE_ENV" = "Y" ]; then
    ENV_FILE=".env"

    # Backup existing .env
    if [ -f "$ENV_FILE" ]; then
        cp "$ENV_FILE" "$ENV_FILE.backup.$(date +%s)"
        echo "✓ Created backup of .env"
    fi

    # Update QUEUE_TYPE
    if grep -q "^QUEUE_TYPE=" "$ENV_FILE"; then
        sed -i '' 's/^QUEUE_TYPE=.*/QUEUE_TYPE=servicebus/' "$ENV_FILE"
    else
        echo "QUEUE_TYPE=servicebus" >> "$ENV_FILE"
    fi

    # Update or add connection string
    if grep -q "^AZURE_SERVICEBUS_CONNECTION_STRING=" "$ENV_FILE"; then
        # Escape special characters in connection string for sed
        ESCAPED_CONN_STR=$(echo "$CONNECTION_STRING" | sed 's/[[\.*^$()+?{|]/\\&/g')
        sed -i '' "s|^AZURE_SERVICEBUS_CONNECTION_STRING=.*|AZURE_SERVICEBUS_CONNECTION_STRING=$ESCAPED_CONN_STR|" "$ENV_FILE"
    else
        echo "AZURE_SERVICEBUS_CONNECTION_STRING=$CONNECTION_STRING" >> "$ENV_FILE"
    fi

    echo "✓ Updated .env file"
    echo ""
    echo "Your system is now configured to use Azure Service Bus!"
    echo "Restart your master and worker processes to use the new queue."
else
    echo ""
    echo "Please manually update your .env file with the configuration above."
fi

echo ""
echo "========================================================================"
echo "USEFUL COMMANDS"
echo "========================================================================"
echo ""
echo "Monitor queue:"
echo "  az servicebus queue show --name jobs --namespace-name $NAMESPACE --resource-group $RESOURCE_GROUP --query 'countDetails'"
echo ""
echo "View messages (peek without consuming):"
echo "  az servicebus queue peek-messages --name jobs --namespace-name $NAMESPACE --resource-group $RESOURCE_GROUP"
echo ""
echo "Delete namespace when done (to avoid charges):"
echo "  az servicebus namespace delete --name $NAMESPACE --resource-group $RESOURCE_GROUP"
echo ""