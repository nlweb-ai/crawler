#!/bin/bash

echo "========================================================================"
echo "AZURE SERVICE BUS SETUP WITH AAD AUTHENTICATION"
echo "========================================================================"
echo ""
echo "This script will:"
echo "1. Create an Azure Service Bus namespace with AAD-only authentication"
echo "2. Create the jobs queue"
echo "3. Configure your .env file for AAD authentication"
echo "4. Update your Python code to use AAD"
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

# Get current user info for later
CURRENT_USER=$(az account show --query user.name -o tsv)
echo "Current Azure user: $CURRENT_USER"
echo ""

# Get or set resource group
echo "Enter resource group name (or press Enter for 'NLW_rvg'):"
read -r RESOURCE_GROUP
RESOURCE_GROUP=${RESOURCE_GROUP:-NLW_rvg}

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
echo "STEP 1: Creating Azure Service Bus"
echo "========================================================================"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  Namespace: $NAMESPACE"
echo "  SKU: $SKU"
echo "  Authentication: Azure AD only (no connection strings)"
echo ""

# Create resource group if it doesn't exist
echo "Creating resource group..."
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" 2>/dev/null || true

# Create Service Bus namespace with AAD-only authentication
echo "Creating Service Bus namespace (this may take 1-2 minutes)..."
if az servicebus namespace create \
    --name "$NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku "$SKU" \
    --disable-local-auth true; then
    echo "✓ Service Bus namespace created with Azure AD authentication"
else
    echo "❌ Failed to create namespace"
    exit 1
fi

# Create the jobs queue
echo ""
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

# Grant current user permissions
echo ""
echo "========================================================================"
echo "STEP 2: Configuring Permissions"
echo "========================================================================"
echo ""
echo "Granting Azure Service Bus Data Owner role to $CURRENT_USER..."

# Get the resource ID of the namespace
NAMESPACE_ID=$(az servicebus namespace show \
    --name "$NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

# Grant role to current user
if az role assignment create \
    --assignee "$CURRENT_USER" \
    --role "Azure Service Bus Data Owner" \
    --scope "$NAMESPACE_ID" 2>/dev/null; then
    echo "✓ Permissions granted"
else
    echo "⚠️  Role assignment might already exist (that's OK)"
fi

echo ""
echo "========================================================================"
echo "STEP 3: Updating Configuration Files"
echo "========================================================================"
echo ""

# Update .env file
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

# Update or add namespace
if grep -q "^AZURE_SERVICEBUS_NAMESPACE=" "$ENV_FILE"; then
    sed -i '' "s/^AZURE_SERVICEBUS_NAMESPACE=.*/AZURE_SERVICEBUS_NAMESPACE=$NAMESPACE/" "$ENV_FILE"
else
    echo "AZURE_SERVICEBUS_NAMESPACE=$NAMESPACE" >> "$ENV_FILE"
fi

# Remove connection string if it exists (we're using AAD)
sed -i '' '/^AZURE_SERVICEBUS_CONNECTION_STRING=/d' "$ENV_FILE"

echo "✓ Updated .env file"

echo ""
echo "========================================================================"
echo "STEP 4: Updating Python Code"
echo "========================================================================"
echo ""

# Update master.py to use AAD
if [ -f "code/core/master.py" ]; then
    # Check if already using AAD
    if grep -q "from queue_interface_aad import get_queue_with_aad" "code/core/master.py"; then
        echo "✓ master.py already using AAD authentication"
    else
        # Update the import
        sed -i '' 's/from queue_interface import get_queue/from queue_interface_aad import get_queue_with_aad as get_queue/' "code/core/master.py"
        echo "✓ Updated master.py to use AAD authentication"
    fi
else
    echo "⚠️  master.py not found - update manually"
fi

# Update worker.py to use AAD
if [ -f "code/core/worker.py" ]; then
    # Check if already using AAD
    if grep -q "from queue_interface_aad import get_queue_with_aad" "code/core/worker.py"; then
        echo "✓ worker.py already using AAD authentication"
    else
        # Update the import
        sed -i '' 's/from queue_interface import get_queue/from queue_interface_aad import get_queue_with_aad as get_queue/' "code/core/worker.py"
        echo "✓ Updated worker.py to use AAD authentication"
    fi
else
    echo "⚠️  worker.py not found - update manually"
fi

# Install required Python packages
echo ""
echo "Installing required Python packages..."
if [ -f "code/requirements.txt" ]; then
    pip3 install azure-identity azure-servicebus --quiet
    echo "✓ Installed azure-identity and azure-servicebus"
else
    echo "⚠️  requirements.txt not found - install packages manually:"
    echo "   pip3 install azure-identity azure-servicebus"
fi

echo ""
echo "========================================================================"
echo "✅ SETUP COMPLETE!"
echo "========================================================================"
echo ""
echo "Your system is now configured to use Azure Service Bus with AAD!"
echo ""
echo "Configuration:"
echo "  Namespace: $NAMESPACE"
echo "  Queue: jobs"
echo "  Authentication: Azure AD (using your Azure CLI credentials)"
echo ""
echo "To test the setup:"
echo "  1. Restart your master: ./start_master.sh"
echo "  2. Restart your worker: ./start_worker.sh"
echo "  3. Check the web console: http://localhost:5001"
echo ""
echo "The queue will authenticate using:"
echo "  - Your Azure CLI credentials (locally)"
echo "  - Managed Identity (when deployed to Azure)"
echo ""
echo "========================================================================"
echo "USEFUL COMMANDS"
echo "========================================================================"
echo ""
echo "Monitor queue (requires Azure CLI):"
echo "  az servicebus queue show --name jobs \\"
echo "    --namespace-name $NAMESPACE \\"
echo "    --resource-group $RESOURCE_GROUP \\"
echo "    --query messageCount"
echo ""
echo "Delete namespace when done (to avoid charges):"
echo "  az servicebus namespace delete \\"
echo "    --name $NAMESPACE \\"
echo "    --resource-group $RESOURCE_GROUP"
echo ""
echo "Check your permissions:"
echo "  az role assignment list --scope $NAMESPACE_ID"
echo ""