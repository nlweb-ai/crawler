#!/bin/bash

# All-in-one script to create all Azure resources and deploy the crawler
# This script will create everything from scratch in a specified resource group

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ASCII Art Banner
echo -e "${CYAN}"
cat << "EOF"
 ╔═══════════════════════════════════════════════════════════╗
 ║     Azure Crawler - Complete Setup & Deployment           ║
 ║     Creating all resources and deploying to AKS           ║
 ╚═══════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Function to print section headers
print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Function to print status
print_status() {
    echo -e "${CYAN}➤${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check prerequisites
print_section "Checking Prerequisites"

# Check Azure CLI
if ! command -v az &> /dev/null; then
    print_error "Azure CLI is not installed"
    echo "  Please install from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
else
    print_success "Azure CLI is installed"
fi

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed"
    echo "  Installing kubectl..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install kubectl
    else
        curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/
    fi
else
    print_success "kubectl is installed"
fi

# Check Docker
if ! command -v docker &> /dev/null; then
    print_warning "Docker is not installed (optional for local builds)"
else
    print_success "Docker is installed"
fi

# Azure Login
print_section "Azure Authentication"

if ! az account show > /dev/null 2>&1; then
    print_status "Please login to Azure..."
    az login
fi

SUBSCRIPTION=$(az account show --query name -o tsv)
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
print_success "Logged in to Azure"
echo "  Subscription: $SUBSCRIPTION"
echo "  ID: $SUBSCRIPTION_ID"

# Get user inputs
print_section "Configuration"

# Resource Group
echo ""
read -p "Enter Resource Group name (or press Enter for 'crawler-rg'): " RESOURCE_GROUP
RESOURCE_GROUP=${RESOURCE_GROUP:-crawler-rg}

# Location
echo ""
echo "Available regions: eastus, westus2, northeurope, westeurope, southeastasia"
read -p "Enter Azure region (or press Enter for 'eastus'): " LOCATION
LOCATION=${LOCATION:-eastus}

# Naming prefix (for unique resource names)
echo ""
read -p "Enter a unique prefix for resource names (or press Enter for auto): " PREFIX
if [ -z "$PREFIX" ]; then
    PREFIX="crawler$(date +%H%M)"
fi

# Service tier selection
echo ""
echo "Select service tier:"
echo "  1) Development (Minimal cost - Free/Basic tiers where possible)"
echo "  2) Production (Standard tiers with redundancy)"
read -p "Enter choice (1 or 2): " TIER_CHOICE

if [ "$TIER_CHOICE" == "2" ]; then
    SQL_SKU="S0"
    SERVICEBUS_SKU="Standard"
    SEARCH_SKU="basic"
    AKS_VM_SIZE="Standard_B4ms"
    AKS_NODE_COUNT=3
    STORAGE_SKU="Standard_GRS"
else
    SQL_SKU="Basic"
    SERVICEBUS_SKU="Basic"
    SEARCH_SKU="free"
    AKS_VM_SIZE="Standard_B2s"
    AKS_NODE_COUNT=2
    STORAGE_SKU="Standard_LRS"
fi

# Generate unique resource names
ACR_NAME="yoastcontainerregistry"
AKS_NAME="${PREFIX}-aks"
SERVICEBUS_NAMESPACE="${PREFIX}-servicebus"
SQL_SERVER="${PREFIX}-sql"
STORAGE_ACCOUNT="${PREFIX}stor"
SEARCH_SERVICE="${PREFIX}-search"

# Passwords
SQL_ADMIN="sqladmin"
# Check if .env exists and has a password - preserve it to avoid mismatch with existing SQL Server
PROJECT_ROOT_CHECK="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "$PROJECT_ROOT_CHECK/.env" ] && grep -q "DB_PASSWORD=" "$PROJECT_ROOT_CHECK/.env"; then
    EXISTING_SQL_PASSWORD=$(grep "DB_PASSWORD=" "$PROJECT_ROOT_CHECK/.env" | cut -d'=' -f2)
    if [ ! -z "$EXISTING_SQL_PASSWORD" ]; then
        SQL_PASSWORD="$EXISTING_SQL_PASSWORD"
        print_status "Preserving existing SQL password from .env file"
    else
        SQL_PASSWORD="Pass@word$(date +%s)!"
    fi
else
    SQL_PASSWORD="Pass@word$(date +%s)!"
fi

# Display configuration
print_section "Configuration Summary"

echo -e "${MAGENTA}Resource Group:${NC} $RESOURCE_GROUP"
echo -e "${MAGENTA}Location:${NC} $LOCATION"
echo -e "${MAGENTA}Resource Prefix:${NC} $PREFIX"
echo ""

# Check for existing resources
print_status "Scanning resource group for existing resources..."
EXISTING_ACR_CHECK=$(az acr list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")
EXISTING_AKS_CHECK=$(az aks list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")
EXISTING_SERVICEBUS_CHECK=$(az servicebus namespace list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")
EXISTING_SQL_CHECK=$(az sql server list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")
EXISTING_SEARCH_CHECK=$(az search service list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

echo ""
if [ ! -z "$EXISTING_ACR_CHECK" ] || [ ! -z "$EXISTING_AKS_CHECK" ] || [ ! -z "$EXISTING_SERVICEBUS_CHECK" ] || [ ! -z "$EXISTING_SQL_CHECK" ] || [ ! -z "$EXISTING_SEARCH_CHECK" ]; then
    echo "Existing resources found:"
    [ ! -z "$EXISTING_ACR_CHECK" ] && echo "  • Container Registry: $EXISTING_ACR_CHECK"
    [ ! -z "$EXISTING_AKS_CHECK" ] && echo "  • AKS Cluster: $EXISTING_AKS_CHECK"
    [ ! -z "$EXISTING_SERVICEBUS_CHECK" ] && echo "  • Service Bus: $EXISTING_SERVICEBUS_CHECK"
    [ ! -z "$EXISTING_SQL_CHECK" ] && echo "  • SQL Server: $EXISTING_SQL_CHECK"
    [ ! -z "$EXISTING_SEARCH_CHECK" ] && echo "  • AI Search: $EXISTING_SEARCH_CHECK"
    echo ""
fi

echo "Resources to be created:"
WILL_CREATE=false
if [ -z "$EXISTING_ACR_CHECK" ]; then
    echo "  • Container Registry: $ACR_NAME"
    WILL_CREATE=true
fi
if [ -z "$EXISTING_AKS_CHECK" ]; then
    echo "  • AKS Cluster: $AKS_NAME ($AKS_NODE_COUNT nodes, $AKS_VM_SIZE)"
    WILL_CREATE=true
fi
if [ -z "$EXISTING_SERVICEBUS_CHECK" ]; then
    echo "  • Service Bus: $SERVICEBUS_NAMESPACE ($SERVICEBUS_SKU)"
    WILL_CREATE=true
fi
if [ -z "$EXISTING_SQL_CHECK" ]; then
    echo "  • SQL Server: $SQL_SERVER ($SQL_SKU)"
    WILL_CREATE=true
fi
if [ -z "$EXISTING_SEARCH_CHECK" ]; then
    echo "  • AI Search: $SEARCH_SERVICE ($SEARCH_SKU)"
    WILL_CREATE=true
fi

if [ "$WILL_CREATE" = false ]; then
    echo "  (None - all resources already exist)"
fi
echo ""

read -p "Continue with this configuration? (y/n): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled."
    exit 1
fi

# Create Resource Group
print_section "Creating Resource Group"

if az group show --name $RESOURCE_GROUP &> /dev/null; then
    print_success "Resource group '$RESOURCE_GROUP' already exists"
else
    print_status "Creating resource group '$RESOURCE_GROUP'..."
    az group create --name $RESOURCE_GROUP --location $LOCATION > /dev/null
    print_success "Created resource group"
fi

# Storage Account - SKIPPED (not used by the application)
# The crawler doesn't currently use blob storage
print_status "Skipping Storage Account creation (not needed by crawler)"

# Create or reuse Service Bus
print_section "Service Bus"

print_status "Checking for existing Service Bus namespace..."
EXISTING_SERVICEBUS=$(az servicebus namespace list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_SERVICEBUS" ]; then
    SERVICEBUS_NAMESPACE=$EXISTING_SERVICEBUS
    print_success "Using existing Service Bus namespace: $SERVICEBUS_NAMESPACE"
else
    print_status "Creating Service Bus namespace '$SERVICEBUS_NAMESPACE'..."
    print_warning "Applying organization security policy (disabling local auth)"

    # Create Service Bus with Azure AD authentication only (no connection strings)
    az servicebus namespace create \
        --resource-group $RESOURCE_GROUP \
        --name $SERVICEBUS_NAMESPACE \
        --location $LOCATION \
        --sku $SERVICEBUS_SKU \
        --disable-local-auth true > /dev/null

    print_success "Created Service Bus with enhanced security (Azure AD auth only)"
fi

# Get Service Bus resource ID for role assignments
SERVICEBUS_ID=$(az servicebus namespace show \
    --resource-group $RESOURCE_GROUP \
    --name $SERVICEBUS_NAMESPACE \
    --query id -o tsv)

# Create queue if it doesn't exist
print_status "Ensuring crawler-queue exists..."
az servicebus queue create \
    --resource-group $RESOURCE_GROUP \
    --namespace-name $SERVICEBUS_NAMESPACE \
    --name crawler-queue \
    --max-size 1024 \
    --default-message-time-to-live P14D > /dev/null 2>&1 || true

# Assign Azure Service Bus Data Owner role to current user
az role assignment create \
    --role "Azure Service Bus Data Owner" \
    --assignee "$CURRENT_USER" \
    --scope "$SERVICEBUS_ID" > /dev/null 2>&1 || true

# Since local auth is disabled, we don't have a connection string
# Applications will use Azure AD authentication
SERVICEBUS_CONNECTION="USE_AZURE_AD_AUTH"

# Create or reuse SQL Database
print_section "SQL Database"

print_status "Checking for existing SQL Server..."
EXISTING_SQL=$(az sql server list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_SQL" ]; then
    SQL_SERVER=$EXISTING_SQL
    print_success "Using existing SQL Server: $SQL_SERVER"

    # IMPORTANT: Do NOT update password for yoast-crawl - it's managed externally
    if [ "$SQL_SERVER" != "yoast-crawl" ]; then
        print_status "Updating SQL Server admin password to match .env..."
        az sql server update \
            --resource-group $RESOURCE_GROUP \
            --name $SQL_SERVER \
            --admin-password "$SQL_PASSWORD" > /dev/null
        print_success "SQL Server password updated"
    else
        print_warning "Using existing SQL Server 'yoast-crawl' - password NOT updated (managed externally)"
    fi

    # Get the first non-master database (user database)
    EXISTING_DB=$(az sql db list --resource-group $RESOURCE_GROUP --server $SQL_SERVER --query "[?name!='master'].name | [0]" -o tsv 2>/dev/null || echo "")
    if [ ! -z "$EXISTING_DB" ]; then
        print_success "Using existing database: $EXISTING_DB"
        SQL_DATABASE=$EXISTING_DB
    else
        print_warning "No user database found on SQL Server - you'll need to create one manually"
        SQL_DATABASE="CrawlerDB"
    fi
else
    print_status "Creating SQL Server '$SQL_SERVER'..."
    az sql server create \
        --resource-group $RESOURCE_GROUP \
        --name $SQL_SERVER \
        --location $LOCATION \
        --admin-user $SQL_ADMIN \
        --admin-password "$SQL_PASSWORD" > /dev/null

    az sql db create \
        --resource-group $RESOURCE_GROUP \
        --server $SQL_SERVER \
        --name CrawlerDB \
        --service-objective $SQL_SKU \
        --zone-redundant false > /dev/null

    print_success "Created SQL Server and database"
fi

# Ensure firewall rule exists
print_status "Ensuring Azure Services firewall rule exists..."
az sql server firewall-rule create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 > /dev/null 2>&1 || true

# Create or reuse Azure AI Search
print_section "Azure AI Search (Vector Database)"

print_status "Checking for existing AI Search service..."
EXISTING_SEARCH=$(az search service list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_SEARCH" ]; then
    SEARCH_SERVICE=$EXISTING_SEARCH
    print_success "Using existing AI Search service: $SEARCH_SERVICE"
else
    print_status "Creating AI Search service '$SEARCH_SERVICE'..."

    # Check if free tier is already used in subscription
    if [ "$SEARCH_SKU" == "free" ]; then
        FREE_SEARCH=$(az search service list --query "[?sku.name=='free'] | [0].name" -o tsv 2>/dev/null || echo "")
        if [ ! -z "$FREE_SEARCH" ]; then
            print_warning "Free tier AI Search already exists: $FREE_SEARCH"
            print_status "Using Basic tier instead..."
            SEARCH_SKU="basic"
        fi
    fi

    az search service create \
        --resource-group $RESOURCE_GROUP \
        --name $SEARCH_SERVICE \
        --sku $SEARCH_SKU \
        --location $LOCATION > /dev/null

    print_success "Created AI Search service"
fi

SEARCH_KEY=$(az search admin-key show \
    --resource-group $RESOURCE_GROUP \
    --service-name $SEARCH_SERVICE \
    --query primaryKey -o tsv)

SEARCH_ENDPOINT="https://${SEARCH_SERVICE}.search.windows.net"

# Create or reuse Container Registry
print_section "Container Registry"

print_status "Checking for existing container registry..."
EXISTING_ACR=$(az acr list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_ACR" ]; then
    ACR_NAME=$EXISTING_ACR
    print_success "Using existing container registry: $ACR_NAME"
else
    print_status "Creating container registry '$ACR_NAME'..."
    az acr create \
        --resource-group $RESOURCE_GROUP \
        --name $ACR_NAME \
        --sku Basic > /dev/null

    print_success "Created container registry"
fi

ACR_LOGIN_SERVER="yoastcontainerregistry.azurecr.io"

# Create or reuse AKS Cluster
print_section "AKS Cluster"

print_status "Checking for existing AKS cluster..."
EXISTING_AKS=$(az aks list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_AKS" ]; then
    AKS_NAME=$EXISTING_AKS
    print_success "Using existing AKS cluster: $AKS_NAME"

    # Ensure ACR is attached
    print_status "Ensuring ACR is attached to AKS..."
    az aks update \
        --resource-group $RESOURCE_GROUP \
        --name $AKS_NAME \
        --attach-acr $ACR_NAME > /dev/null 2>&1 || true
else
    # Register Microsoft.ContainerService provider if not already registered
    print_status "Checking AKS provider registration..."
    PROVIDER_STATUS=$(az provider show --namespace Microsoft.ContainerService --query registrationState -o tsv 2>/dev/null || echo "NotRegistered")

    if [ "$PROVIDER_STATUS" != "Registered" ]; then
        print_warning "Microsoft.ContainerService provider not registered. Registering now..."
        az provider register --namespace Microsoft.ContainerService

        # Wait for registration to complete (up to 5 minutes)
        echo "Waiting for provider registration (this may take up to 5 minutes)..."
        for i in {1..30}; do
            STATUS=$(az provider show --namespace Microsoft.ContainerService --query registrationState -o tsv)
            if [ "$STATUS" == "Registered" ]; then
                print_success "Provider registered successfully"
                break
            fi
            echo -n "."
            sleep 10
        done
        echo ""
    else
        print_success "AKS provider already registered"
    fi

    print_status "Creating AKS cluster '$AKS_NAME' with Managed Identity (this may take 5-10 minutes)..."
    az aks create \
        --resource-group $RESOURCE_GROUP \
        --name $AKS_NAME \
        --node-count $AKS_NODE_COUNT \
        --node-vm-size $AKS_VM_SIZE \
        --attach-acr $ACR_NAME \
        --generate-ssh-keys \
        --enable-managed-identity \
        --enable-cluster-autoscaler \
        --min-count 1 \
        --max-count 5 > /dev/null

    print_success "Created AKS cluster with Managed Identity"
fi

# Get AKS credentials
print_status "Configuring kubectl..."
az aks get-credentials --resource-group $RESOURCE_GROUP --name $AKS_NAME --overwrite-existing

print_success "kubectl configured"

# Configure Managed Identity for Azure resources
print_section "Configuring Managed Identity"

# Get the kubelet identity (used by nodes to pull images and access resources)
print_status "Getting AKS Kubelet Managed Identity..."
KUBELET_IDENTITY=$(az aks show \
    --resource-group $RESOURCE_GROUP \
    --name $AKS_NAME \
    --query identityProfile.kubeletidentity.clientId -o tsv)

print_status "Kubelet Identity ID: $KUBELET_IDENTITY"

# Grant Service Bus Data Owner role to Kubelet Identity
print_status "Granting Service Bus permissions to Managed Identity..."
az role assignment create \
    --role "Azure Service Bus Data Owner" \
    --assignee $KUBELET_IDENTITY \
    --scope "$SERVICEBUS_ID" > /dev/null 2>&1 || true

print_success "Configured Managed Identity with Service Bus permissions"

# Get tenant ID for configuration
AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)
AZURE_CLIENT_ID=$KUBELET_IDENTITY

# Generate .env file
print_section "Generating Configuration Files"

# Get the directory where this script is located and find project root
SCRIPT_DIR_TEMP="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT_TEMP="$(dirname "$SCRIPT_DIR_TEMP")"

cat > "$PROJECT_ROOT_TEMP/.env" << EOF
# Generated by setup-and-deploy.sh on $(date)
# Resource Group: $RESOURCE_GROUP

# Resource Group
RESOURCE_GROUP=$RESOURCE_GROUP

# Queue Configuration (Azure AD Auth Only - No Connection Strings)
QUEUE_TYPE=servicebus
AZURE_SERVICEBUS_NAMESPACE=$SERVICEBUS_NAMESPACE
AZURE_SERVICE_BUS_QUEUE_NAME=crawler-queue
# Note: Using Azure AD auth - no connection string available due to security policy

# Database Configuration
DB_SERVER=$SQL_SERVER.database.windows.net
DB_DATABASE=$SQL_DATABASE
DB_USERNAME=$SQL_ADMIN
DB_PASSWORD=$SQL_PASSWORD

# Azure AI Search (Vector Database)
AZURE_SEARCH_ENDPOINT=$SEARCH_ENDPOINT
AZURE_SEARCH_KEY=$SEARCH_KEY
AZURE_SEARCH_INDEX_NAME=crawler-vectors

# Azure AD Managed Identity (no client secret needed)
AZURE_CLIENT_ID=$AZURE_CLIENT_ID
AZURE_TENANT_ID=$AZURE_TENANT_ID
# Note: Using Managed Identity - no client secret required

# Azure OpenAI (Optional - add your own if available)
# AZURE_OPENAI_ENDPOINT=https://your-openai.openai.azure.com/
# AZURE_OPENAI_KEY=your-key
# AZURE_OPENAI_DEPLOYMENT_NAME=gpt-35-turbo
# AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-3-small
EOF

print_success "Created .env file"

# Create Kubernetes secrets
print_status "Creating Kubernetes secrets..."

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Run the create-secrets script from the correct location
cd "$PROJECT_ROOT"
./azure/create-secrets-from-env.sh > /dev/null

print_success "Created k8s/secrets.yaml"

# Build and push Docker image
print_section "Building and Pushing Docker Image"

print_status "Building unified crawler image using Azure Container Registry..."
echo ""
print_warning "This process may take 5-10 minutes. Build output will be shown below."
echo ""

# Build single unified image
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
print_status "Building crawler image (crawler:latest)..."
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if az acr build \
    --registry $ACR_NAME \
    --image crawler:latest \
    --file k8s/Dockerfile \
    .; then
    print_success "Successfully built crawler image"
else
    print_error "Failed to build crawler image"
    echo ""
    echo -e "${RED}Build failed. Common issues:${NC}"
    echo "  1. Check that k8s/Dockerfile exists"
    echo "  2. Check that code/requirements.txt exists"
    echo "  3. Review the build output above for specific errors"
    echo "  4. Ensure ACR has enough storage quota"
    exit 1
fi

echo ""

# Verify image was pushed to ACR
print_section "Verifying Image in Container Registry"

print_status "Checking for crawler:latest in ACR..."
if az acr repository show --name $ACR_NAME --repository crawler > /dev/null 2>&1; then
    CRAWLER_TAG=$(az acr repository show-tags --name $ACR_NAME --repository crawler --output tsv 2>/dev/null | grep -w "latest" || echo "")
    if [ ! -z "$CRAWLER_TAG" ]; then
        print_success "Crawler image verified in ACR"
    else
        print_error "Crawler image repository exists but 'latest' tag not found"
        exit 1
    fi
else
    print_error "Crawler image not found in ACR"
    exit 1
fi

echo ""
print_success "Image successfully built and pushed to ACR"
echo "  • $ACR_LOGIN_SERVER/crawler:latest"

# Deploy to Kubernetes
print_section "Deploying to Kubernetes"

print_status "Kubernetes manifests already configured with correct image references..."

# Apply Kubernetes resources
print_status "Creating namespace..."
kubectl apply -f k8s/namespace.yaml

print_status "Creating secrets..."
kubectl apply -f k8s/secrets.yaml

print_status "Creating configmap..."
kubectl apply -f k8s/configmap.yaml

print_status "Deploying master..."
kubectl apply -f k8s/master-deployment.yaml
kubectl apply -f k8s/master-service.yaml

print_status "Deploying workers..."
kubectl apply -f k8s/worker-deployment.yaml

print_success "Deployed to Kubernetes"

# Wait for deployment
print_section "Waiting for Deployment"

print_status "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=crawler-master -n crawler --timeout=180s > /dev/null 2>&1 || true
kubectl wait --for=condition=ready pod -l app=crawler-worker -n crawler --timeout=180s > /dev/null 2>&1 || true

# Get external IP
print_status "Getting external IP address..."
EXTERNAL_IP=""
for i in {1..60}; do
    EXTERNAL_IP=$(kubectl get service crawler-master-external -n crawler -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
    if [ ! -z "$EXTERNAL_IP" ]; then
        break
    fi
    sleep 5
done

# Summary
print_section "🎉 Deployment Complete!"

echo ""
echo -e "${GREEN}${BOLD}All resources have been created and deployed successfully!${NC}"
echo ""
echo -e "${CYAN}Resource Group:${NC} $RESOURCE_GROUP"
echo -e "${CYAN}Location:${NC} $LOCATION"
echo ""
echo -e "${YELLOW}Created Resources:${NC}"
echo "  • AKS Cluster: $AKS_NAME"
echo "  • Container Registry: $ACR_NAME"
echo "  • Service Bus: $SERVICEBUS_NAMESPACE"
echo "  • SQL Database: $SQL_SERVER"
echo "  • Storage Account: $STORAGE_ACCOUNT"
echo "  • AI Search: $SEARCH_SERVICE"
echo ""

if [ ! -z "$EXTERNAL_IP" ]; then
    echo -e "${GREEN}${BOLD}Crawler is available at:${NC}"
    echo "  • Web UI: http://$EXTERNAL_IP/"
    echo "  • API: http://$EXTERNAL_IP/api/status"
else
    echo -e "${YELLOW}External IP is still being assigned. Check with:${NC}"
    echo "  kubectl get service crawler-master-external -n crawler"
fi

echo ""
echo -e "${MAGENTA}Useful Commands:${NC}"
echo "  • View pods: kubectl get pods -n crawler"
echo "  • View logs: kubectl logs -n crawler -l app=crawler-master -f"
echo "  • Scale workers: kubectl scale deployment crawler-worker -n crawler --replicas=5"
echo ""
echo -e "${CYAN}Configuration saved to:${NC} .env"
echo -e "${CYAN}Kubernetes secrets:${NC} k8s/secrets.yaml"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Test the API: curl http://$EXTERNAL_IP/api/status"
echo "  2. Add sites via the Web UI"
echo "  3. Monitor with: kubectl logs -n crawler -f"
echo ""

# Cost estimate
if [ "$TIER_CHOICE" == "1" ]; then
    echo -e "${GREEN}Estimated monthly cost (Dev tier): ~\$50-80${NC}"
else
    echo -e "${YELLOW}Estimated monthly cost (Prod tier): ~\$200-300${NC}"
fi

echo ""
echo -e "${RED}Important:${NC} To avoid charges when not in use:"
echo "  • Stop AKS: az aks stop --name $AKS_NAME --resource-group $RESOURCE_GROUP"
echo "  • Delete all: az group delete --name $RESOURCE_GROUP --yes"
echo ""

# Save deployment info
cat > deployment-info.txt << EOF
Deployment Information
Created: $(date)

Resource Group: $RESOURCE_GROUP
Location: $LOCATION

Resources:
- AKS Cluster: $AKS_NAME
- Container Registry: $ACR_NAME
- Service Bus: $SERVICEBUS_NAMESPACE
- SQL Server: $SQL_SERVER
- Storage Account: $STORAGE_ACCOUNT
- AI Search: $SEARCH_SERVICE

Access:
- External IP: $EXTERNAL_IP
- Web UI: http://$EXTERNAL_IP/
- API: http://$EXTERNAL_IP/api/status

Commands:
- View pods: kubectl get pods -n crawler
- View logs: kubectl logs -n crawler -l app=crawler-master -f
- Scale: kubectl scale deployment crawler-worker -n crawler --replicas=N
EOF

print_success "Deployment information saved to: deployment-info.txt"