#!/bin/bash

# Deploy Crawler to Azure Kubernetes Service (AKS)
# Uses your existing Azure resources (Service Bus, SQL Database, Storage)

set -e

echo "================================================"
echo "Deploy Crawler to Azure AKS"
echo "================================================"
echo ""

# Load environment from .env if it exists
if [ -f .env ]; then
    echo "Loading configuration from .env..."
    source .env
fi

# Configuration - Use from .env or defaults
RESOURCE_GROUP="${RESOURCE_GROUP:-crawler-rg}"
LOCATION="${LOCATION:-eastus}"
CLUSTER_NAME="${CLUSTER_NAME:-crawler-aks}"
ACR_NAME="${ACR_NAME:-}"  # Will be created if not exists

echo "Using resource group: $RESOURCE_GROUP"

# Check if logged into Azure
echo "Checking Azure login..."
if ! az account show > /dev/null 2>&1; then
    echo "Please login to Azure first:"
    az login
fi

SUBSCRIPTION=$(az account show --query name -o tsv)
echo "Using subscription: $SUBSCRIPTION"
echo ""

# Step 1: Create AKS cluster if it doesn't exist
echo "Step 1: Checking AKS cluster..."
if az aks show --name $CLUSTER_NAME --resource-group $RESOURCE_GROUP > /dev/null 2>&1; then
    echo "  ✓ AKS cluster '$CLUSTER_NAME' already exists"
else
    echo "  Creating AKS cluster..."
    az aks create \
        --resource-group $RESOURCE_GROUP \
        --name $CLUSTER_NAME \
        --node-count 2 \
        --node-vm-size Standard_B2s \
        --generate-ssh-keys \
        --enable-cluster-autoscaler \
        --min-count 1 \
        --max-count 5
    echo "  ✓ AKS cluster created"
fi

# Step 2: Create Azure Container Registry if needed
echo ""
echo "Step 2: Setting up Container Registry..."

if [ -z "$ACR_NAME" ]; then
    # Try to find existing ACR in resource group
    ACR_NAME=$(az acr list --resource-group $RESOURCE_GROUP --query "[0].name" -o tsv 2>/dev/null || echo "")

    if [ -z "$ACR_NAME" ]; then
        # Create new ACR
        ACR_NAME="crawleracr$(date +%s)"
        echo "  Creating new ACR: $ACR_NAME"
        az acr create \
            --resource-group $RESOURCE_GROUP \
            --name $ACR_NAME \
            --sku Basic

        # Attach ACR to AKS
        az aks update \
            --resource-group $RESOURCE_GROUP \
            --name $CLUSTER_NAME \
            --attach-acr $ACR_NAME
    else
        echo "  ✓ Using existing ACR: $ACR_NAME"
    fi
else
    echo "  ✓ Using specified ACR: $ACR_NAME"
fi

ACR_LOGIN_SERVER=$(az acr show --name $ACR_NAME --query loginServer -o tsv)
echo "  ACR Login Server: $ACR_LOGIN_SERVER"

# Step 3: Build and push Docker images
echo ""
echo "Step 3: Building and pushing Docker images..."
echo "  Building images for: $ACR_LOGIN_SERVER"

# Login to ACR
echo "  Logging into ACR..."
az acr login --name $ACR_NAME

# Build and push using ACR build (no local Docker needed)
echo "  Building master image..."
az acr build \
    --registry $ACR_NAME \
    --image crawler-master:latest \
    --file k8s/Dockerfile.master \
    .

echo "  Building worker image..."
az acr build \
    --registry $ACR_NAME \
    --image crawler-worker:latest \
    --file k8s/Dockerfile.worker \
    .

# Step 4: Get AKS credentials
echo ""
echo "Step 4: Configuring kubectl..."
az aks get-credentials --resource-group $RESOURCE_GROUP --name $CLUSTER_NAME --overwrite-existing
echo "  ✓ kubectl configured for cluster: $CLUSTER_NAME"

# Step 5: Update Kubernetes manifests with ACR
echo ""
echo "Step 5: Updating Kubernetes manifests..."

# Create temporary directory for updated manifests
TEMP_DIR=$(mktemp -d)
cp -r k8s/* $TEMP_DIR/

# Update image references in deployments
sed -i.bak "s|image: crawler-master:latest|image: $ACR_LOGIN_SERVER/crawler-master:latest|g" \
    $TEMP_DIR/master-deployment.yaml

sed -i.bak "s|image: crawler-worker:latest|image: $ACR_LOGIN_SERVER/crawler-worker:latest|g" \
    $TEMP_DIR/worker-deployment.yaml

echo "  ✓ Updated image references to use ACR"

# Step 6: Check for secrets
echo ""
echo "Step 6: Checking Kubernetes secrets..."

if [ ! -f k8s/secrets.yaml ]; then
    echo ""
    echo "ERROR: k8s/secrets.yaml not found!"
    echo ""
    echo "Please create k8s/secrets.yaml from your existing Azure resources:"
    echo "  1. Copy k8s/secrets-template.yaml to k8s/secrets.yaml"
    echo "  2. Fill in base64-encoded values from your .env file"
    echo ""
    echo "Quick encoding helper:"
    echo "  echo -n 'your-value' | base64"
    echo ""
    echo "Required secrets from your existing resources:"
    echo "  - Service Bus namespace and credentials"
    echo "  - SQL Database server and credentials"
    echo "  - Storage account credentials"
    echo "  - Azure AD service principal (if using AAD auth)"
    exit 1
fi

# Step 7: Deploy to AKS
echo ""
echo "Step 7: Deploying to AKS..."

echo "  Creating namespace..."
kubectl apply -f $TEMP_DIR/namespace.yaml

echo "  Creating secrets..."
kubectl apply -f k8s/secrets.yaml

echo "  Creating configmap..."
kubectl apply -f $TEMP_DIR/configmap.yaml

echo "  Deploying master..."
kubectl apply -f $TEMP_DIR/master-deployment.yaml
kubectl apply -f $TEMP_DIR/master-service.yaml

echo "  Deploying workers..."
kubectl apply -f $TEMP_DIR/worker-deployment.yaml

# Clean up temp directory
rm -rf $TEMP_DIR

# Step 8: Wait for deployment
echo ""
echo "Step 8: Waiting for deployment to be ready..."

echo "  Waiting for master pod..."
kubectl wait --for=condition=ready pod -l app=crawler-master -n crawler --timeout=120s || true

echo "  Waiting for worker pods..."
kubectl wait --for=condition=ready pod -l app=crawler-worker -n crawler --timeout=120s || true

# Step 9: Get access information
echo ""
echo "================================================"
echo "Deployment Complete!"
echo "================================================"
echo ""

# Get external IP
echo "Getting external IP address..."
EXTERNAL_IP=""
for i in {1..30}; do
    EXTERNAL_IP=$(kubectl get service crawler-master-external -n crawler -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
    if [ ! -z "$EXTERNAL_IP" ]; then
        break
    fi
    echo "  Waiting for external IP... ($i/30)"
    sleep 5
done

if [ ! -z "$EXTERNAL_IP" ]; then
    echo ""
    echo "✓ Crawler API is available at: http://$EXTERNAL_IP"
    echo "  Web UI: http://$EXTERNAL_IP/"
    echo "  API Status: http://$EXTERNAL_IP/api/status"
else
    echo "External IP not yet assigned. Check with:"
    echo "  kubectl get service crawler-master-external -n crawler"
fi

echo ""
echo "Useful commands:"
echo "  View pods:        kubectl get pods -n crawler"
echo "  View logs:"
echo "    Master:         kubectl logs -n crawler -l app=crawler-master -f"
echo "    Workers:        kubectl logs -n crawler -l app=crawler-worker -f"
echo "  Scale workers:    kubectl scale deployment crawler-worker -n crawler --replicas=10"
echo "  Port forward:     kubectl port-forward -n crawler svc/crawler-master-service 5001:5001"
echo ""
echo "To update deployment after code changes:"
echo "  ./azure/deploy-to-aks.sh"
echo ""
echo "To delete deployment (keeps AKS cluster):"
echo "  kubectl delete namespace crawler"
echo ""
echo "To delete everything (including AKS cluster):"
echo "  az aks delete --name $CLUSTER_NAME --resource-group $RESOURCE_GROUP --yes"