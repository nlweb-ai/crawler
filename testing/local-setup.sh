#!/bin/bash

# Setup script for local testing on Mac
# This script helps you set up and test the crawler locally

set -e

echo "================================================"
echo "Crawler Local Testing Setup for Mac"
echo "================================================"
echo ""

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Docker
if command_exists docker; then
    echo "✅ Docker is installed: $(docker --version)"
else
    echo "❌ Docker is not installed"
    echo "   Please install Docker Desktop from: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker is running
if docker info >/dev/null 2>&1; then
    echo "✅ Docker is running"
else
    echo "❌ Docker is not running"
    echo "   Please start Docker Desktop"
    exit 1
fi

# Check kubectl
if command_exists kubectl; then
    echo "✅ kubectl is installed: $(kubectl version --client --short 2>/dev/null)"
else
    echo "⚠️  kubectl is not installed"
    echo "   Installing kubectl..."
    brew install kubectl
fi

echo ""
echo "================================================"
echo "Choose your local testing method:"
echo "================================================"
echo ""
echo "1) Docker Compose (Simplest - recommended for quick testing)"
echo "2) Docker Desktop Kubernetes (Full K8s experience)"
echo "3) Minikube (Alternative K8s environment)"
echo ""
read -p "Enter your choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo "Setting up Docker Compose testing..."
        echo ""

        # Check if .env exists
        if [ ! -f .env ]; then
            echo "Creating .env file from example..."
            if [ -f .env.example ]; then
                cp .env.example .env
                echo "✅ Created .env file - Please edit it with your Azure credentials"
            else
                echo "⚠️  No .env.example found. Creating template .env file..."
                cat > .env << 'EOF'
# Azure Service Bus
AZURE_SERVICE_BUS_NAMESPACE=your-namespace.servicebus.windows.net
AZURE_SERVICE_BUS_QUEUE_NAME=crawler-queue

# Azure AD Service Principal
AZURE_CLIENT_ID=your-client-id
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_SECRET=your-client-secret

# Database
DB_SERVER=your-server.database.windows.net
DB_DATABASE=CrawlerDB
DB_USERNAME=your-username
DB_PASSWORD=your-password

# Blob Storage
BLOB_STORAGE_ACCOUNT_NAME=your-storage-account
BLOB_STORAGE_CONTAINER_NAME=crawler-data

# Optional: Azure OpenAI
AZURE_OPENAI_ENDPOINT=https://your-openai.openai.azure.com/
AZURE_OPENAI_KEY=your-key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-35-turbo

# Optional: Azure Cognitive Search
AZURE_SEARCH_ENDPOINT=https://your-search.search.windows.net
AZURE_SEARCH_KEY=your-key
AZURE_SEARCH_INDEX_NAME=schema-org-index
EOF
                echo "✅ Created template .env file - Please edit it with your Azure credentials"
            fi
        else
            echo "✅ .env file already exists"
        fi

        echo ""
        echo "To start the services:"
        echo "  docker-compose up --build"
        echo ""
        echo "To scale workers:"
        echo "  docker-compose up --build --scale worker=5"
        echo ""
        echo "To stop:"
        echo "  docker-compose down"
        echo ""
        echo "The API will be available at: http://localhost:5001"
        ;;

    2)
        echo ""
        echo "Setting up Docker Desktop Kubernetes..."
        echo ""

        # Check if Kubernetes is enabled in Docker Desktop
        if kubectl config get-contexts | grep -q "docker-desktop"; then
            echo "✅ Docker Desktop Kubernetes is available"

            # Switch to docker-desktop context
            kubectl config use-context docker-desktop
            echo "✅ Switched to docker-desktop context"
        else
            echo "❌ Docker Desktop Kubernetes is not enabled"
            echo ""
            echo "To enable:"
            echo "1. Open Docker Desktop"
            echo "2. Go to Settings/Preferences → Kubernetes"
            echo "3. Check 'Enable Kubernetes'"
            echo "4. Click 'Apply & Restart'"
            echo "5. Wait for Kubernetes to start (green indicator)"
            echo "6. Run this script again"
            exit 1
        fi

        # Create secrets file if it doesn't exist
        if [ ! -f k8s/secrets.yaml ]; then
            echo ""
            echo "Creating secrets.yaml from template..."
            echo "You need to encode your secrets and add them to k8s/secrets.yaml"
            echo ""
            echo "Example encoding:"
            echo "  echo -n 'your-value' | base64"
            echo ""
            read -p "Press enter to continue..."
        fi

        echo ""
        echo "Building images locally..."
        ./k8s/build.sh

        echo ""
        echo "To deploy to local Kubernetes:"
        echo "  ./k8s/deploy.sh apply"
        echo ""
        echo "To check status:"
        echo "  ./k8s/deploy.sh status"
        echo ""
        echo "To get the service URL:"
        echo "  kubectl get service crawler-master-external -n crawler"
        ;;

    3)
        echo ""
        echo "Setting up Minikube..."
        echo ""

        # Check minikube
        if command_exists minikube; then
            echo "✅ Minikube is installed: $(minikube version --short)"
        else
            echo "⚠️  Minikube is not installed"
            echo "   Installing minikube..."
            brew install minikube
        fi

        # Start minikube if not running
        if minikube status >/dev/null 2>&1; then
            echo "✅ Minikube is running"
        else
            echo "Starting Minikube..."
            minikube start --driver=docker --cpus=4 --memory=4096
        fi

        # Use minikube's docker daemon
        echo ""
        echo "Configuring Docker to use Minikube's daemon..."
        eval $(minikube docker-env)

        echo ""
        echo "Building images in Minikube..."
        ./k8s/build.sh

        echo ""
        echo "To deploy to Minikube:"
        echo "  ./k8s/deploy.sh apply"
        echo ""
        echo "To access the service:"
        echo "  minikube service crawler-master-external -n crawler"
        echo ""
        echo "To open dashboard:"
        echo "  minikube dashboard"
        ;;

    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "================================================"
echo "Additional Commands"
echo "================================================"
echo ""
echo "View logs:"
echo "  Docker Compose: docker-compose logs -f"
echo "  Kubernetes: kubectl logs -n crawler -f <pod-name>"
echo ""
echo "Shell into container:"
echo "  Docker Compose: docker-compose exec master bash"
echo "  Kubernetes: kubectl exec -n crawler -it <pod-name> -- bash"
echo ""
echo "Clean up everything:"
echo "  Docker Compose: docker-compose down -v"
echo "  Kubernetes: ./k8s/deploy.sh delete"