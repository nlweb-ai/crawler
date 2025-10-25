#!/bin/bash

# Build and push Docker images for Kubernetes deployment
# Usage: ./k8s/build.sh [registry]
#
# Examples:
#   ./k8s/build.sh                    # Build locally
#   ./k8s/build.sh myregistry.azurecr.io  # Build and push to Azure Container Registry
#   ./k8s/build.sh docker.io/myuser   # Build and push to Docker Hub

set -e

# Get the registry from command line argument
REGISTRY=${1:-""}

# Set image names
if [ -z "$REGISTRY" ]; then
    MASTER_IMAGE="crawler-master:latest"
    WORKER_IMAGE="crawler-worker:latest"
    echo "Building images locally (no registry specified)"
else
    MASTER_IMAGE="$REGISTRY/crawler-master:latest"
    WORKER_IMAGE="$REGISTRY/crawler-worker:latest"
    echo "Building images for registry: $REGISTRY"
fi

# Get the script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

echo "================================"
echo "Building Crawler Docker Images"
echo "================================"
echo ""
echo "Project root: $PROJECT_ROOT"
echo "Master image: $MASTER_IMAGE"
echo "Worker image: $WORKER_IMAGE"
echo ""

# Build master image
echo "Building master image..."
docker build -f "$SCRIPT_DIR/Dockerfile.master" -t "$MASTER_IMAGE" "$PROJECT_ROOT"

# Build worker image
echo "Building worker image..."
docker build -f "$SCRIPT_DIR/Dockerfile.worker" -t "$WORKER_IMAGE" "$PROJECT_ROOT"

# Push to registry if specified
if [ ! -z "$REGISTRY" ]; then
    echo ""
    echo "Pushing images to registry..."
    echo "Pushing master image..."
    docker push "$MASTER_IMAGE"

    echo "Pushing worker image..."
    docker push "$WORKER_IMAGE"

    echo ""
    echo "Images pushed successfully!"
    echo ""
    echo "Update your deployment files to use these images:"
    echo "  Master: $MASTER_IMAGE"
    echo "  Worker: $WORKER_IMAGE"
else
    echo ""
    echo "Images built successfully (local only)!"
    echo ""
    echo "To push to a registry, run:"
    echo "  $0 <registry>"
fi