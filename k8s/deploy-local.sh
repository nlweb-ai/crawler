#!/bin/bash

# Deploy crawler application to local Kubernetes (Docker Desktop or Minikube)
# This script uses local overrides for reduced resources and NodePort access

set -e

COMMAND=${1:-"apply"}
NAMESPACE="crawler"

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "================================================"
echo "Crawler Local Kubernetes Deployment"
echo "================================================"
echo ""

# Check if using minikube
if kubectl config current-context | grep -q "minikube"; then
    echo "Using Minikube context"
    # Point docker to minikube's docker daemon
    eval $(minikube docker-env)
    ACCESS_INFO="minikube service crawler-master-external -n $NAMESPACE"
elif kubectl config current-context | grep -q "docker-desktop"; then
    echo "Using Docker Desktop Kubernetes"
    ACCESS_INFO="http://localhost:30001"
else
    echo "Warning: Unknown Kubernetes context"
    ACCESS_INFO="Check your service configuration"
fi

case $COMMAND in
    apply)
        echo "Building images locally..."
        echo ""

        # Build images locally (no push to registry)
        docker build -f "$SCRIPT_DIR/Dockerfile.master" -t crawler-master:latest "$SCRIPT_DIR/.."
        docker build -f "$SCRIPT_DIR/Dockerfile.worker" -t crawler-worker:latest "$SCRIPT_DIR/.."

        echo ""
        echo "Deploying crawler application (local mode)..."
        echo ""

        # Check if secrets.yaml exists
        if [ ! -f "$SCRIPT_DIR/secrets.yaml" ]; then
            echo "ERROR: secrets.yaml not found!"
            echo ""
            echo "Please create secrets.yaml from secrets-template.yaml:"
            echo "  1. Copy k8s/secrets-template.yaml to k8s/secrets.yaml"
            echo "  2. Fill in your base64-encoded secret values"
            echo ""
            echo "Quick encoding example:"
            echo "  echo -n 'your-value' | base64"
            exit 1
        fi

        # Apply namespace
        echo "Creating namespace..."
        kubectl apply -f "$SCRIPT_DIR/namespace.yaml"

        # Apply secrets
        echo "Creating secrets..."
        kubectl apply -f "$SCRIPT_DIR/secrets.yaml"

        # Apply configmap
        echo "Creating configmap..."
        kubectl apply -f "$SCRIPT_DIR/configmap.yaml"

        # Deploy master with local overrides
        echo "Deploying master..."
        kubectl apply -f "$SCRIPT_DIR/local/master-deployment-local.yaml"
        kubectl apply -f "$SCRIPT_DIR/master-service.yaml"
        kubectl apply -f "$SCRIPT_DIR/local/master-service-local.yaml"

        # Deploy workers with local overrides
        echo "Deploying workers..."
        kubectl apply -f "$SCRIPT_DIR/local/worker-deployment-local.yaml"

        echo ""
        echo "Local deployment complete!"
        echo ""
        echo "Access the API at: $ACCESS_INFO"
        echo ""
        echo "Check status with: $0 status"
        echo "View logs with: $0 logs"
        ;;

    delete)
        echo "Deleting crawler application..."
        echo ""

        kubectl delete -f "$SCRIPT_DIR/local/worker-deployment-local.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/local/master-service-local.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/master-service.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/local/master-deployment-local.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/configmap.yaml" --ignore-not-found=true

        if [ -f "$SCRIPT_DIR/secrets.yaml" ]; then
            kubectl delete -f "$SCRIPT_DIR/secrets.yaml" --ignore-not-found=true
        fi

        echo ""
        echo "Resources deleted"
        ;;

    status)
        echo "Checking deployment status..."
        echo ""

        echo "=== Deployments ==="
        kubectl get deployments -n $NAMESPACE

        echo ""
        echo "=== Pods ==="
        kubectl get pods -n $NAMESPACE

        echo ""
        echo "=== Services ==="
        kubectl get services -n $NAMESPACE

        echo ""
        echo "Access the API at: $ACCESS_INFO"
        ;;

    logs)
        echo "Showing logs..."
        echo ""

        echo "=== Master Logs (last 20 lines) ==="
        kubectl logs -n $NAMESPACE -l app=crawler-master --tail=20

        echo ""
        echo "=== Worker Logs (last 10 lines per pod) ==="
        kubectl logs -n $NAMESPACE -l app=crawler-worker --tail=10 --prefix=true
        ;;

    port-forward)
        echo "Setting up port forwarding to master pod..."
        echo "API will be available at http://localhost:5001"
        echo "Press Ctrl+C to stop"
        kubectl port-forward -n $NAMESPACE deployment/crawler-master 5001:5001
        ;;

    *)
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  apply         - Build and deploy locally"
        echo "  delete        - Delete all resources"
        echo "  status        - Check deployment status"
        echo "  logs          - Show logs"
        echo "  port-forward  - Forward local port to master"
        exit 1
        ;;
esac