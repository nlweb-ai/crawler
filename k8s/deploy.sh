#!/bin/bash

# Deploy crawler application to Kubernetes
# Usage: ./k8s/deploy.sh [command]
#
# Commands:
#   apply    - Deploy/update all resources
#   delete   - Delete all resources
#   status   - Check deployment status
#   logs     - Show logs from master and workers

set -e

COMMAND=${1:-"apply"}
NAMESPACE="crawler"

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "================================"
echo "Crawler Kubernetes Deployment"
echo "================================"
echo ""

case $COMMAND in
    apply)
        echo "Deploying crawler application..."
        echo ""

        # Check if secrets.yaml exists
        if [ ! -f "$SCRIPT_DIR/secrets.yaml" ]; then
            echo "ERROR: secrets.yaml not found!"
            echo ""
            echo "Please create secrets.yaml from secrets-template.yaml:"
            echo "  1. Copy k8s/secrets-template.yaml to k8s/secrets.yaml"
            echo "  2. Fill in your base64-encoded secret values"
            echo "  3. Run this script again"
            echo ""
            echo "To encode values: echo -n 'your-value' | base64"
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

        # Deploy master
        echo "Deploying master..."
        kubectl apply -f "$SCRIPT_DIR/master-deployment.yaml"
        kubectl apply -f "$SCRIPT_DIR/master-service.yaml"

        # Deploy workers
        echo "Deploying workers..."
        kubectl apply -f "$SCRIPT_DIR/worker-deployment.yaml"

        echo ""
        echo "Deployment complete!"
        echo ""
        echo "Check status with: $0 status"
        echo "View logs with: $0 logs"
        ;;

    delete)
        echo "Deleting crawler application..."
        echo ""

        kubectl delete -f "$SCRIPT_DIR/worker-deployment.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/master-service.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/master-deployment.yaml" --ignore-not-found=true
        kubectl delete -f "$SCRIPT_DIR/configmap.yaml" --ignore-not-found=true

        if [ -f "$SCRIPT_DIR/secrets.yaml" ]; then
            kubectl delete -f "$SCRIPT_DIR/secrets.yaml" --ignore-not-found=true
        fi

        echo ""
        echo "Resources deleted (namespace retained)"
        echo "To delete namespace: kubectl delete namespace $NAMESPACE"
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
        echo "=== HPA Status ==="
        kubectl get hpa -n $NAMESPACE

        echo ""
        echo "To get external URL:"
        echo "kubectl get service crawler-master-external -n $NAMESPACE"
        ;;

    logs)
        echo "Showing logs..."
        echo ""

        echo "=== Master Logs (last 20 lines) ==="
        kubectl logs -n $NAMESPACE -l app=crawler-master --tail=20

        echo ""
        echo "=== Worker Logs (last 10 lines per pod) ==="
        kubectl logs -n $NAMESPACE -l app=crawler-worker --tail=10 --prefix=true

        echo ""
        echo "For continuous logs:"
        echo "  Master: kubectl logs -n $NAMESPACE -l app=crawler-master -f"
        echo "  Workers: kubectl logs -n $NAMESPACE -l app=crawler-worker -f --prefix=true"
        ;;

    scale)
        REPLICAS=${2:-5}
        echo "Scaling workers to $REPLICAS replicas..."
        kubectl scale deployment crawler-worker -n $NAMESPACE --replicas=$REPLICAS
        echo "Scaled successfully"
        ;;

    *)
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  apply    - Deploy/update all resources"
        echo "  delete   - Delete all resources"
        echo "  status   - Check deployment status"
        echo "  logs     - Show logs from master and workers"
        echo "  scale N  - Scale workers to N replicas"
        exit 1
        ;;
esac