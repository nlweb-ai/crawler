#!/bin/bash

# Create a static IP address for stable URL access

set -e

# Configuration
if [ -f .env ]; then
    source .env
fi

RESOURCE_GROUP="${1:-${RESOURCE_GROUP:-Yoast}}"
LOCATION="${2:-${LOCATION:-eastus}}"
IP_NAME="${3:-crawler-static-ip}"

echo "================================================"
echo "Creating Static IP for Crawler"
echo "================================================"
echo ""
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "IP Name: $IP_NAME"
echo ""

# Check if logged in to Azure
if ! az account show > /dev/null 2>&1; then
    echo "Please login to Azure first:"
    az login
fi

# Check if static IP already exists
EXISTING_IP=$(az network public-ip show \
    --resource-group $RESOURCE_GROUP \
    --name $IP_NAME \
    --query ipAddress -o tsv 2>/dev/null || echo "")

if [ ! -z "$EXISTING_IP" ]; then
    echo "✓ Static IP already exists: $EXISTING_IP"
    STATIC_IP=$EXISTING_IP
else
    echo "Creating static IP..."
    az network public-ip create \
        --resource-group $RESOURCE_GROUP \
        --name $IP_NAME \
        --sku Standard \
        --allocation-method static \
        --location $LOCATION

    STATIC_IP=$(az network public-ip show \
        --resource-group $RESOURCE_GROUP \
        --name $IP_NAME \
        --query ipAddress -o tsv)

    echo "✓ Created static IP: $STATIC_IP"
fi

# Create updated service manifest
echo ""
echo "Creating updated Kubernetes service manifest..."

cat > k8s/master-service-static.yaml << EOF
apiVersion: v1
kind: Service
metadata:
  name: crawler-master-external
  namespace: crawler
  annotations:
    service.beta.kubernetes.io/azure-load-balancer-resource-group: $RESOURCE_GROUP
    service.beta.kubernetes.io/azure-dns-label-name: crawler-$RESOURCE_GROUP
spec:
  selector:
    app: crawler-master
  ports:
  - port: 80
    targetPort: 5001
    protocol: TCP
    name: http
  type: LoadBalancer
  loadBalancerIP: $STATIC_IP
EOF

echo "✓ Created k8s/master-service-static.yaml"
echo ""
echo "To apply the static IP to your deployment:"
echo "  kubectl apply -f k8s/master-service-static.yaml"
echo ""
echo "================================================"
echo "Your stable URLs will be:"
echo "================================================"
echo ""
echo "Static IP URLs:"
echo "  • Web UI: http://$STATIC_IP/"
echo "  • API Status: http://$STATIC_IP/api/status"
echo ""
echo "DNS Label URLs (Azure-provided):"
echo "  • Web UI: http://crawler-$RESOURCE_GROUP.$LOCATION.cloudapp.azure.com/"
echo "  • API Status: http://crawler-$RESOURCE_GROUP.$LOCATION.cloudapp.azure.com/api/status"
echo ""
echo "This IP address will remain the same even if you:"
echo "  • Redeploy the application"
echo "  • Delete and recreate the Kubernetes service"
echo "  • Restart the AKS cluster"
echo ""
echo "To use a custom domain:"
echo "  1. Add an A record pointing to: $STATIC_IP"
echo "  2. Access via: http://yourcrawler.yourdomain.com/"
echo ""

# Save to deployment info
echo "Static IP: $STATIC_IP" >> deployment-info.txt
echo "DNS Label: crawler-$RESOURCE_GROUP.$LOCATION.cloudapp.azure.com" >> deployment-info.txt