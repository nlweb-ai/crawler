#!/bin/bash

# Register required Azure resource providers for AKS deployment

set -e

echo "================================================"
echo "Registering Azure Resource Providers"
echo "================================================"
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to register a provider
register_provider() {
    local NAMESPACE=$1
    echo -n "Checking $NAMESPACE... "

    STATUS=$(az provider show --namespace $NAMESPACE --query registrationState -o tsv 2>/dev/null || echo "NotRegistered")

    if [ "$STATUS" == "Registered" ]; then
        echo -e "${GREEN}Already registered${NC}"
    else
        echo -e "${YELLOW}Registering...${NC}"
        az provider register --namespace $NAMESPACE

        # Wait for registration
        echo -n "  Waiting for registration"
        for i in {1..60}; do
            STATUS=$(az provider show --namespace $NAMESPACE --query registrationState -o tsv)
            if [ "$STATUS" == "Registered" ]; then
                echo -e " ${GREEN}Done${NC}"
                break
            fi
            echo -n "."
            sleep 5
        done
    fi
}

# Register required providers
echo "Registering required providers for AKS and related services..."
echo ""

register_provider "Microsoft.ContainerService"  # AKS
register_provider "Microsoft.ContainerRegistry"  # ACR
register_provider "Microsoft.Storage"           # Storage Accounts
register_provider "Microsoft.ServiceBus"        # Service Bus
register_provider "Microsoft.Sql"              # SQL Database
register_provider "Microsoft.Search"           # AI Search
register_provider "Microsoft.Network"          # Networking
register_provider "Microsoft.Compute"          # VMs for AKS nodes
register_provider "Microsoft.OperationalInsights"  # Monitoring
register_provider "Microsoft.OperationsManagement"  # Monitoring

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}All providers registered successfully!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "You can now run: ./azure/setup-and-deploy.sh"