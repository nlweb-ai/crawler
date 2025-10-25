#!/bin/bash

# Script to clean up all resources in a resource group
# This will delete everything except the resource group itself

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

print_status() {
    echo -e "${BLUE}➤${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Ask for resource group name
echo ""
read -p "Enter Resource Group name to clean up: " RESOURCE_GROUP

if [ -z "$RESOURCE_GROUP" ]; then
    print_error "Resource group name cannot be empty"
    exit 1
fi

# Verify resource group exists
print_status "Checking if resource group '$RESOURCE_GROUP' exists..."
if ! az group show --name $RESOURCE_GROUP > /dev/null 2>&1; then
    print_error "Resource group '$RESOURCE_GROUP' does not exist"
    exit 1
fi

print_success "Resource group found"

# List all resources
print_header "Resources in '$RESOURCE_GROUP'"
az resource list --resource-group $RESOURCE_GROUP --output table

# Count resources
RESOURCE_COUNT=$(az resource list --resource-group $RESOURCE_GROUP --query "length(@)" -o tsv)
echo ""
print_warning "Found $RESOURCE_COUNT resources"

# Confirm deletion
echo ""
echo -e "${RED}WARNING: This will delete ALL resources in the resource group!${NC}"
echo -e "${RED}This action cannot be undone!${NC}"
echo ""
read -p "Are you sure you want to delete all resources? (type 'yes' to confirm): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    print_warning "Cleanup cancelled"
    exit 0
fi

# Delete AKS clusters first (they take the longest)
print_header "Deleting AKS Clusters"
AKS_CLUSTERS=$(az aks list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$AKS_CLUSTERS" ]; then
    while IFS= read -r cluster; do
        print_status "Deleting AKS cluster: $cluster (this may take 5-10 minutes)..."
        az aks delete --resource-group $RESOURCE_GROUP --name $cluster --yes --no-wait
    done <<< "$AKS_CLUSTERS"
    print_success "AKS deletion initiated (running in background)"
else
    print_status "No AKS clusters found"
fi

# Delete SQL Servers (and their databases)
print_header "Deleting SQL Servers"
SQL_SERVERS=$(az sql server list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$SQL_SERVERS" ]; then
    while IFS= read -r server; do
        # IMPORTANT: Never delete yoast-crawl - it's protected
        if [ "$server" == "yoast-crawl" ]; then
            print_warning "SKIPPING SQL Server: $server (protected - will not delete)"
        else
            print_status "Deleting SQL Server: $server..."
            az sql server delete --resource-group $RESOURCE_GROUP --name $server --yes > /dev/null 2>&1 &
        fi
    done <<< "$SQL_SERVERS"
    print_success "SQL Server deletion initiated (protected servers skipped)"
else
    print_status "No SQL Servers found"
fi

# Delete AI Search services
print_header "Deleting AI Search Services"
SEARCH_SERVICES=$(az search service list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$SEARCH_SERVICES" ]; then
    while IFS= read -r search; do
        print_status "Deleting AI Search service: $search..."
        az search service delete --resource-group $RESOURCE_GROUP --name $search --yes
    done <<< "$SEARCH_SERVICES"
    print_success "AI Search services deleted"
else
    print_status "No AI Search services found"
fi

# Delete Service Bus namespaces
print_header "Deleting Service Bus Namespaces"
SERVICEBUS_NAMESPACES=$(az servicebus namespace list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$SERVICEBUS_NAMESPACES" ]; then
    while IFS= read -r namespace; do
        print_status "Deleting Service Bus namespace: $namespace..."
        az servicebus namespace delete --resource-group $RESOURCE_GROUP --name $namespace --no-wait
    done <<< "$SERVICEBUS_NAMESPACES"
    print_success "Service Bus deletion initiated"
else
    print_status "No Service Bus namespaces found"
fi

# Delete Container Registries
print_header "Deleting Container Registries"
ACRS=$(az acr list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$ACRS" ]; then
    while IFS= read -r acr; do
        print_status "Deleting Container Registry: $acr..."
        az acr delete --resource-group $RESOURCE_GROUP --name $acr --yes
    done <<< "$ACRS"
    print_success "Container Registries deleted"
else
    print_status "No Container Registries found"
fi

# Delete Storage Accounts
print_header "Deleting Storage Accounts"
STORAGE_ACCOUNTS=$(az storage account list --resource-group $RESOURCE_GROUP --query "[].name" -o tsv)
if [ ! -z "$STORAGE_ACCOUNTS" ]; then
    while IFS= read -r storage; do
        print_status "Deleting Storage Account: $storage..."
        az storage account delete --resource-group $RESOURCE_GROUP --name $storage --yes
    done <<< "$STORAGE_ACCOUNTS"
    print_success "Storage Accounts deleted"
else
    print_status "No Storage Accounts found"
fi

# Delete any remaining resources
print_header "Deleting Remaining Resources"
REMAINING=$(az resource list --resource-group $RESOURCE_GROUP --query "[].id" -o tsv)
if [ ! -z "$REMAINING" ]; then
    while IFS= read -r resource; do
        print_status "Deleting resource: $resource..."
        az resource delete --ids $resource --no-wait
    done <<< "$REMAINING"
    print_success "Remaining resources deletion initiated"
else
    print_status "No remaining resources found"
fi

# Wait for deletions to complete (only if there were resources to delete)
if [ "$RESOURCE_COUNT" -gt "0" ]; then
    print_header "Waiting for Deletions to Complete"
    print_status "Checking deletion status (this may take several minutes)..."
    sleep 10

    for i in {1..60}; do
        REMAINING_COUNT=$(az resource list --resource-group $RESOURCE_GROUP --query "length(@)" -o tsv)
        if [ "$REMAINING_COUNT" -eq "0" ]; then
            print_success "All resources deleted!"
            break
        fi
        echo -n "."
        sleep 10
    done
    echo ""

    # Final check
    FINAL_COUNT=$(az resource list --resource-group $RESOURCE_GROUP --query "length(@)" -o tsv)
    if [ "$FINAL_COUNT" -eq "0" ]; then
        print_success "Resource group '$RESOURCE_GROUP' is now empty and ready for fresh deployment"
    else
        print_warning "$FINAL_COUNT resources still remain (may still be deleting in background)"
        echo ""
        print_status "Remaining resources:"
        az resource list --resource-group $RESOURCE_GROUP --output table
    fi
else
    print_success "No resources to delete - resource group is already empty"
fi

echo ""
print_header "Cleanup Complete"
echo "You can now run ./setup-and-deploy.sh for a fresh deployment"
echo ""
