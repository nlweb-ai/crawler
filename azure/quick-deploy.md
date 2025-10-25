# Quick Azure Deployment Guide

Since you already have Azure resources (Service Bus, SQL Database, Storage), here's how to deploy your crawler to Azure Kubernetes Service (AKS).

## Prerequisites

1. Azure CLI installed and logged in:
```bash
az login
```

2. Your existing `.env` file with Azure credentials (including RESOURCE_GROUP=NLW_rvg)

## Quick Deploy (2 Steps)

### Step 1: Create Kubernetes Secrets (FIRST TIME ONLY)

**You only need to do this ONCE** (or when credentials change):

```bash
# Generate secrets.yaml from your .env file
./azure/create-secrets-from-env.sh
```

This automatically creates `k8s/secrets.yaml` from your `.env` file.

**When to re-run this script:**
- ❌ NOT needed for regular deployments
- ✅ Only when Azure credentials change
- ✅ Only when adding new services
- ✅ Only when passwords change

### Step 2: Deploy to AKS

Since your resource group (NLW_rvg) is now in `.env`, just run:

```bash
# Deploy (uses RESOURCE_GROUP from .env)
./azure/deploy-to-aks.sh
```

The script will:
- Read RESOURCE_GROUP=NLW_rvg from your .env
- Create AKS cluster in that resource group
- Build and deploy everything

This script will:
1. Create an AKS cluster in your existing resource group
2. Create an Azure Container Registry (ACR)
3. Build and push Docker images to ACR
4. Deploy the crawler to AKS
5. Give you the public URL

## What Gets Created

Only these NEW resources are created:
- **AKS Cluster**: For running your containers
- **Azure Container Registry**: For storing Docker images

Your existing resources are USED but NOT modified:
- ✓ Service Bus (uses existing)
- ✓ SQL Database (uses existing)
- ✓ Storage Account (uses existing)

## Access Your Deployment

After deployment:

```bash
# Get the public IP
kubectl get service crawler-master-external -n crawler

# View the UI
http://<EXTERNAL-IP>/

# Check API status
http://<EXTERNAL-IP>/api/status
```

## Monitor

```bash
# View all pods
kubectl get pods -n crawler

# Watch logs
kubectl logs -n crawler -l app=crawler-master -f
kubectl logs -n crawler -l app=crawler-worker -f

# Scale workers
kubectl scale deployment crawler-worker -n crawler --replicas=10
```

## Update After Code Changes

```bash
# Just run the deploy script again
./azure/deploy-to-aks.sh
```

## Cost Management

The script creates:
- **AKS**: 2 nodes (auto-scales 1-5), B2s size (~$30/month per node)
- **ACR**: Basic tier (~$5/month)

Total additional cost: ~$65/month minimum

To minimize costs:
```bash
# Stop AKS when not in use (preserves everything)
az aks stop --name crawler-aks --resource-group $RESOURCE_GROUP

# Start again when needed
az aks start --name crawler-aks --resource-group $RESOURCE_GROUP
```

## Clean Up

```bash
# Delete just the Kubernetes deployment (keeps cluster)
kubectl delete namespace crawler

# Delete AKS cluster (keeps ACR)
az aks delete --name crawler-aks --resource-group $RESOURCE_GROUP --yes

# Delete ACR (if you want)
az acr delete --name <acr-name> --resource-group $RESOURCE_GROUP --yes
```

## Troubleshooting

### Secrets not working?
```bash
# Verify secrets are created
kubectl get secrets -n crawler

# Check pod events for errors
kubectl describe pod <pod-name> -n crawler
```

### Can't connect to Azure resources?
- Check firewall rules on SQL Database
- Verify Service Bus connection string
- Ensure AKS cluster has network access

### Pods crashing?
```bash
# Check logs
kubectl logs <pod-name> -n crawler --previous

# Check resource limits
kubectl top pods -n crawler
```