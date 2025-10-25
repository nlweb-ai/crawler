# Azure Deployment Scripts

This directory contains scripts for deploying the crawler to Azure.

## 🚀 Quick Start - Complete Setup

### One Command Setup

The easiest way to deploy everything:

```bash
./azure/setup-and-deploy.sh
```

This interactive script will:
1. Ask for your resource group name (creates it if needed)
2. Let you choose the Azure region
3. Create ALL required Azure resources:
   - Azure Kubernetes Service (AKS)
   - Azure Container Registry (ACR)
   - Azure Service Bus (queue)
   - Azure SQL Database
   - Azure Storage Account
   - Azure AI Search (vector database)
4. Build and push Docker images
5. Deploy everything to Kubernetes
6. Provide you with the public URL

**Time to complete:** ~15-20 minutes

## 📋 What Gets Created

| Resource | Purpose | Tier Options |
|----------|---------|--------------|
| **Resource Group** | Container for all resources | N/A |
| **AKS Cluster** | Runs the crawler containers | 2-5 nodes, auto-scaling |
| **Container Registry** | Stores Docker images | Basic |
| **Service Bus** | Job queue | Basic/Standard |
| **SQL Database** | Stores crawler data | Basic/S0 |
| **Storage Account** | Blob storage for files | LRS/GRS |
| **AI Search** | Vector database for embeddings | Free/Basic |
| **Service Principal** | Azure AD authentication | Contributor role |

## 💰 Cost Estimates

### Development Tier
- **Monthly cost:** ~$50-80
- Uses Free/Basic tiers where possible
- 2 small AKS nodes
- Suitable for testing and development

### Production Tier
- **Monthly cost:** ~$200-300
- Standard tiers with redundancy
- 3+ larger AKS nodes
- Suitable for production workloads

### Cost Optimization

To minimize costs when not in use:

```bash
# Stop AKS cluster (preserves everything)
az aks stop --name <cluster-name> --resource-group <resource-group>

# Start again when needed
az aks start --name <cluster-name> --resource-group <resource-group>

# Delete everything (when done)
az group delete --name <resource-group> --yes
```

## 🛠️ Individual Scripts

### setup-and-deploy.sh
Complete setup from scratch. Creates all resources and deploys.

### deploy-to-aks.sh
Deploy to existing AKS cluster (assumes resources exist).

### create-secrets-from-env.sh
Generate Kubernetes secrets from .env file.

### migrate-to-yoast.sh
Legacy migration script for moving between resource groups.

## 📁 Generated Files

After running `setup-and-deploy.sh`:

- **`.env`** - Environment variables with all Azure resource details
- **`k8s/secrets.yaml`** - Kubernetes secrets (do not commit!)
- **`deployment-info.txt`** - Summary of deployment

## 🔧 Prerequisites

- Azure CLI (`az`)
- kubectl
- An Azure subscription
- Sufficient permissions to create resources

## 📊 Monitoring Your Deployment

### View Status
```bash
# Check pods
kubectl get pods -n crawler

# View logs
kubectl logs -n crawler -l app=crawler-master -f
kubectl logs -n crawler -l app=crawler-worker -f

# Get external IP
kubectl get service crawler-master-external -n crawler
```

### Scale Workers
```bash
# Scale to 10 workers
kubectl scale deployment crawler-worker -n crawler --replicas=10

# Auto-scale based on CPU
kubectl autoscale deployment crawler-worker -n crawler --min=2 --max=20 --cpu-percent=70
```

## 🔒 Security Notes

1. **Secrets Management**
   - Never commit `.env` or `k8s/secrets.yaml` to git
   - Use Azure Key Vault for production

2. **Network Security**
   - Consider using Private Endpoints
   - Enable network policies in AKS
   - Use managed identities when possible

3. **Access Control**
   - Service Principal has Contributor role on resource group
   - Consider using more restrictive roles in production

## 🆘 Troubleshooting

### Script fails during resource creation
- Check Azure subscription limits
- Ensure unique resource names (script adds timestamps)
- Verify you have sufficient permissions

### Cannot access the crawler after deployment
- Wait for external IP assignment (can take 5 minutes)
- Check firewall rules on SQL Database
- Verify Service Bus connection

### Pods not starting
```bash
# Check pod status
kubectl describe pod <pod-name> -n crawler

# Check events
kubectl get events -n crawler --sort-by='.lastTimestamp'
```

## 🔄 Updating Your Deployment

After code changes:

1. **Rebuild images**
```bash
az acr build --registry <acr-name> --image crawler-master:latest --file k8s/Dockerfile.master .
az acr build --registry <acr-name> --image crawler-worker:latest --file k8s/Dockerfile.worker .
```

2. **Restart pods**
```bash
kubectl rollout restart deployment/crawler-master -n crawler
kubectl rollout restart deployment/crawler-worker -n crawler
```

## 📝 Environment Variables

The script generates a complete `.env` file with:

- Resource group configuration
- Service Bus connection details
- SQL Database credentials
- Storage account connection string
- AI Search endpoint and key
- Service Principal credentials

## 🏃 Next Steps

After deployment:

1. **Test the API**
   ```bash
   curl http://<external-ip>/api/status
   ```

2. **Access the Web UI**
   - Open `http://<external-ip>/` in your browser

3. **Add sites to crawl**
   - Use the Web UI or API to add sites

4. **Monitor progress**
   - Check logs and queue status

## 📚 Additional Resources

- [Azure Kubernetes Service Documentation](https://docs.microsoft.com/en-us/azure/aks/)
- [Azure Service Bus Documentation](https://docs.microsoft.com/en-us/azure/service-bus-messaging/)
- [Azure AI Search Documentation](https://docs.microsoft.com/en-us/azure/search/)