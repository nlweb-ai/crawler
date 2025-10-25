# Kubernetes Deployment for Crawler

This directory contains all the files needed to deploy the crawler application on Kubernetes with one master and multiple workers.

## Architecture

- **Master (1 instance)**: Runs the API server and scheduler
  - Exposes REST API on port 5001
  - Manages site scheduling and job creation
  - Provides web UI

- **Workers (scalable)**: Process crawling jobs from the queue
  - Auto-scales from 2 to 20 instances based on CPU/memory
  - Processes jobs from Azure Service Bus
  - Updates database and blob storage

## Prerequisites

1. **Kubernetes cluster** (AKS, EKS, GKE, or local k8s)
2. **Docker registry** (Azure Container Registry, Docker Hub, etc.)
3. **Azure resources**:
   - Azure Service Bus namespace and queue
   - Azure SQL Database
   - Azure Storage Account
   - Azure AD Service Principal (for authentication)
   - (Optional) Azure OpenAI and Cognitive Search

## Setup Instructions

### 1. Prepare Secrets

Copy the secrets template and fill in your values:

```bash
cp k8s/secrets-template.yaml k8s/secrets.yaml
```

Edit `k8s/secrets.yaml` and add your base64-encoded secrets:

```bash
# Encode a value
echo -n 'your-value' | base64
```

**Important**: Never commit `secrets.yaml` to version control!

### 2. Configure Application

Edit `k8s/configmap.yaml` to set your configuration:
- Queue names
- Database name
- Storage container names
- Optional AI service configurations

### 3. Build Docker Images

Build and push images to your registry:

```bash
# Build locally (for testing)
./k8s/build.sh

# Build and push to Azure Container Registry
./k8s/build.sh myregistry.azurecr.io

# Build and push to Docker Hub
./k8s/build.sh docker.io/myusername
```

### 4. Update Image References

Edit the deployment files to use your registry:

In `master-deployment.yaml` and `worker-deployment.yaml`:
```yaml
image: myregistry.azurecr.io/crawler-master:latest
image: myregistry.azurecr.io/crawler-worker:latest
```

### 5. Deploy to Kubernetes

```bash
# Deploy everything
./k8s/deploy.sh apply

# Check deployment status
./k8s/deploy.sh status

# View logs
./k8s/deploy.sh logs

# Scale workers manually
./k8s/deploy.sh scale 10
```

## Monitoring

### Check Pod Status
```bash
kubectl get pods -n crawler
```

### View Master Logs
```bash
kubectl logs -n crawler -l app=crawler-master -f
```

### View Worker Logs
```bash
kubectl logs -n crawler -l app=crawler-worker -f --prefix=true
```

### Get External URL
```bash
kubectl get service crawler-master-external -n crawler
```

## Scaling

Workers auto-scale based on CPU and memory usage:
- Minimum: 2 workers
- Maximum: 20 workers
- Scale up at 70% CPU or 80% memory

Manual scaling:
```bash
kubectl scale deployment crawler-worker -n crawler --replicas=10
```

## Cleanup

Remove all resources:
```bash
./k8s/deploy.sh delete
```

Delete namespace (complete cleanup):
```bash
kubectl delete namespace crawler
```

## Troubleshooting

### Pods not starting
Check pod events:
```bash
kubectl describe pod <pod-name> -n crawler
```

### Connection issues
Verify secrets are correctly encoded:
```bash
kubectl get secret crawler-secrets -n crawler -o yaml
```

### Database connectivity
Ensure Azure SQL firewall rules allow connections from your Kubernetes cluster.

### Service Bus issues
Verify Service Principal has correct permissions on the Service Bus namespace.

## Production Considerations

1. **High Availability**: Consider deploying master with multiple replicas behind a load balancer
2. **Persistent Storage**: Add PersistentVolumes for local data if needed
3. **Network Policies**: Implement network policies to restrict pod-to-pod communication
4. **Resource Limits**: Adjust CPU/memory limits based on actual usage
5. **Monitoring**: Add Prometheus/Grafana for metrics
6. **Logging**: Configure centralized logging (ELK, Azure Monitor, etc.)
7. **Secrets Management**: Use Azure Key Vault with CSI driver for better secret management
8. **Ingress**: Add Ingress controller for better HTTP routing and SSL termination