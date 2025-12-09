# Schema.org Crawler

A distributed web crawler designed to fetch and process schema.org structured data from websites at scale.

## 🚀 Quick Start

Before running `make bootstrap`:
* Install [Python 3.12](https://www.python.org/downloads/release/python-31212/)
* Install [Docker](https://www.docker.com/products/docker-desktop/)
* Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/)
* Install [Homebrew](https://brew.sh/)

Run `make bootstrap` to create a virtual environment at `.venv` with requirements installed.
* This will install `uv` if it is not installed.

### Deploy to Azure Kubernetes (Production)

#### Option 1: Complete Setup from Scratch
```bash
# This interactive script will:
# 1. Ask for resource group name and region
# 2. Create ALL Azure resources (AKS, Database, Queue, Storage, etc.)
# 3. Build and deploy everything
# 4. Give you the public URL (~15-20 minutes)
./azure/setup-and-deploy.sh
```

#### Option 2: Deploy to Existing Resources
If you already have Azure resources (database, queue, etc.):

```bash
# Step 1: Configure your environment
cp .env.example .env
# Edit .env with your Azure credentials

# Step 2: Create Kubernetes secrets
./azure/create-secrets-from-env.sh

# Step 3: Deploy to AKS
./azure/deploy-to-aks.sh
```

#### Access Your Deployment
After deployment completes:
```bash
# Get the public URL
kubectl get service crawler-master-external -n crawler

# Access the crawler
# Web UI: http://<EXTERNAL-IP>/
# API: http://<EXTERNAL-IP>/api/status
```

#### Create Stable URL (Optional)
```bash
# Create a static IP for stable URL
./azure/create-static-ip.sh
```

See [Azure Deployment Guide](azure/README.md) for detailed instructions.

### Local Development
```bash
# Start master service (API + Scheduler)
make master

# Start worker service (in another terminal)
make worker
```

## 📁 Project Structure

```
crawler/
├── azure/              # Azure deployment scripts
├── code/               # Source code
│   ├── core/          # Core crawler logic
│   └── tests/         # Unit tests
├── k8s/               # Kubernetes manifests
├── testing/           # Testing and monitoring scripts
├── data/              # Test data (git-ignored)
└── start_*.sh         # Production starter scripts
```

## 🏗️ Architecture

The crawler consists of:

- **Master Service**: REST API and job scheduler
- **Worker Service(s)**: Process crawling jobs from queue
- **Azure Service Bus**: Job queue
- **Azure SQL Database**: Metadata and state
- **Azure Blob Storage**: Raw data storage
- **Azure AI Search**: Vector database for embeddings

## 🔧 Configuration

Create a `.env` file with your Azure credentials:
```bash
cp .env.example .env
# Edit .env with your Azure resources
```

Required environment variables (common):
- `QUEUE_TYPE` - `file` (default), `servicebus`, or `storage`
- `QUEUE_DIR` - directory for file queue (if `QUEUE_TYPE=file`)
- `AZURE_SERVICEBUS_CONNECTION_STRING` or `AZURE_SERVICEBUS_NAMESPACE` and `AZURE_SERVICE_BUS_QUEUE_NAME` - Service Bus (if `QUEUE_TYPE=servicebus`)
- `AZURE_STORAGE_CONNECTION_STRING` - Azure Storage (if `QUEUE_TYPE=storage`)
- `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD` - SQL Server credentials (used by `pymssql`)
- `BLOB_STORAGE_ACCOUNT_NAME` - Storage account (optional)
- `AZURE_SEARCH_ENDPOINT`, `AZURE_SEARCH_KEY` - Azure Cognitive Search (optional)
 
Ports and health endpoints:
- Master API default port: `5001` (see `API_PORT` env var). Health/status endpoint: `/api/status`.
- Worker status server default port: `8080` (env `WORKER_STATUS_PORT`) with `/status` and `/health` endpoints.

## 📊 API Endpoints

- `GET /` - Web UI
- `GET /api/status` - System status
- `POST /api/sites` - Add site to crawl
- `GET /api/queue/status` - Queue statistics
- `POST /api/process/{site_url}` - Trigger manual processing

## 🧪 Testing

Run the complete test suite:
```bash
./testing/run_k8s_test.sh
```

See [Testing Guide](testing/README.md) for more testing options.

## 🚢 Kubernetes Management

### Common Commands
```bash
# View all resources
kubectl get all -n crawler

# Check pod status
kubectl get pods -n crawler

# View logs
kubectl logs -n crawler -l app=crawler-master -f    # Master logs
kubectl logs -n crawler -l app=crawler-worker -f    # Worker logs

# Scale workers
kubectl scale deployment crawler-worker -n crawler --replicas=10

# Restart deployments (after config changes)
kubectl rollout restart deployment/crawler-master -n crawler
kubectl rollout restart deployment/crawler-worker -n crawler

# Access pod shell for debugging
kubectl exec -it <pod-name> -n crawler -- /bin/bash

# Delete and redeploy
kubectl delete namespace crawler
./azure/deploy-to-aks.sh
```

### Cost Management
```bash
# Stop AKS cluster (saves ~$60-200/month)
az aks stop --name <cluster-name> --resource-group <rg>

# Start again when needed
az aks start --name <cluster-name> --resource-group <rg>
```

### Docker Compose (Development)
```bash
docker-compose up --build
```

## 📈 Monitoring

- View logs: `kubectl logs -n crawler -l app=crawler-master -f`
- Check status: `curl http://<ip>/api/status`
- Monitor queue: `python3 testing/monitoring/monitor_queue.py`

## 📝 Documentation

- [Azure Deployment Guide](azure/README.md)
- [Kubernetes Deployment](k8s/README.md)
- [Local Testing Guide](LOCAL_TESTING.md)
- [API Documentation](CRAWLER_DOCUMENTATION.md)
- [Testing Documentation](testing/README.md)

## 📄 License

[MIT License](LICENSE)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## 🆘 Support

For issues or questions:
- Check the [documentation](CRAWLER_DOCUMENTATION.md)
- Review [testing scripts](testing/README.md)
- Open an issue on GitHub