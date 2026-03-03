# BlockID Kubernetes Deployment

Deploy BlockID backend and pipeline on Kubernetes for scalable production.

## Prerequisites

- kubectl configured
- Docker image `blockid:latest` (or set image in manifests)
- Kubernetes cluster (GKE, EKS, AKS, or kind/minikube)

## Quick Start

### 1. Create namespace and config

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
```

### 2. Create secrets

```bash
cp k8s/secret.yaml.example k8s/secret.yaml
# Edit secret.yaml with real values
kubectl apply -f k8s/secret.yaml
```

### 3. Deploy

```bash
kubectl apply -f k8s/
```

Or apply individually:

```bash
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/pipeline-cronjob.yaml
```

## Image

By default manifests use `blockid:latest`. For a registry image:

```bash
# Update image in backend-deployment.yaml and pipeline-cronjob.yaml
# image: docker.io/youruser/blockid:latest
```

## PostgreSQL

BlockID uses SQLite by default. For production at scale, use PostgreSQL:

- **Managed**: GCP Cloud SQL, AWS RDS, Azure Database
- **Helm**: `helm install postgres bitnami/postgresql -n blockid`

Set `DATABASE_URL` in ConfigMap/Secret for Postgres connection.

## Monitoring

Deploy Prometheus and Grafana via Helm:

```bash
# Add Helm repos
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts

# Prometheus
helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring --create-namespace

# Grafana
helm install grafana grafana/grafana -n monitoring
```

Configure Prometheus to scrape `blockid-api.blockid.svc:80/metrics`.

## Apply order

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/secret.yaml   # after creating from example
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/pipeline-cronjob.yaml
```

Or: `kubectl apply -f k8s/` (skip secret.yaml if not created)

## Canary Deployment

Test new versions on 10% traffic before full rollout:

### Manifests

- `backend-deployments.yaml` — stable (9 replicas) + canary (1 replica)
- `service-canary.yaml` — service routes by replica count (~90% stable, ~10% canary)

### Deploy canary

```bash
# Manual via kubectl
kubectl set image deployment/blockid-api-canary api=blockid:v1.5 -n blockid
kubectl scale deployment/blockid-api-canary --replicas=1 -n blockid
```

### GitHub Actions

1. **Manual**: Run workflow "BlockID Canary" with `image_tag` (e.g. `main-abc1234`)
2. **Automatic**: Include `[canary]` in commit message on push to main — deploys to canary instead of full deploy

### Promote / Rollback

```bash
# Promote canary → stable
kubectl set image deployment/blockid-api-stable api=blockid:v1.5 -n blockid
kubectl scale deployment/blockid-api-stable --replicas=9 -n blockid
kubectl scale deployment/blockid-api-canary --replicas=0 -n blockid

# Rollback canary
kubectl scale deployment/blockid-api-canary --replicas=0 -n blockid
```

See `monitoring/CANARY_DASHBOARD.md` for Grafana panels (error rate, latency, pipeline failures, Helius cost).

## Future upgrades

- Horizontal Pod Autoscaler (HPA)
- Multi-region RPC fallback
- Blue/Green deploy
- Per-endpoint canary, multi-region canary, A/B risk engine
