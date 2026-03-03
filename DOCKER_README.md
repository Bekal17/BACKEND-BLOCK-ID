# BlockID Docker Production Deployment

Deploy BlockID backend, pipeline, and monitoring with Docker Compose.

## Quick Start

### 1. Create production.env

```bash
cp production.env.example production.env
# Edit production.env and set HELIUS_API_KEY and other secrets
```

### 2. Build and Run

```bash
docker compose build
docker compose up -d
```

### 3. URLs

| Service      | URL                    |
|-------------|------------------------|
| BlockID API | http://localhost:8000  |
| Prometheus  | http://localhost:9090  |
| Grafana     | http://localhost:3000  (admin/admin) |

## Services

- **blockid-api**: FastAPI server on port 8000
- **prometheus**: Metrics scraping
- **grafana**: Dashboards (add Prometheus datasource: `http://prometheus:9090`)
- **blockid-db** (optional): PostgreSQL — `docker compose --profile postgres up -d`

## Pipeline (optional)

Run the full pipeline on demand:

```bash
docker compose --profile pipeline run --rm blockid-pipeline
```

Or schedule via cron / Kubernetes CronJob.

## Health Check

- `GET /health` returns `{"status": "ok"}`
- Docker healthcheck runs every 30s

## Backup Strategy

### Volumes

- `blockid_data`: SQLite DB and data files
- `blockid_logs`: Application logs
- `grafana_data`: Grafana dashboards

### Backup commands

```bash
# Backup DB (Linux/macOS)
docker compose exec blockid-api tar czf - /app/data > blockid-backup-$(date +%Y%m%d).tar.gz

# Backup volumes (replace PROJECT with your compose project name, e.g. backendblockid)
docker run --rm -v PROJECT_blockid_data:/data -v $(pwd):/backup alpine tar czf /backup/blockid-data.tar.gz -C /data .
```

### Restore

```bash
# Restore data volume
docker run --rm -v PROJECT_blockid_data:/data -v $(pwd):/backup alpine tar xzf /backup/blockid-data.tar.gz -C /data
```

## Future Upgrades

- Kubernetes deployment (Helm charts)
- Auto-scaling (HPA)
- Multi-region RPC fallback
- CI/CD pipeline (GitHub Actions)
