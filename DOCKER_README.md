# BlockID — Docker Production Deployment

> Deploy the full BlockID backend stack locally or on any VPS using Docker Compose.
> Includes API server, pipeline, PostgreSQL, Prometheus metrics, and Grafana dashboards.

---

## Stack

| Service | Description | Port |
|---|---|---|
| `blockid-api` | FastAPI backend (Uvicorn) | 8000 |
| `blockid-pipeline` | Scoring pipeline (on-demand) | — |
| `blockid-db` | PostgreSQL (optional profile) | 5432 |
| `prometheus` | Metrics scraping | 9090 |
| `grafana` | Dashboards (admin/admin) | 3000 |

---

## Prerequisites

- Docker 24+
- Docker Compose v2+
- Helius API key ([helius.dev](https://helius.dev))
- Solana keypair (for oracle publishing)

---

## Quick Start

### 1. Clone the repo
```bash
git clone https://github.com/Bekal17/BACKEND-BLOCK-ID
cd BACKEND-BLOCK-ID
```

### 2. Configure environment
```bash
cp production.env.example production.env
```

Edit `production.env`:
```env
# Required
HELIUS_API_KEY=your_helius_api_key
DATABASE_URL=postgresql://blockid:blockid@blockid-db:5432/blockid
ORACLE_PRIVATE_KEY=your_solana_keypair_base58
ORACLE_PROGRAM_ID=your_anchor_program_id
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=your_key

# Optional
LOG_LEVEL=INFO
BILLING_ENABLED=0
API_HOST=0.0.0.0
API_PORT=8000
```

### 3. Build and run
```bash
docker compose build
docker compose up -d
```

### 4. Verify
```bash
curl http://localhost:8000/health
# {"status": "ok", "database_ok": true, ...}
```

---

## Profiles

### With PostgreSQL (recommended)
```bash
docker compose --profile postgres up -d
```

### Run pipeline on demand
```bash
docker compose --profile pipeline run --rm blockid-pipeline
```

### Full stack (API + DB + monitoring)
```bash
docker compose --profile postgres up -d
docker compose up -d prometheus grafana
```

---

## Grafana Setup

1. Open [http://localhost:3000](http://localhost:3000) → login: `admin` / `admin`
2. Add datasource → Prometheus → URL: `http://prometheus:9090`
3. Import dashboard or create panels for:
   - API request rate (`http_request_duration_seconds`)
   - Error rate by endpoint
   - DB connection pool usage

---

## Health Check

```bash
# API health
curl http://localhost:8000/health

# Docker container status
docker compose ps

# Live logs
docker compose logs -f blockid-api
```

Docker healthcheck runs automatically every 30 seconds via `GET /health`.

---

## Backup & Restore

### Backup PostgreSQL
```bash
docker compose exec blockid-db pg_dump -U blockid blockid > backup-$(date +%Y%m%d).sql
```

### Restore PostgreSQL
```bash
docker compose exec -T blockid-db psql -U blockid blockid < backup-20260310.sql
```

### Backup volumes
```bash
# Replace PROJECT with your compose project name (e.g. backendblockid)
docker run --rm \
  -v PROJECT_blockid_data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/blockid-data-$(date +%Y%m%d).tar.gz -C /data .
```

### Restore volumes
```bash
docker run --rm \
  -v PROJECT_blockid_data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/blockid-data-20260310.tar.gz -C /data
```

---

## Updating

```bash
git pull origin main
docker compose build
docker compose up -d
```

Zero-downtime update (if using multiple replicas):
```bash
docker compose up -d --no-deps --build blockid-api
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `connection refused` on port 8000 | Check `docker compose logs blockid-api` for startup errors |
| DB connection failed | Ensure `DATABASE_URL` matches the `blockid-db` service credentials |
| CORS errors | Add your frontend domain to `allow_origins` in `server.py` |
| 502 from reverse proxy | Ensure Uvicorn is binding to `0.0.0.0`, not `127.0.0.1` |
| Pipeline not running | Check `HELIUS_API_KEY` is set and valid |

---

## Production Tips

- Put Nginx or Caddy in front of port 8000 for SSL termination
- Set `BILLING_ENABLED=1` to enforce API quota limits
- Use Railway or Fly.io for managed hosting instead of self-hosting
- Set up UptimeRobot to monitor `GET /health` every 5 minutes

---

## Roadmap

- [ ] Kubernetes Helm charts
- [ ] GitHub Actions CI/CD pipeline
- [ ] Auto-scaling (HPA)
- [ ] Multi-region RPC fallback
- [ ] Redis caching layer

---

## License

Proprietary — © 2026 BlockID. All rights reserved.
