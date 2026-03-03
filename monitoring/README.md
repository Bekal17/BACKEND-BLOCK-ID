# BlockID Prometheus & Grafana Monitoring

## Quick Start

### 1. Install Prometheus

**Windows (PowerShell):**
```powershell
# Download from https://prometheus.io/download/
# Or with chocolatey: choco install prometheus
```

**WSL/Linux:**
```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.47.0/prometheus-2.47.0.linux-amd64.tar.gz
tar xzf prometheus-2.47.0.linux-amd64.tar.gz
cd prometheus-2.47.0.linux-amd64
```

### 2. Run BlockID API

```bash
cd D:\BACKENDBLOCKID
py -m uvicorn backend_blockid.api_server.server:app --host 0.0.0.0 --port 8000
```

### 3. Run Prometheus

From project root:
```bash
prometheus --config.file=monitoring/prometheus.yml
```

Prometheus UI: http://localhost:9090

### 4. Install Grafana

**WSL/Docker:**
```bash
docker run -d -p 3000:3000 --name grafana grafana/grafana
```

**Windows:** Download from https://grafana.com/grafana/download

### 5. Add Prometheus Data Source

1. Open Grafana http://localhost:3000 (admin/admin)
2. Configuration → Data sources → Add Prometheus
3. URL: `http://localhost:9090` (or `http://host.docker.internal:9090` if Grafana in Docker)
4. Save & Test

### 6. Import Dashboard

Create panels or import the provided dashboard:

| Panel | Query |
|-------|-------|
| Pipeline runs | `blockid_pipeline_runs_total` |
| Pipeline failures | `blockid_pipeline_failures_total` |
| Helius cost | `blockid_helius_cost_total` |
| RPC latency | `blockid_rpc_latency_seconds` |
| RPC failures | `blockid_rpc_failures_total` |
| RPC switch count | `blockid_rpc_switch_total` |
| Wallets scanned | `blockid_wallets_scanned_total` |
| Review queue size | `blockid_review_queue_size` |
| API latency | `histogram_quantile(0.95, rate(blockid_http_request_duration_seconds_bucket[5m]))` |

### 7. Grafana Alerts

Create alert rules:

- **Pipeline failure**: `increase(blockid_pipeline_failures_total[1h]) > 0`
- **Helius cost spike**: `increase(blockid_helius_cost_total[1d]) > 5`
- **Review queue large**: `blockid_review_queue_size > 100`
- **API down**: `up{job="blockid"} == 0`

## Metrics Reference

| Metric | Type | Description |
|--------|------|-------------|
| `blockid_pipeline_runs_total` | Gauge | Total pipeline runs (from DB) |
| `blockid_pipeline_failures_total` | Gauge | Total pipeline failures |
| `blockid_helius_api_calls_total` | Gauge | Total Helius API calls |
| `blockid_helius_cost_total` | Gauge | Total Helius cost (USD) |
| `blockid_wallets_scanned_total` | Gauge | Total wallets scanned |
| `blockid_review_queue_size` | Gauge | Pending review queue count |
| `blockid_http_request_duration_seconds` | Histogram | API request latency |
| `blockid_rpc_latency_seconds` | Histogram | RPC request latency (per endpoint) |
| `blockid_rpc_failures_total` | Counter | RPC request failures |
| `blockid_rpc_switch_total` | Counter | RPC failover switch count |
