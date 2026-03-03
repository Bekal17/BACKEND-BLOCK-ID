# BlockID Grafana Alert → Rollback Webhook

Configure Grafana alerts to trigger rollback when:

- **Health check fails** — `probe_success{job="blockid"} == 0`
- **Pipeline failures spike** — `increase(blockid_pipeline_failures_total[1h]) > 0`
- **API latency > threshold** — `histogram_quantile(0.95, rate(blockid_http_request_duration_seconds_bucket[5m])) > 5`
- **Helius cost abnormal** — `blockid_helius_cost_total > 5`

## Setup

1. Create Grafana alert rule with one of the conditions above.
2. Add Contact Point → Webhook.
3. Webhook URL: `http://your-rollback-service/rollback` (see below).
4. Webhook payload can trigger a script that runs:
   ```bash
   kubectl rollout undo deployment/blockid-api -n blockid
   ```

## Rollback script (run on alert)

```bash
#!/bin/bash
# /opt/blockid/rollback.sh
kubectl rollout undo deployment/blockid-api -n blockid
echo "[rollback] triggered version=previous"
```

Or use the Python CLI:

```bash
python -m backend_blockid.tools.rollback_deployment
```

## Future upgrades

- Canary deployment
- Blue/green deploy
- Automatic DB migration rollback
- Risk score rollback
