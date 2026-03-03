# BlockID Load Testing with Locust

Stress test FastAPI endpoints before mainnet.

## Install

```bash
pip install locust
```

## Run

### Web UI (default)
```bash
locust -f backend_blockid/tools/locust_blockid.py
```
Open http://localhost:8089

### Headless
```bash
locust -f backend_blockid/tools/locust_blockid.py --headless -u 100 -r 10 -t 60s
```

### Custom host
```bash
locust -f backend_blockid/tools/locust_blockid.py --host http://your-server:8000
```

## Test Scenarios

| Task | Endpoint | Weight |
|------|----------|--------|
| wallet_profile | GET /wallet/{wallet} | 5 |
| badge | GET /wallet/{wallet}/investigation_badge | 2 |
| graph | GET /wallet/{wallet}/graph | 2 |
| report | GET /wallet/{wallet}/report | 1 |
| realtime_update | POST /realtime/update_wallet/{wallet} | 3 |
| pipeline_batch_update | POST /realtime/update_wallet (batch) | 1 |

Wallets loaded from `backend_blockid/data/test_wallets.csv`.

## Suggested Test

- Users: 50 → 500
- Spawn rate: 10/sec
- Duration: 5–10 min

## Metrics to Observe

- Response time (median, p95, p99)
- Error rate
- DB query time
- CPU / RAM usage
- Helius calls (if enabled)

## Report

Results saved to `backend_blockid/reports/load_test_results.csv` on test completion.

## Future Upgrades

- Distributed Locust workers
- Kubernetes load test
- Phantom plugin simulation
- Exchange API simulation
