# BlockID Canary Grafana Dashboard

Compare stable vs canary metrics during canary deployment.

## Panels

| Panel | Query (stable) | Query (canary) |
|-------|----------------|----------------|
| Error rate | `rate(blockid_http_requests_total{version="stable"}[5m])` | `rate(blockid_http_requests_total{version="canary"}[5m])` |
| Latency p95 | `histogram_quantile(0.95, rate(blockid_http_request_duration_seconds_bucket{version="stable"}[5m]))` | Same with `version="canary"` |
| Pipeline failures | `blockid_pipeline_failures_total` (add version via relabel) |
| Helius cost | `blockid_helius_cost_total` |

## Version Label

Add `version` to metrics by configuring Prometheus to relabel pod labels:

```yaml
relabel_configs:
  - source_labels: [__meta_kubernetes_pod_label_version]
    target_label: version
```

Or set `VERSION` env in deployment and expose in /metrics.

## Promotion Rules

Monitor for X hours. Promote if:

- API error rate (canary) ≤ stable
- Latency p95 (canary) ≤ 1.2 × stable
- Pipeline success rate OK
- Helius cost within limit
- No spike in false positive rate

## Future upgrades

- Automatic statistical canary analysis
- Per-endpoint canary
- Multi-region canary
- A/B testing for risk engine
