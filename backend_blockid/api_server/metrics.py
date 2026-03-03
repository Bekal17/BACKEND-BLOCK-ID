"""
BlockID Prometheus metrics for Grafana dashboards.

Tracks pipeline health, API performance, and Helius cost.
DB-sourced metrics (pipeline_run_log, helius_usage, review_queue) are
updated on each /metrics scrape. Request latency is tracked in-process.

Future upgrades:
  - Kubernetes monitoring
  - Multi-node metrics
  - Per-wallet metrics
  - SLA tracking
"""
from __future__ import annotations

import time

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, generate_latest

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# In-process metrics (updated by app code)
# ---------------------------------------------------------------------------

http_request_duration_seconds = Histogram(
    "blockid_http_request_duration_seconds",
    "API request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# DB-sourced metrics (Gauges, updated on each /metrics scrape)
# ---------------------------------------------------------------------------

pipeline_runs_total = Gauge(
    "blockid_pipeline_runs_total",
    "Total pipeline runs (from DB)",
    registry=REGISTRY,
)
pipeline_failures_total = Gauge(
    "blockid_pipeline_failures_total",
    "Total pipeline failures (from DB)",
    registry=REGISTRY,
)
helius_api_calls_total = Gauge(
    "blockid_helius_api_calls_total",
    "Total Helius API calls (from DB)",
    registry=REGISTRY,
)
helius_cost_total = Gauge(
    "blockid_helius_cost_total",
    "Total Helius estimated cost in USD (from DB)",
    registry=REGISTRY,
)
wallets_scanned_total = Gauge(
    "blockid_wallets_scanned_total",
    "Total wallets scanned by pipeline (from DB)",
    registry=REGISTRY,
)
review_queue_size = Gauge(
    "blockid_review_queue_size",
    "Current review queue pending count",
    registry=REGISTRY,
)


def _update_db_metrics() -> None:
    """Query DB and update gauges. Call before generate_latest()."""
    try:
        from backend_blockid.database.connection import get_connection

        conn = get_connection()
        cur = conn.cursor()

        # pipeline_run_log
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_run_log'")
        if cur.fetchone():
            cur.execute("SELECT COUNT(*) FROM pipeline_run_log")
            pipeline_runs_total.set(cur.fetchone()[0] or 0)
            cur.execute("SELECT COUNT(*) FROM pipeline_run_log WHERE success = 0")
            pipeline_failures_total.set(cur.fetchone()[0] or 0)
            cur.execute("SELECT COALESCE(SUM(wallets_scanned), 0) FROM pipeline_run_log")
            wallets_scanned_total.set(cur.fetchone()[0] or 0)
        else:
            pipeline_runs_total.set(0)
            pipeline_failures_total.set(0)
            wallets_scanned_total.set(0)

        # helius_usage
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='helius_usage'")
        if cur.fetchone():
            cur.execute(
                "SELECT COALESCE(SUM(request_count), 0), COALESCE(SUM(estimated_cost), 0) FROM helius_usage"
            )
            row = cur.fetchone()
            helius_api_calls_total.set(int(row[0] or 0))
            helius_cost_total.set(float(row[1] or 0.0))
        else:
            helius_api_calls_total.set(0)
            helius_cost_total.set(0.0)

        # review_queue
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='review_queue'")
        if cur.fetchone():
            cur.execute("SELECT COUNT(*) FROM review_queue WHERE status = 'pending'")
            review_queue_size.set(cur.fetchone()[0] or 0)
        else:
            review_queue_size.set(0)

        conn.close()
    except Exception as e:
        logger.warning("metrics_db_update_error", error=str(e))


# structlog event rule: the first positional arg to logger.info() IS the event name.
# Do NOT pass event= as kwarg—that causes "got multiple values for argument 'event'" TypeError.


def record_pipeline_run(success: bool, wallets_scanned: int) -> None:
    """Log pipeline run for metrics (called from run_full_pipeline)."""
    logger.info("pipeline_run_recorded", module="metrics", success=success, wallets_scanned=wallets_scanned)


def generate_metrics() -> bytes:
    """Generate Prometheus exposition format. Updates DB gauges before export."""
    _update_db_metrics()
    return generate_latest(REGISTRY)
