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

import asyncio

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, generate_latest

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

http_request_duration_seconds = Histogram(
    "blockid_http_request_duration_seconds",
    "API request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=REGISTRY,
)

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


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _update_db_metrics_async() -> None:
    """Query DB and update gauges. Call before generate_latest()."""
    try:
        from backend_blockid.database.pg_connection import get_conn, release_conn

        conn = await get_conn()
        try:
            if await _table_exists(conn, "pipeline_run_log"):
                row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM pipeline_run_log")
                pipeline_runs_total.set(row["cnt"] or 0)
                row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM pipeline_run_log WHERE success = 0")
                pipeline_failures_total.set(row["cnt"] or 0)
                row = await conn.fetchrow("SELECT COALESCE(SUM(wallets_scanned), 0) as total FROM pipeline_run_log")
                wallets_scanned_total.set(row["total"] or 0)
            else:
                pipeline_runs_total.set(0)
                pipeline_failures_total.set(0)
                wallets_scanned_total.set(0)

            if await _table_exists(conn, "helius_usage"):
                row = await conn.fetchrow(
                    "SELECT COALESCE(SUM(request_count), 0) as calls, COALESCE(SUM(estimated_cost), 0) as cost FROM helius_usage"
                )
                helius_api_calls_total.set(int(row["calls"] or 0))
                helius_cost_total.set(float(row["cost"] or 0.0))
            else:
                helius_api_calls_total.set(0)
                helius_cost_total.set(0.0)

            if await _table_exists(conn, "review_queue"):
                row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM review_queue WHERE status = 'pending'")
                review_queue_size.set(row["cnt"] or 0)
            else:
                review_queue_size.set(0)
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.warning("metrics_db_update_error", error=str(e))


def _update_db_metrics() -> None:
    """Sync wrapper for _update_db_metrics_async."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_update_db_metrics_async())
        else:
            loop.run_until_complete(_update_db_metrics_async())
    except Exception:
        pass


def record_pipeline_run(success: bool, wallets_scanned: int) -> None:
    """Log pipeline run for metrics (called from run_full_pipeline)."""
    logger.info("pipeline_run_recorded", module="metrics", success=success, wallets_scanned=wallets_scanned)


def generate_metrics() -> bytes:
    """Generate Prometheus exposition format. Updates DB gauges before export."""
    _update_db_metrics()
    return generate_latest(REGISTRY)
