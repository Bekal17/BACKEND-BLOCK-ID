"""
BlockID Monitoring Dashboard API.

Tracks system health, pipeline status, Helius cost, and trust score metrics.
Real-time monitoring before mainnet deployment.

Future upgrades:
  - Grafana integration
  - Prometheus metrics
  - Email/Telegram alerts
  - Historical charts

Endpoints:
  GET /monitor/health
  GET /monitor/pipeline_status
  GET /monitor/helius_usage
  GET /monitor/trust_stats
  GET /monitor/review_queue
  GET /monitor/dashboard — HTML UI
  GET /monitor/alerts — active alerts
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.tools.helius_cost_monitor import get_today_stats, get_top_wallets_today, DAILY_LIMIT
from backend_blockid.tools.review_queue_engine import list_pending

logger = get_logger(__name__)

router = APIRouter(prefix="/monitor", tags=["Monitoring"])
REVIEW_QUEUE_ALERT_THRESHOLD = int(os.getenv("REVIEW_QUEUE_ALERT_THRESHOLD", "100"))


def _today_start_ts() -> int:
    now = time.gmtime(time.time())
    return int(time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0)))


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _get_table_columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name=$1",
        table,
    )
    return {r["column_name"] for r in rows}


async def _check_db() -> bool:
    try:
        conn = await get_conn()
        try:
            await conn.fetchrow("SELECT 1")
            return True
        finally:
            await release_conn(conn)
    except Exception:
        return False


async def _blockid_logs_exists() -> bool:
    try:
        conn = await get_conn()
        try:
            return await _table_exists(conn, "blockid_logs")
        finally:
            await release_conn(conn)
    except Exception:
        return False


async def _table_has_column(table: str, column: str) -> bool:
    try:
        conn = await get_conn()
        try:
            cols = await _get_table_columns(conn, table)
            return column in cols
        finally:
            await release_conn(conn)
    except Exception:
        return False


async def _wallets_scored_today() -> int:
    try:
        cutoff = _today_start_ts()
        conn = await get_conn()
        try:
            if await _table_has_column("trust_scores", "last_updated"):
                row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM trust_scores WHERE last_updated >= $1", cutoff)
            else:
                row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM trust_scores WHERE score IS NOT NULL")
            return row["cnt"] or 0
        finally:
            await release_conn(conn)
    except Exception:
        return 0


async def _pda_success_rate_24h() -> dict:
    if not await _blockid_logs_exists():
        return {"ok": 0, "error": 0, "success_rate": None}
    cutoff = int(time.time()) - 86400
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT status, COUNT(*) as cnt FROM blockid_logs
            WHERE stage = 'pda_publish' AND timestamp >= $1
            GROUP BY status
            """,
            cutoff,
        )
        counts = {row["status"]: row["cnt"] for row in rows}
        ok = int(counts.get("ok", 0))
        err = int(counts.get("error", 0))
        total = ok + err
        rate = round(ok / total, 4) if total > 0 else None
        return {"ok": ok, "error": err, "success_rate": rate}
    finally:
        await release_conn(conn)


async def _avg_latency_24h(stage: str) -> int | None:
    if not await _blockid_logs_exists():
        return None
    cutoff = int(time.time()) - 86400
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT AVG(latency_ms) as avg_latency FROM blockid_logs
            WHERE stage = $1 AND latency_ms IS NOT NULL AND timestamp >= $2
            """,
            stage, cutoff,
        )
        if row and row["avg_latency"] is not None:
            return int(row["avg_latency"])
        return None
    finally:
        await release_conn(conn)


async def record_pipeline_run_async(
    run_start_ts: int,
    run_end_ts: int | None,
    success: bool,
    wallets_scanned: int = 0,
    errors_count: int = 0,
    steps_completed: int = 0,
    message: str | None = None,
) -> None:
    """Record a pipeline run for monitoring (call from run_full_pipeline)."""
    try:
        conn = await get_conn()
        try:
            await conn.execute(
                """INSERT INTO pipeline_run_log
                   (run_start_ts, run_end_ts, success, wallets_scanned, errors_count, steps_completed, message)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                run_start_ts, run_end_ts, 1 if success else 0, wallets_scanned, errors_count, steps_completed, message,
            )
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.warning("monitor_record_pipeline_run_failed", error=str(e))


def record_pipeline_run(
    run_start_ts: int,
    run_end_ts: int | None,
    success: bool,
    wallets_scanned: int = 0,
    errors_count: int = 0,
    steps_completed: int = 0,
    message: str | None = None,
) -> None:
    """Sync wrapper for record_pipeline_run_async."""
    import asyncio
    asyncio.get_event_loop().run_until_complete(
        record_pipeline_run_async(run_start_ts, run_end_ts, success, wallets_scanned, errors_count, steps_completed, message)
    )


async def _get_last_pipeline_run() -> dict | None:
    """Return last pipeline run from pipeline_run_log."""
    try:
        conn = await get_conn()
        try:
            if not await _table_exists(conn, "pipeline_run_log"):
                return None
            row = await conn.fetchrow(
                """SELECT run_start_ts, run_end_ts, success, wallets_scanned, errors_count, steps_completed
                   FROM pipeline_run_log ORDER BY run_start_ts DESC LIMIT 1"""
            )
            if not row:
                return None
            return {
                "run_start_ts": row["run_start_ts"],
                "run_end_ts": row["run_end_ts"],
                "success": bool(row["success"]),
                "wallets_scanned": row["wallets_scanned"] or 0,
                "errors_count": row["errors_count"] or 0,
                "steps_completed": row["steps_completed"] or 0,
            }
        finally:
            await release_conn(conn)
    except Exception:
        return None


@router.get("/health")
async def get_health() -> dict:
    """
    System health: database_connection_ok, last_pipeline_run, api_status.
    """
    db_ok = await _check_db()
    last_run = await _get_last_pipeline_run()
    return {
        "database_connection_ok": db_ok,
        "last_pipeline_run": last_run,
        "api_status": "ok",
    }


@router.get("/pipeline_status")
async def get_pipeline_status() -> dict:
    """
    Pipeline status: last_run_time, wallets_scanned, errors_count.
    """
    last = await _get_last_pipeline_run()
    if not last:
        return {
            "last_run_time": None,
            "wallets_scanned": 0,
            "errors_count": 0,
            "success": None,
            "message": "No pipeline run recorded yet",
        }
    return {
        "last_run_time": last["run_end_ts"] or last["run_start_ts"],
        "wallets_scanned": last["wallets_scanned"],
        "errors_count": last["errors_count"],
        "success": last["success"],
        "steps_completed": last["steps_completed"],
    }


@router.get("/status")
async def get_monitor_status() -> dict:
    """
    Monitoring status summary.
    """
    last = await _get_last_pipeline_run()
    error_count = 0
    if await _blockid_logs_exists():
        cutoff = int(time.time()) - 86400
        conn = await get_conn()
        try:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as cnt FROM blockid_logs WHERE status='error' AND timestamp >= $1",
                cutoff,
            )
            error_count = row["cnt"] or 0
        finally:
            await release_conn(conn)
    pda = await _pda_success_rate_24h()
    avg_rpc = await _avg_latency_24h("helius_fetch")
    return {
        "pipeline_ok": last["success"] if last else None,
        "errors_last_24h": error_count,
        "avg_rpc_latency_ms": avg_rpc,
        "pda_success_rate": pda["success_rate"],
    }


@router.get("/errors")
async def get_monitor_errors() -> dict:
    """
    Recent error logs from blockid_logs (last 24h, max 200).
    """
    if not await _blockid_logs_exists():
        return {"errors": [], "count": 0}
    cutoff = int(time.time()) - 86400
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT timestamp, stage, status, message, wallet
            FROM blockid_logs
            WHERE status = 'error' AND timestamp >= $1
            ORDER BY timestamp DESC
            LIMIT 200
            """,
            cutoff,
        )
        items = [
            {"timestamp": r["timestamp"], "stage": r["stage"], "status": r["status"], "message": r["message"], "wallet": r["wallet"]}
            for r in rows
        ]
        return {"errors": items, "count": len(items)}
    finally:
        await release_conn(conn)


@router.get("/latency")
async def get_monitor_latency() -> dict:
    """
    Average latency by stage (last 24h).
    """
    if not await _blockid_logs_exists():
        return {"avg_latency_ms": {}}
    cutoff = int(time.time()) - 86400
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT stage, AVG(latency_ms) as avg_latency
            FROM blockid_logs
            WHERE latency_ms IS NOT NULL AND timestamp >= $1
            GROUP BY stage
            """,
            cutoff,
        )
        return {"avg_latency_ms": {r["stage"]: int(r["avg_latency"] or 0) for r in rows}}
    finally:
        await release_conn(conn)


@router.get("/pipeline-summary")
async def get_monitor_pipeline_summary() -> dict:
    """
    Pipeline summary for dashboard.
    """
    last = await _get_last_pipeline_run()
    pda = await _pda_success_rate_24h()
    avg_rpc = await _avg_latency_24h("helius_fetch")
    errors = await get_monitor_errors()
    return {
        "pipeline_ok": last["success"] if last else None,
        "wallets_scored_today": await _wallets_scored_today(),
        "avg_rpc_latency_ms": avg_rpc,
        "pda_failures_24h": pda["error"],
        "pda_success_rate": pda["success_rate"],
        "errors_last_24h": errors.get("count", 0),
    }


@router.get("/helius_usage")
def get_helius_usage() -> dict:
    """
    Helius usage today: today_calls, estimated_cost, top_expensive_wallets.
    """
    try:
        today_calls, estimated_cost = get_today_stats()
        top = get_top_wallets_today(10)
        return {
            "today_calls": today_calls,
            "estimated_cost_usd": round(estimated_cost, 6),
            "daily_limit_usd": DAILY_LIMIT,
            "over_budget": estimated_cost > DAILY_LIMIT,
            "top_expensive_wallets": [
                {"wallet": w[:20] + "..." if len(w) > 20 else w, "calls": c, "cost_usd": round(cost, 6)}
                for w, c, cost in top
            ],
        }
    except Exception as e:
        logger.warning("monitor_helius_usage_error", error=str(e))
        return {
            "today_calls": 0,
            "estimated_cost_usd": 0.0,
            "daily_limit_usd": DAILY_LIMIT,
            "over_budget": False,
            "top_expensive_wallets": [],
            "error": str(e),
        }


@router.get("/trust_stats")
async def get_trust_stats() -> dict:
    """
    Trust score stats: average_score, number_high_risk_wallets, new_scam_wallets_today.
    """
    try:
        conn = await get_conn()
        try:
            cutoff = _today_start_ts()

            row = await conn.fetchrow("SELECT AVG(score) as avg_score FROM trust_scores WHERE score IS NOT NULL")
            avg = float(row["avg_score"] or 0)

            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM trust_scores WHERE score < 40 AND score IS NOT NULL")
            high_risk = row["cnt"] or 0

            new_scam = 0
            if await _table_exists(conn, "scam_wallets"):
                row = await conn.fetchrow(
                    "SELECT COUNT(*) as cnt FROM scam_wallets WHERE detected_at >= $1",
                    cutoff,
                )
                new_scam = row["cnt"] or 0

            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM trust_scores WHERE score IS NOT NULL")
            total_scored = row["cnt"] or 0

            return {
                "average_score": round(avg, 2),
                "total_wallets_scored": total_scored,
                "number_high_risk_wallets": high_risk,
                "new_scam_wallets_today": new_scam,
            }
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.warning("monitor_trust_stats_error", error=str(e))
        return {
            "average_score": 0.0,
            "total_wallets_scored": 0,
            "number_high_risk_wallets": 0,
            "new_scam_wallets_today": 0,
            "error": str(e),
        }


@router.get("/review_queue")
def get_review_queue() -> dict:
    """
    Review queue: pending wallets count.
    """
    try:
        items = list_pending()
        count = len(items)
        return {
            "pending_count": count,
            "over_threshold": count > REVIEW_QUEUE_ALERT_THRESHOLD,
            "threshold": REVIEW_QUEUE_ALERT_THRESHOLD,
        }
    except Exception as e:
        logger.warning("monitor_review_queue_error", error=str(e))
        return {
            "pending_count": 0,
            "over_threshold": False,
            "threshold": REVIEW_QUEUE_ALERT_THRESHOLD,
            "error": str(e),
        }


async def _get_alerts() -> list[dict]:
    """Return active alerts based on thresholds."""
    alerts: list[dict] = []
    try:
        _, cost = get_today_stats()
        if cost > DAILY_LIMIT:
            alerts.append({
                "type": "helius_cost",
                "message": f"Helius cost ${cost:.2f} exceeds limit ${DAILY_LIMIT}",
                "severity": "high",
            })

        last = await _get_last_pipeline_run()
        if last and not last["success"]:
            alerts.append({
                "type": "pipeline_failed",
                "message": "Last pipeline run failed",
                "severity": "high",
            })

        items = list_pending()
        if len(items) > REVIEW_QUEUE_ALERT_THRESHOLD:
            alerts.append({
                "type": "review_queue",
                "message": f"Review queue ({len(items)}) exceeds threshold ({REVIEW_QUEUE_ALERT_THRESHOLD})",
                "severity": "medium",
            })
    except Exception:
        pass
    return alerts


@router.get("/alerts")
async def get_alerts() -> dict:
    """Active alerts (helius_cost > limit, pipeline_failed, review_queue > 100)."""
    alerts = await _get_alerts()
    return {"alerts": alerts, "count": len(alerts)}


def _load_dashboard_html() -> str:
    """Load monitor.html template."""
    path = Path(__file__).resolve().parent / "templates" / "monitor.html"
    if path.exists():
        return path.read_text(encoding="utf-8")
    return _default_dashboard_html()


def _default_dashboard_html() -> str:
    """Fallback inline HTML if template missing."""
    return """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>BlockID Monitor</title>
<style>body{font-family:system-ui;max-width:900px;margin:2rem auto;padding:1rem;background:#0f0f12;color:#e4e4e7;}
.card{background:#18181b;border-radius:8px;padding:1rem;margin:1rem 0;border:1px solid #27272a;}
h1{color:#fafafa;} .ok{color:#22c55e;} .warn{color:#eab308;} .err{color:#ef4444;}
#refresh{color:#71717a;font-size:0.9rem;}</style></head>
<body>
<h1>BlockID Monitoring Dashboard</h1>
<p id="refresh">Loading... (auto-refresh 60s)</p>
<div id="content"></div>
<script>
fetch('/monitor/health').then(r=>r.json()).then(d=>{
  document.getElementById('content').innerHTML='<div class="card">Health: DB='+(d.database_connection_ok?'<span class="ok">OK</span>':'<span class="err">FAIL</span>')+' | API=OK</div>';
});
setInterval(()=>location.reload(),60000);
</script></body></html>"""


@router.get("/dashboard", response_class=HTMLResponse)
def get_dashboard() -> HTMLResponse:
    """
    Simple HTML dashboard. Auto-refresh every 60 seconds.
    """
    logger.info("monitor_dashboard_viewed")
    try:
        html = _load_dashboard_html()
    except Exception:
        html = _default_dashboard_html()
    return HTMLResponse(html)
