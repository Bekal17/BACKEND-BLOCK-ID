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
from backend_blockid.database.connection import get_connection
from backend_blockid.tools.helius_cost_monitor import get_today_stats, get_top_wallets_today, DAILY_LIMIT
from backend_blockid.tools.review_queue_engine import list_pending

logger = get_logger(__name__)

router = APIRouter(prefix="/monitor", tags=["Monitoring"])
REVIEW_QUEUE_ALERT_THRESHOLD = int(os.getenv("REVIEW_QUEUE_ALERT_THRESHOLD", "100"))


def _today_start_ts() -> int:
    now = time.gmtime(time.time())
    return int(time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0)))


def _check_db() -> bool:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


def _blockid_logs_exists() -> bool:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blockid_logs'")
        exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception:
        return False


def _table_has_column(table: str, column: str) -> bool:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = {row[1] for row in cur.fetchall()}
        conn.close()
        return column in cols
    except Exception:
        return False


def _wallets_scored_today() -> int:
    try:
        cutoff = _today_start_ts()
        conn = get_connection()
        cur = conn.cursor()
        if _table_has_column("trust_scores", "last_updated"):
            cur.execute("SELECT COUNT(*) FROM trust_scores WHERE last_updated >= ?", (cutoff,))
        else:
            cur.execute("SELECT COUNT(*) FROM trust_scores WHERE score IS NOT NULL")
        count = cur.fetchone()[0] or 0
        conn.close()
        return count
    except Exception:
        return 0


def _pda_success_rate_24h() -> dict:
    if not _blockid_logs_exists():
        return {"ok": 0, "error": 0, "success_rate": None}
    cutoff = int(time.time()) - 86400
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT status, COUNT(*) FROM blockid_logs
        WHERE stage = 'pda_publish' AND timestamp >= ?
        GROUP BY status
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    counts = {status: cnt for status, cnt in rows}
    ok = int(counts.get("ok", 0))
    err = int(counts.get("error", 0))
    total = ok + err
    rate = round(ok / total, 4) if total > 0 else None
    return {"ok": ok, "error": err, "success_rate": rate}


def _avg_latency_24h(stage: str) -> int | None:
    if not _blockid_logs_exists():
        return None
    cutoff = int(time.time()) - 86400
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT AVG(latency_ms) FROM blockid_logs
        WHERE stage = ? AND latency_ms IS NOT NULL AND timestamp >= ?
        """,
        (stage, cutoff),
    )
    row = cur.fetchone()
    conn.close()
    if row and row[0] is not None:
        return int(row[0])
    return None


def record_pipeline_run(
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
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO pipeline_run_log
               (run_start_ts, run_end_ts, success, wallets_scanned, errors_count, steps_completed, message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_start_ts, run_end_ts, 1 if success else 0, wallets_scanned, errors_count, steps_completed, message),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("monitor_record_pipeline_run_failed", error=str(e))


def _get_last_pipeline_run() -> dict | None:
    """Return last pipeline run from pipeline_run_log."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_run_log'")
        if not cur.fetchone():
            conn.close()
            return None
        cur.execute(
            """SELECT run_start_ts, run_end_ts, success, wallets_scanned, errors_count, steps_completed
               FROM pipeline_run_log ORDER BY run_start_ts DESC LIMIT 1"""
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "run_start_ts": row[0],
            "run_end_ts": row[1],
            "success": bool(row[2]),
            "wallets_scanned": row[3] or 0,
            "errors_count": row[4] or 0,
            "steps_completed": row[5] or 0,
        }
    except Exception:
        return None


@router.get("/health")
def get_health() -> dict:
    """
    System health: database_connection_ok, last_pipeline_run, api_status.
    """
    db_ok = _check_db()
    last_run = _get_last_pipeline_run()
    return {
        "database_connection_ok": db_ok,
        "last_pipeline_run": last_run,
        "api_status": "ok",
    }


@router.get("/pipeline_status")
def get_pipeline_status() -> dict:
    """
    Pipeline status: last_run_time, wallets_scanned, errors_count.
    """
    last = _get_last_pipeline_run()
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
def get_monitor_status() -> dict:
    """
    Monitoring status summary.
    """
    last = _get_last_pipeline_run()
    error_count = 0
    if _blockid_logs_exists():
        cutoff = int(time.time()) - 86400
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM blockid_logs WHERE status='error' AND timestamp >= ?",
            (cutoff,),
        )
        error_count = cur.fetchone()[0] or 0
        conn.close()
    pda = _pda_success_rate_24h()
    avg_rpc = _avg_latency_24h("helius_fetch")
    return {
        "pipeline_ok": last["success"] if last else None,
        "errors_last_24h": error_count,
        "avg_rpc_latency_ms": avg_rpc,
        "pda_success_rate": pda["success_rate"],
    }


@router.get("/errors")
def get_monitor_errors() -> dict:
    """
    Recent error logs from blockid_logs (last 24h, max 200).
    """
    if not _blockid_logs_exists():
        return {"errors": [], "count": 0}
    cutoff = int(time.time()) - 86400
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, stage, status, message, wallet
        FROM blockid_logs
        WHERE status = 'error' AND timestamp >= ?
        ORDER BY timestamp DESC
        LIMIT 200
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    items = [
        {"timestamp": ts, "stage": stage, "status": status, "message": msg, "wallet": wallet}
        for (ts, stage, status, msg, wallet) in rows
    ]
    return {"errors": items, "count": len(items)}


@router.get("/latency")
def get_monitor_latency() -> dict:
    """
    Average latency by stage (last 24h).
    """
    if not _blockid_logs_exists():
        return {"avg_latency_ms": {}}
    cutoff = int(time.time()) - 86400
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT stage, AVG(latency_ms)
        FROM blockid_logs
        WHERE latency_ms IS NOT NULL AND timestamp >= ?
        GROUP BY stage
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    return {"avg_latency_ms": {stage: int(avg or 0) for stage, avg in rows}}


@router.get("/pipeline-summary")
def get_monitor_pipeline_summary() -> dict:
    """
    Pipeline summary for dashboard.
    """
    last = _get_last_pipeline_run()
    pda = _pda_success_rate_24h()
    avg_rpc = _avg_latency_24h("helius_fetch")
    return {
        "pipeline_ok": last["success"] if last else None,
        "wallets_scored_today": _wallets_scored_today(),
        "avg_rpc_latency_ms": avg_rpc,
        "pda_failures_24h": pda["error"],
        "pda_success_rate": pda["success_rate"],
        "errors_last_24h": get_monitor_errors().get("count", 0),
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
def get_trust_stats() -> dict:
    """
    Trust score stats: average_score, number_high_risk_wallets, new_scam_wallets_today.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cutoff = _today_start_ts()

        # Average score
        cur.execute("SELECT AVG(score) FROM trust_scores WHERE score IS NOT NULL")
        row = cur.fetchone()
        avg = float(row[0] or 0)

        # High risk (score < 40)
        cur.execute("SELECT COUNT(*) FROM trust_scores WHERE score < 40 AND score IS NOT NULL")
        high_risk = cur.fetchone()[0] or 0

        # New scam wallets today (scam_wallets.detected_at)
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_wallets'")
        if cur.fetchone():
            cur.execute(
                "SELECT COUNT(*) FROM scam_wallets WHERE detected_at >= ?",
                (cutoff,),
            )
            new_scam = cur.fetchone()[0] or 0
        else:
            new_scam = 0

        total_scored = 0
        cur.execute("SELECT COUNT(*) FROM trust_scores WHERE score IS NOT NULL")
        total_scored = cur.fetchone()[0] or 0

        conn.close()
        return {
            "average_score": round(avg, 2),
            "total_wallets_scored": total_scored,
            "number_high_risk_wallets": high_risk,
            "new_scam_wallets_today": new_scam,
        }
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


def _get_alerts() -> list[dict]:
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

        last = _get_last_pipeline_run()
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
def get_alerts() -> dict:
    """Active alerts (helius_cost > limit, pipeline_failed, review_queue > 100)."""
    return {"alerts": _get_alerts(), "count": len(_get_alerts())}


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
