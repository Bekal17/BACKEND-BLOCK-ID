"""
Billing service: customer lookup, usage tracking, plan limits.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from backend_blockid.database.connection import get_connection

PRICING_PATH = Path(__file__).resolve().parent.parent / "config" / "pricing_config.json"

_pricing_cache: dict | None = None


def _load_pricing() -> dict:
    global _pricing_cache
    if _pricing_cache is not None:
        return _pricing_cache
    if PRICING_PATH.exists():
        with open(PRICING_PATH, encoding="utf-8") as f:
            _pricing_cache = json.load(f)
    else:
        _pricing_cache = {"plans": {}, "pay_as_you_go": {}}
    return _pricing_cache


def get_plan_limits(plan: str) -> dict:
    p = _load_pricing()
    plans = p.get("plans", {})
    return plans.get(plan, plans.get("starter", {
        "wallet_checks_limit": 10000,
        "batch_enabled": False,
        "webhook_enabled": False,
        "reports_limit": 0,
    }))


def get_customer(api_key: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT api_key, stripe_customer_id, plan, created_at FROM customers WHERE api_key = ?",
        (api_key,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "api_key": row["api_key"] if hasattr(row, "keys") else row[0],
        "stripe_customer_id": row["stripe_customer_id"] if hasattr(row, "keys") else row[1],
        "plan": row["plan"] if hasattr(row, "keys") else row[2],
        "created_at": row["created_at"] if hasattr(row, "keys") else row[3],
    }


def get_or_create_usage(api_key: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        "SELECT wallet_checks, batch_checks, reports_generated, last_reset FROM api_usage WHERE api_key = ?",
        (api_key,),
    )
    row = cur.fetchone()
    if row:
        conn.close()
        return {
            "wallet_checks": row["wallet_checks"] if hasattr(row, "keys") else row[0],
            "batch_checks": row["batch_checks"] if hasattr(row, "keys") else row[1],
            "reports_generated": row["reports_generated"] if hasattr(row, "keys") else row[2],
            "last_reset": row["last_reset"] if hasattr(row, "keys") else row[3],
        }
    cur.execute(
        "INSERT INTO api_usage (api_key, wallet_checks, batch_checks, reports_generated, last_reset) VALUES (?, 0, 0, 0, ?)",
        (api_key, now),
    )
    conn.commit()
    conn.close()
    return {"wallet_checks": 0, "batch_checks": 0, "reports_generated": 0, "last_reset": now}


def increment_usage(api_key: str, wallet_checks: int = 0, batch_checks: int = 0, reports: int = 0) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO api_usage (api_key, wallet_checks, batch_checks, reports_generated, last_reset)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(api_key) DO UPDATE SET
            wallet_checks = wallet_checks + excluded.wallet_checks,
            batch_checks = batch_checks + excluded.batch_checks,
            reports_generated = reports_generated + excluded.reports_generated
        """,
        (api_key, wallet_checks, batch_checks, reports, int(time.time())),
    )
    conn.commit()
    conn.close()


def update_customer_plan(api_key: str | None, stripe_customer_id: str | None, plan: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    if api_key:
        cur.execute("UPDATE customers SET plan = ? WHERE api_key = ?", (plan, api_key))
    if stripe_customer_id:
        cur.execute("UPDATE customers SET plan = ? WHERE stripe_customer_id = ?", (plan, stripe_customer_id))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok


def upsert_customer(api_key: str, stripe_customer_id: str | None, plan: str) -> None:
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        """
        INSERT INTO customers (api_key, stripe_customer_id, plan, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(api_key) DO UPDATE SET
            stripe_customer_id = COALESCE(excluded.stripe_customer_id, stripe_customer_id),
            plan = excluded.plan
        """,
        (api_key, stripe_customer_id, plan, now),
    )
    conn.commit()
    conn.close()


def check_limit(plan: str, metric: str, current: int, batch_size: int = 1) -> tuple[bool, int]:
    limits = get_plan_limits(plan)
    if metric == "reports":
        limit_val = limits.get("reports_limit", 0)
    else:
        limit_val = limits.get("wallet_checks_limit", 10000)
    if limit_val < 0:
        return True, -1
    return (current + batch_size) <= limit_val, max(0, limit_val - current - batch_size)
