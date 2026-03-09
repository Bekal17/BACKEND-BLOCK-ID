"""
Billing service: customer lookup, usage tracking, plan limits.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from backend_blockid.database.pg_connection import get_conn, release_conn

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


async def get_customer_async(api_key: str) -> dict | None:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT api_key, stripe_customer_id, plan, created_at FROM customers WHERE api_key = $1",
            api_key,
        )
        if not row:
            return None
        return {
            "api_key": row["api_key"],
            "stripe_customer_id": row["stripe_customer_id"],
            "plan": row["plan"],
            "created_at": row["created_at"],
        }
    finally:
        await release_conn(conn)


def get_customer(api_key: str) -> dict | None:
    """Sync wrapper for get_customer_async."""
    return asyncio.get_event_loop().run_until_complete(get_customer_async(api_key))


async def get_or_create_usage_async(api_key: str) -> dict:
    conn = await get_conn()
    try:
        now = int(time.time())
        row = await conn.fetchrow(
            "SELECT wallet_checks, batch_checks, reports_generated, last_reset FROM api_usage WHERE api_key = $1",
            api_key,
        )
        if row:
            return {
                "wallet_checks": row["wallet_checks"],
                "batch_checks": row["batch_checks"],
                "reports_generated": row["reports_generated"],
                "last_reset": row["last_reset"],
            }
        await conn.execute(
            "INSERT INTO api_usage (api_key, wallet_checks, batch_checks, reports_generated, last_reset) VALUES ($1, 0, 0, 0, $2)",
            api_key, now,
        )
        return {"wallet_checks": 0, "batch_checks": 0, "reports_generated": 0, "last_reset": now}
    finally:
        await release_conn(conn)


def get_or_create_usage(api_key: str) -> dict:
    """Sync wrapper for get_or_create_usage_async."""
    return asyncio.get_event_loop().run_until_complete(get_or_create_usage_async(api_key))


async def increment_usage_async(api_key: str, wallet_checks: int = 0, batch_checks: int = 0, reports: int = 0) -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO api_usage (api_key, wallet_checks, batch_checks, reports_generated, last_reset)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(api_key) DO UPDATE SET
                wallet_checks = api_usage.wallet_checks + EXCLUDED.wallet_checks,
                batch_checks = api_usage.batch_checks + EXCLUDED.batch_checks,
                reports_generated = api_usage.reports_generated + EXCLUDED.reports_generated
            """,
            api_key, wallet_checks, batch_checks, reports, int(time.time()),
        )
    finally:
        await release_conn(conn)


def increment_usage(api_key: str, wallet_checks: int = 0, batch_checks: int = 0, reports: int = 0) -> None:
    """Sync wrapper for increment_usage_async."""
    asyncio.get_event_loop().run_until_complete(
        increment_usage_async(api_key, wallet_checks, batch_checks, reports)
    )


async def update_customer_plan_async(api_key: str | None, stripe_customer_id: str | None, plan: str) -> bool:
    conn = await get_conn()
    try:
        ok = False
        if api_key:
            result = await conn.execute("UPDATE customers SET plan = $1 WHERE api_key = $2", plan, api_key)
            if "UPDATE 1" in result:
                ok = True
        if stripe_customer_id:
            result = await conn.execute("UPDATE customers SET plan = $1 WHERE stripe_customer_id = $2", plan, stripe_customer_id)
            if "UPDATE 1" in result:
                ok = True
        return ok
    finally:
        await release_conn(conn)


def update_customer_plan(api_key: str | None, stripe_customer_id: str | None, plan: str) -> bool:
    """Sync wrapper for update_customer_plan_async."""
    return asyncio.get_event_loop().run_until_complete(
        update_customer_plan_async(api_key, stripe_customer_id, plan)
    )


async def upsert_customer_async(api_key: str, stripe_customer_id: str | None, plan: str) -> None:
    conn = await get_conn()
    try:
        now = int(time.time())
        await conn.execute(
            """
            INSERT INTO customers (api_key, stripe_customer_id, plan, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(api_key) DO UPDATE SET
                stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, customers.stripe_customer_id),
                plan = EXCLUDED.plan
            """,
            api_key, stripe_customer_id, plan, now,
        )
    finally:
        await release_conn(conn)


def upsert_customer(api_key: str, stripe_customer_id: str | None, plan: str) -> None:
    """Sync wrapper for upsert_customer_async."""
    asyncio.get_event_loop().run_until_complete(
        upsert_customer_async(api_key, stripe_customer_id, plan)
    )


def check_limit(plan: str, metric: str, current: int, batch_size: int = 1) -> tuple[bool, int]:
    limits = get_plan_limits(plan)
    if metric == "reports":
        limit_val = limits.get("reports_limit", 0)
    else:
        limit_val = limits.get("wallet_checks_limit", 10000)
    if limit_val < 0:
        return True, -1
    return (current + batch_size) <= limit_val, max(0, limit_val - current - batch_size)
