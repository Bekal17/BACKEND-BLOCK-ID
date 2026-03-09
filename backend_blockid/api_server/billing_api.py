"""
BlockID Billing API — Paddle billing integration.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Header, Request

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter(tags=["billing"])

PLANS = {
    "free": {"quota": 100, "price": 0, "paddle_price_id": None},
    "pro": {"quota": 50000, "price": 29, "paddle_price_id": "pri_XXXXX"},
    "enterprise": {"quota": 999999, "price": 199, "paddle_price_id": "pri_XXXXX"},
}


def _get_user_id(x_user_id: str | None) -> str:
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id


@router.get("/subscription")
async def get_subscription(
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """
    Get user's current subscription.
    Returns default free plan (quota: 1000) if no row found.
    """
    user_id = _get_user_id(x_user_id)

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT plan, status, quota, paddle_subscription_id,
                   current_period_end, cancel_at_period_end
            FROM subscriptions
            WHERE user_id = $1
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            user_id,
        )
        if not row:
            return {
                "plan": "free",
                "status": "active",
                "quota": 1000,
                "paddle_subscription_id": None,
                "current_period_end": None,
                "cancel_at_period_end": False,
            }
        return {
            "plan": row["plan"] or "free",
            "status": row["status"] or "active",
            "quota": int(row["quota"] or 1000),
            "paddle_subscription_id": row["paddle_subscription_id"],
            "current_period_end": row["current_period_end"].isoformat() if row["current_period_end"] else None,
            "cancel_at_period_end": bool(row["cancel_at_period_end"]),
        }
    finally:
        await release_conn(conn)


@router.get("/plans")
async def get_plans(
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Return list of all plans: free, pro, enterprise."""
    _get_user_id(x_user_id)
    return [
        {"name": "free", "price": 0, "quota": 1000},
        {"name": "pro", "price": 29, "quota": 50000},
        {"name": "enterprise", "price": 199, "quota": 999999},
    ]


@router.post("/webhook/paddle")
async def paddle_webhook(request: Request):
    """
    Handle Paddle webhook events. No auth required.
    Events: subscription.created, subscription.cancelled, subscription.updated
    """
    body = await request.json()
    event_type = body.get("event_type") or ""
    data = body.get("data") or {}
    custom_data = data.get("custom_data") or {}
    user_id = (custom_data.get("user_id") or "").strip()
    plan = (custom_data.get("plan") or "free").strip().lower()
    if plan not in PLANS:
        plan = "free"

    conn = await get_conn()
    try:
        if event_type == "subscription.created" and user_id:
            paddle_sub_id = str(data.get("subscription_id") or data.get("id") or "")
            quota = PLANS[plan]["quota"]
            await conn.execute(
                """
                INSERT INTO subscriptions (user_id, plan, status, quota, paddle_subscription_id, updated_at)
                VALUES ($1, $2, 'active', $3, $4, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    plan = EXCLUDED.plan,
                    status = 'active',
                    quota = EXCLUDED.quota,
                    paddle_subscription_id = EXCLUDED.paddle_subscription_id,
                    updated_at = NOW()
                """,
                user_id,
                plan,
                quota,
                paddle_sub_id or None,
            )
            if user_id:
                await conn.execute(
                    "UPDATE api_keys SET quota_limit = $1, updated_at = NOW() WHERE user_id = $2",
                    quota,
                    user_id,
                )

        elif event_type == "subscription.cancelled":
            if user_id:
                await conn.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'cancelled', cancel_at_period_end = true, updated_at = NOW()
                    WHERE user_id = $1
                    """,
                    user_id,
                )

        elif event_type == "subscription.updated":
            if user_id:
                status = str(data.get("status") or "active")
                await conn.execute(
                    """
                    UPDATE subscriptions
                    SET status = $1, updated_at = NOW()
                    WHERE user_id = $2
                    """,
                    status,
                    user_id,
                )

        return {"status": "ok"}
    finally:
        await release_conn(conn)
