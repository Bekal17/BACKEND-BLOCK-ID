"""
Subscription tier + wallet scan usage API (Paddle webhooks).
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)
router = APIRouter(prefix="/subscription", tags=["Subscription"])

TIER_LIMITS: dict[str, dict[str, Any]] = {
    "FREE": {"scan_limit": 10, "handle_limit": 0, "wallet_graph": False},
    "EXPLORER": {"scan_limit": 100, "handle_limit": 1, "wallet_graph": True},
    "PRO": {"scan_limit": None, "handle_limit": 3, "wallet_graph": True},
}


@router.get("/status/{wallet}")
async def get_subscription_status(wallet: str) -> dict:
    """
    Get subscription tier and usage for a wallet.
    Returns tier, limits, and current usage.
    Defaults to FREE if no subscription found.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        sub = await conn.fetchrow(
            """
            SELECT tier, status, paddle_subscription_id,
                   current_period_end, cancel_at_period_end
            FROM subscriptions
            WHERE wallet = $1
            """,
            wallet,
        )

        current_month = datetime.now().strftime("%Y-%m")
        usage = await conn.fetchrow(
            """
            SELECT scan_count FROM wallet_scan_usage
            WHERE wallet = $1 AND month = $2
            """,
            wallet,
            current_month,
        )

        tier = "FREE"
        status = "active"
        period_end = None
        cancel_at_period_end = False

        if sub and sub["status"] == "active":
            tier = sub["tier"] or "FREE"
            status = sub["status"]
            period_end = sub["current_period_end"]
            cancel_at_period_end = bool(sub["cancel_at_period_end"])

        limits = TIER_LIMITS.get(tier, TIER_LIMITS["FREE"])
        scans_used = int(usage["scan_count"]) if usage else 0
        scan_limit = limits["scan_limit"]

        return {
            "wallet": wallet,
            "tier": tier,
            "status": status,
            "scan_limit": scan_limit,
            "scans_used": scans_used,
            "scans_remaining": (scan_limit - scans_used) if scan_limit is not None else None,
            "handle_limit": limits["handle_limit"],
            "wallet_graph": limits["wallet_graph"],
            "current_period_end": period_end.isoformat() if period_end else None,
            "cancel_at_period_end": cancel_at_period_end,
            "paddle_subscription_id": sub["paddle_subscription_id"] if sub else None,
        }
    finally:
        await release_conn(conn)


@router.post("/scan/increment/{wallet}")
async def increment_scan_usage(wallet: str) -> dict:
    """
    Increment wallet scan count for current month.
    Check if wallet has exceeded their tier limit first.
    Returns { allowed, scans_used, scan_limit, tier }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        sub = await conn.fetchrow(
            "SELECT tier, status FROM subscriptions WHERE wallet = $1",
            wallet,
        )
        tier = "FREE"
        if sub and sub["status"] == "active":
            tier = sub["tier"] or "FREE"

        limits = TIER_LIMITS.get(tier, TIER_LIMITS["FREE"])
        scan_limit = limits["scan_limit"]

        current_month = datetime.now().strftime("%Y-%m")

        usage = await conn.fetchrow(
            """
            SELECT scan_count FROM wallet_scan_usage
            WHERE wallet = $1 AND month = $2
            """,
            wallet,
            current_month,
        )
        scans_used = int(usage["scan_count"]) if usage else 0

        if scan_limit is not None and scans_used >= scan_limit:
            return {
                "allowed": False,
                "scans_used": scans_used,
                "scan_limit": scan_limit,
                "tier": tier,
                "message": f"Scan limit reached for {tier} tier. Upgrade to get more scans.",
            }

        await conn.execute(
            """
            INSERT INTO wallet_scan_usage (wallet, month, scan_count, updated_at)
            VALUES ($1, $2, 1, NOW())
            ON CONFLICT (wallet, month)
            DO UPDATE SET
              scan_count = wallet_scan_usage.scan_count + 1,
              updated_at = NOW()
            """,
            wallet,
            current_month,
        )

        return {
            "allowed": True,
            "scans_used": scans_used + 1,
            "scan_limit": scan_limit,
            "tier": tier,
        }
    finally:
        await release_conn(conn)


class PaddleWebhookBody(BaseModel):
    event_type: str
    data: dict


@router.post("/webhook/paddle")
async def paddle_webhook(body: PaddleWebhookBody) -> dict:
    """
    Handle Paddle webhook events.
    Updates subscription tier based on payment status.

    Events handled:
    - subscription.created → set tier to EXPLORER or PRO
    - subscription.updated → update tier/period
    - subscription.canceled → set status to canceled
    - subscription.payment_failed → set status to past_due
    """
    event_type = body.event_type
    data = body.data

    logger.info("paddle_webhook_received", event_type=event_type)

    conn = await get_conn()
    try:
        if event_type in ("subscription.created", "subscription.activated", "subscription.updated"):
            wallet = (data.get("custom_data") or {}).get("wallet")
            if not wallet and event_type != "subscription.updated":
                logger.warning("paddle_webhook_no_wallet", event=event_type)
                return {"status": "ignored", "reason": "no wallet in custom_data"}

            items = data.get("items") or [{}]
            price_id = (items[0].get("price") or {}).get("id", "")
            tier = _get_tier_from_price(price_id)

            if event_type == "subscription.updated" and not wallet:
                sub_id = data.get("id")
                tier_update = _get_tier_from_price(price_id) if price_id else None
                await conn.execute(
                    """
                    UPDATE subscriptions SET
                      tier = COALESCE($2, tier),
                      current_period_start = COALESCE($3, current_period_start),
                      current_period_end = COALESCE($4, current_period_end),
                      updated_at = NOW()
                    WHERE paddle_subscription_id = $1
                    """,
                    sub_id,
                    tier_update,
                    _parse_date((data.get("current_billing_period") or {}).get("starts_at")),
                    _parse_date((data.get("current_billing_period") or {}).get("ends_at")),
                )
                logger.info("subscription_updated", subscription_id=sub_id)
            elif wallet:
                await conn.execute(
                    """
                    INSERT INTO subscriptions
                      (wallet, tier, paddle_subscription_id, paddle_customer_id,
                       status, current_period_start, current_period_end, updated_at)
                    VALUES ($1, $2, $3, $4, 'active', $5, $6, NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                      tier = EXCLUDED.tier,
                      paddle_subscription_id = EXCLUDED.paddle_subscription_id,
                      paddle_customer_id = EXCLUDED.paddle_customer_id,
                      status = 'active',
                      current_period_start = EXCLUDED.current_period_start,
                      current_period_end = EXCLUDED.current_period_end,
                      cancel_at_period_end = FALSE,
                      updated_at = NOW()
                    """,
                    wallet,
                    tier,
                    data.get("id"),
                    data.get("customer_id"),
                    _parse_date((data.get("current_billing_period") or {}).get("starts_at")),
                    _parse_date((data.get("current_billing_period") or {}).get("ends_at")),
                )
                logger.info("subscription_upsert", wallet=wallet, tier=tier, event=event_type)

        elif event_type == "subscription.canceled":
            sub_id = data.get("id")
            await conn.execute(
                """
                UPDATE subscriptions SET
                  status = 'canceled',
                  cancel_at_period_end = TRUE,
                  updated_at = NOW()
                WHERE paddle_subscription_id = $1
                """,
                sub_id,
            )
            logger.info("subscription_canceled", subscription_id=sub_id)

        elif event_type == "subscription.payment_failed":
            sub_id = data.get("id")
            await conn.execute(
                """
                UPDATE subscriptions SET
                  status = 'past_due',
                  updated_at = NOW()
                WHERE paddle_subscription_id = $1
                """,
                sub_id,
            )

        return {"status": "ok"}
    finally:
        await release_conn(conn)


def _get_tier_from_price(price_id: str) -> str:
    """Map Paddle price ID to BlockID tier."""
    explorer_price = os.getenv("PADDLE_EXPLORER_PRICE_ID", "")
    pro_price = os.getenv("PADDLE_PRO_PRICE_ID", "")

    if price_id and price_id == pro_price:
        return "PRO"
    if price_id and price_id == explorer_price:
        return "EXPLORER"
    return "EXPLORER"


def _parse_date(date_str: Optional[str]):
    """Parse ISO date string to datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
    except Exception:
        return None
