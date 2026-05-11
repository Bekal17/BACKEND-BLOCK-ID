"""
Auto-recalculate trust scores for active users.
Protected endpoint — called by external cron service every 7 days.
Returns immediately, runs recalculation in background.
"""
import os
import asyncio
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, BackgroundTasks
from fastapi import Request
from pydantic import BaseModel

import asyncpg
from backend_blockid.oracle.blacklist_sync import sync_allenhark_blacklist

logger = logging.getLogger("cron_recalculate")

router = APIRouter(prefix="/cron", tags=["cron"])

CRON_SECRET = os.environ.get("CRON_SECRET", "")


class RecalculateRequest(BaseModel):
    wallet: Optional[str] = None


async def run_batch_recalculate():
    """Background task: recalculate all active wallets."""
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    if not DATABASE_URL:
        logger.error("[CRON] DATABASE_URL not set")
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        # Get active wallets from social activity only
        rows = await pool.fetch("""
            SELECT DISTINCT wallet FROM social_posts
            WHERE created_at > NOW() - INTERVAL '7 days'
            ORDER BY wallet
        """)

        wallets = [row["wallet"] for row in rows]
        total = len(wallets)
        logger.info(f"[CRON] Active wallets: {total}")

        if total == 0:
            logger.info("[CRON] No active wallets. Done.")
            return

        from backend_blockid.oracle.realtime_wallet_pipeline import (
            run_realtime_wallet_pipeline,
        )

        success = 0
        failed = 0

        for i, wallet in enumerate(wallets, 1):
            try:
                logger.info(f"[CRON] [{i}/{total}] {wallet[:16]}...")
                await run_realtime_wallet_pipeline(wallet)
                success += 1
            except Exception as e:
                logger.error(f"[CRON] [{i}/{total}] FAIL {wallet[:16]}: {e}")
                failed += 1

            # Rate limit: 2 seconds between wallets
            if i < total:
                await asyncio.sleep(2)

        logger.info(f"[CRON] Complete. Success: {success}, Failed: {failed}")

    except Exception as e:
        logger.error(f"[CRON] Batch error: {e}")
    finally:
        await pool.close()


async def run_single_wallet_recalculate(wallet: str):
    """Recalculate trust score for a single wallet (testing / admin use)."""
    import asyncpg

    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    if not DATABASE_URL:
        logger.error("[CRON-SINGLE] DATABASE_URL not set")
        return

    try:
        from backend_blockid.oracle.realtime_wallet_pipeline import (
            run_realtime_wallet_pipeline,
        )
        logger.info(f"[CRON-SINGLE] Starting recalculate for {wallet[:16]}...")
        await run_realtime_wallet_pipeline(wallet)
        logger.info(f"[CRON-SINGLE] Completed recalculate for {wallet[:16]}")
    except Exception as e:
        logger.error(f"[CRON-SINGLE] FAIL {wallet[:16]}: {e}")


@router.post("/recalculate")
async def auto_recalculate(
    background_tasks: BackgroundTasks,
    request: RecalculateRequest = RecalculateRequest(),
    x_cron_secret: str = Header(None),
):
    """
    Trigger batch recalculate in background.
    Returns immediately with status "started".
    """
    if not CRON_SECRET or x_cron_secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    # Single-wallet mode: body has {"wallet": "..."}
    if request.wallet:
        background_tasks.add_task(run_single_wallet_recalculate, request.wallet)
        return {
            "status": "started",
            "mode": "single_wallet",
            "wallet": request.wallet,
            "message": f"Single wallet recalculate triggered: {request.wallet[:16]}...",
            "timestamp": datetime.utcnow().isoformat(),
        }

    # Full-sweep mode (existing behavior, used by Thursday cron)
    background_tasks.add_task(run_batch_recalculate)

    return {
        "status": "started",
        "message": "Batch recalculate triggered in background",
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.post("/sync-blacklist")
async def sync_blacklist(request: Request):
    """
    Daily cron endpoint to sync allenhark scammer blacklist.
    Call this once per day from cron-job.org.
    Protected by X-Cron-Secret header.
    """
    secret = request.headers.get("X-Cron-Secret")
    if secret != os.environ.get("CRON_SECRET", ""):
        return {"error": "unauthorized"}, 401

    result = await sync_allenhark_blacklist()
    return result
