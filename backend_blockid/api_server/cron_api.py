"""
Auto-recalculate trust scores for active users.
Protected endpoint — called by external cron service every 7 days.
Returns immediately, runs recalculation in background.
"""
import os
import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException, BackgroundTasks

import asyncpg

logger = logging.getLogger("cron_recalculate")

router = APIRouter(prefix="/cron", tags=["cron"])

CRON_SECRET = os.environ.get("CRON_SECRET", "")


async def run_batch_recalculate():
    """Background task: recalculate all active wallets."""
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    if not DATABASE_URL:
        logger.error("[CRON] DATABASE_URL not set")
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        # Get active wallets
        try:
            rows = await pool.fetch("""
                SELECT DISTINCT wallet FROM wallet_meta
                WHERE last_login IS NOT NULL
                AND last_login > NOW() - INTERVAL '7 days'
                ORDER BY wallet
            """)
        except Exception:
            rows = await pool.fetch("""
                SELECT DISTINCT wallet FROM wallet_meta
                WHERE updated_at > NOW() - INTERVAL '7 days'
                UNION
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


@router.post("/recalculate")
async def auto_recalculate(
    background_tasks: BackgroundTasks,
    x_cron_secret: str = Header(None),
):
    """
    Trigger batch recalculate in background.
    Returns immediately with status "started".
    """
    if not CRON_SECRET or x_cron_secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    background_tasks.add_task(run_batch_recalculate)

    return {
        "status": "started",
        "message": "Batch recalculate triggered in background",
        "timestamp": datetime.utcnow().isoformat(),
    }
