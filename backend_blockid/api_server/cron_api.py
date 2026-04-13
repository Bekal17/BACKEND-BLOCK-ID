"""
Auto-recalculate trust scores for active users.
Protected endpoint — called by external cron service every 7 days.
"""
import os
import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException
from starlette.responses import JSONResponse

logger = logging.getLogger("cron_recalculate")

router = APIRouter(prefix="/cron", tags=["cron"])

CRON_SECRET = os.environ.get("CRON_SECRET", "")


@router.post("/recalculate")
async def auto_recalculate(x_cron_secret: str = Header(None)):
    """
    Recalculate trust scores for all active wallets.
    Protected by CRON_SECRET header.
    """
    if not CRON_SECRET or x_cron_secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")

    from backend_blockid.oracle.realtime_wallet_pipeline import (
        run_realtime_wallet_pipeline,
    )

    # Get active wallets from DB
    import asyncpg

    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    if not DATABASE_URL:
        return JSONResponse(
            status_code=500,
            content={"error": "DATABASE_URL not set"},
        )

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        # Try last_login first, fallback to updated_at or social activity
        try:
            rows = await pool.fetch("""
                SELECT DISTINCT wallet FROM wallet_meta
                WHERE last_login IS NOT NULL
                AND last_login > NOW() - INTERVAL '7 days'
                ORDER BY wallet
            """)
        except Exception:
            # Fallback: use updated_at or social_posts activity
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

        logger.info(f"[CRON] Auto-recalculate started. Active wallets: {total}")

        if total == 0:
            return {"status": "ok", "message": "No active wallets", "total": 0}

        success = 0
        failed = 0

        for i, wallet in enumerate(wallets, 1):
            try:
                logger.info(f"[CRON] [{i}/{total}] Recalculating {wallet[:16]}...")
                await run_realtime_wallet_pipeline(wallet)
                success += 1
            except Exception as e:
                logger.error(f"[CRON] [{i}/{total}] FAILED {wallet[:16]}: {e}")
                failed += 1

            # Rate limit: 2 seconds between wallets
            if i < total:
                await asyncio.sleep(2)

        result = {
            "status": "ok",
            "total": total,
            "success": success,
            "failed": failed,
            "timestamp": datetime.utcnow().isoformat(),
        }
        logger.info(f"[CRON] Done: {result}")
        return result

    finally:
        await pool.close()
