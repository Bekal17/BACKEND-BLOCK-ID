"""
Public landing-page stats (no auth).

Uses SQLAlchemy against DATABASE_URL / BLOCKID_DB_URL.
"""

from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["stats"])


def _database_url() -> str:
    url = (os.getenv("DATABASE_URL") or os.getenv("BLOCKID_DB_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL or BLOCKID_DB_URL is not set")
    return url


def _engine():
    return create_engine(_database_url(), pool_pre_ping=True)


def _compute_stats() -> dict:
    engine = _engine()
    with engine.connect() as conn:
        total_users = int(
            conn.execute(text("SELECT COUNT(*) FROM identity_nft")).scalar_one()
        )
        total_scans = int(
            conn.execute(
                text("SELECT COUNT(DISTINCT wallet) FROM helius_usage")
            ).scalar_one()
        )
        total_posts = int(
            conn.execute(text("SELECT COUNT(*) FROM social_posts")).scalar_one()
        )

    return {
        "total_users": total_users,
        "total_scans": total_scans,
        "total_posts": total_posts,
    }


@router.get("/stats")
async def get_public_stats():
    """
    Public metrics for marketing / landing page.
    """
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }

    try:
        data = await asyncio.to_thread(_compute_stats)
    except Exception as e:
        logger.warning("stats_endpoint_error", error=str(e))
        return JSONResponse(
            status_code=503,
            content={
                "total_users": 0,
                "total_scans": 0,
                "total_posts": 0,
                "error": "stats_unavailable",
            },
            headers=cors_headers,
        )

    return JSONResponse(content=data, headers=cors_headers)
