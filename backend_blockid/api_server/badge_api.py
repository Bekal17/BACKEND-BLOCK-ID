"""
Trust Badge API for BlockID.

GET /wallet/{wallet}/badge         — badge info for frontend
GET /wallet/{wallet}/badge.svg     — SVG badge
GET /wallet/{wallet}/badge_timeline — evolution timeline for UI and Phantom plugin
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from backend_blockid.ai_engine.generate_badge import generate_badge, generate_svg_badge
from backend_blockid.blockid_logging import get_logger
from backend_blockid.tools.badge_engine import get_badge_timeline

logger = get_logger(__name__)

router = APIRouter(prefix="/wallet", tags=["badge"])


@router.get("/{wallet}/badge")
def get_wallet_badge(wallet: str) -> dict:
    """
    Return trust badge info for frontend display.
    Uses trust_scores table only. Does not recompute score.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        badge = generate_badge(wallet)
    except Exception as e:
        logger.warning("badge_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=404, detail="Wallet not found") from e

    logger.info(
        "badge",
        wallet=wallet[:16] + "...",
        score=badge["score"],
        badge=badge["badge"],
    )

    return {
        "wallet": wallet,
        "score": badge["score"],
        "risk": badge["risk"],
        "risk_level_text": badge["risk_level_text"],
        "badge": badge["badge"],
        "color": badge["color"],
        "summary": badge["summary"],
        "top_reasons": badge["top_reasons"],
        "message": badge["message"],
    }


@router.get("/{wallet}/badge.svg")
def get_wallet_badge_svg(wallet: str, size: str = "medium") -> Response:
    """Return SVG badge. Frontend can embed directly."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        badge = generate_badge(wallet)
    except Exception as e:
        logger.warning("badge_svg_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=404, detail="Wallet not found") from e

    svg = generate_svg_badge(
        badge["score"],
        badge["badge"],
        badge["color"],
        size=size if size in ("small", "medium", "large") else "medium",
    )
    return Response(content=svg, media_type="image/svg+xml")


@router.get("/{wallet}/badge_timeline")
def get_wallet_badge_timeline(wallet: str) -> list[dict]:
    """
    Return badge evolution timeline for wallet.
    For UI timeline chart and Phantom plugin overlay (badge + change arrow).
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        timeline = get_badge_timeline(wallet)
    except Exception as e:
        logger.warning("badge_timeline_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e

    return [{"date": t["date"], "badge": t["badge"], "score": t["score"]} for t in timeline]
