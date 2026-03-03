"""
Explainable wallet reasons endpoint.

GET /wallet/{wallet}/explain?lang=en
Returns human-readable explanations for trust score and reason codes.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend_blockid.ai_engine.explain_reason import (
    generate_explanation,
    generate_summary,
)
from backend_blockid.ai_engine.reason_templates import DEFAULT_LANG, REASON_TEMPLATES
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/wallet", tags=["explain"])


@router.get("/{wallet}/explain")
def explain_wallet(wallet: str, lang: str = DEFAULT_LANG) -> dict:
    """
    Return human-readable explanations for a wallet's trust score and reasons.
    Uses template-based, deterministic text. No ML.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    if lang not in REASON_TEMPLATES:
        lang = DEFAULT_LANG

    try:
        result = generate_explanation(wallet, lang=lang)
    except Exception as e:
        logger.warning("explain_wallet_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail="Failed to load wallet data") from e

    explanations = result.get("explanations") or []

    summary = generate_summary(explanations, top_n=2)
    details = [e["text"] for e in explanations]

    logger.info(
        "explain",
        wallet=wallet[:16] + "...",
        reasons=len(explanations),
        lang=lang,
    )

    return {
        "wallet": wallet,
        "score": result.get("score", 50),
        "risk": result.get("risk", "1"),
        "summary": summary,
        "details": details,
    }
