"""
BlockID Review Queue API.

GET /review_queue - list pending items
POST /review_queue/{wallet}/approve - approve wallet for publish
POST /review_queue/{wallet}/reject - reject wallet
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend_blockid.blockid_logging import get_logger
from backend_blockid.tools.review_queue_engine import approve, list_pending, reject

logger = get_logger(__name__)

router = APIRouter(prefix="/review_queue", tags=["review_queue"])


@router.get("")
def get_review_queue() -> list[dict]:
    """Return pending wallets for manual review."""
    items = list_pending()
    return items


@router.post("/{wallet}/approve")
def approve_wallet(wallet: str) -> dict:
    """Approve wallet; publish allowed next run."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    if approve(wallet):
        logger.info("review_queue_approved", wallet=wallet[:16] + "...")
        return {"wallet": wallet, "status": "approved"}
    raise HTTPException(status_code=404, detail="Wallet not in review queue")


@router.post("/{wallet}/reject")
def reject_wallet(wallet: str) -> dict:
    """Reject wallet; remains blocked from publish."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    if reject(wallet):
        logger.info("review_queue_rejected", wallet=wallet[:16] + "...")
        return {"wallet": wallet, "status": "rejected"}
    raise HTTPException(status_code=404, detail="Wallet not in review queue")
