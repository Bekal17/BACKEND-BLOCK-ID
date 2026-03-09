"""
B2B API — public endpoints for external customers.
Protected by ApiKeyMiddleware (/v1/ prefix).
Uses existing trust score logic from database.repositories.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator

from backend_blockid.database.repositories import get_trust_score_latest, get_latest_trust_scores_batch

router = APIRouter(prefix="/v1", tags=["B2B API"])


# -----------------------------------------------------------------------------
# Request/Response models
# -----------------------------------------------------------------------------


class BatchScoreRequest(BaseModel):
    wallets: list[str] = Field(..., description="List of wallet addresses (max 50)")

    @model_validator(mode="after")
    def check_max_wallets(self) -> "BatchScoreRequest":
        if len(self.wallets) > 50:
            raise ValueError("Maximum 50 wallets per batch")
        return self


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _validate_wallet(wallet: str) -> None:
    """Validate Solana pubkey format. Raises HTTPException if invalid."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid wallet pubkey")


def _updated_at_iso(ts: int) -> str:
    """Unix seconds to ISO 8601 UTC string."""
    if ts == 0:
        return "1970-01-01T00:00:00Z"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return str(ts)


def _record_to_b2b(wallet: str, record: dict) -> dict:
    """Build B2B response from DB record. No oracle/pda details."""
    score_val = float(record.get("score") or 0)
    computed_at = int(record.get("computed_at") or 0)
    risk = 1  # default

    meta_json = record.get("metadata_json") or "{}"
    try:
        meta = json.loads(meta_json)
        if "risk" in meta and isinstance(meta["risk"], (int, float)):
            risk = int(meta["risk"])
            risk = max(0, min(3, risk))
        else:
            risk_level = record.get("risk_level") or ""
            if str(risk_level).isdigit():
                risk = int(risk_level)
                risk = max(0, min(3, risk))
            else:
                risk = _score_to_risk_int(int(round(score_val)))
    except (json.JSONDecodeError, TypeError, ValueError):
        risk_level = record.get("risk_level") or ""
        if str(risk_level).isdigit():
            risk = int(risk_level)
            risk = max(0, min(3, risk))
        else:
            risk = _score_to_risk_int(int(round(score_val)))

    return {
        "wallet": wallet,
        "score": int(round(score_val)),
        "risk": risk,
        "updated_at": _updated_at_iso(computed_at),
    }


def _score_to_risk_int(score: int) -> int:
    """Map trust score (0-100) to risk level 0-3."""
    if score <= 25:
        return 3
    if score <= 50:
        return 2
    if score <= 75:
        return 1
    return 0


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------


@router.get("/score/{wallet}")
async def get_score(wallet: str) -> dict:
    """
    Get trust score for a single wallet.
    Returns 404 if wallet not scored.
    """
    _validate_wallet(wallet)
    latest = await get_trust_score_latest(wallet)
    if not latest:
        raise HTTPException(status_code=404, detail="Trust score not found for this wallet")
    return _record_to_b2b(wallet, latest)


@router.post("/score/batch")
async def get_score_batch(req: BatchScoreRequest) -> list[dict]:
    """
    Get trust scores for up to 50 wallets.
    Missing wallets return { wallet, status: "not_scored" }.
    """
    wallets = [w.strip() for w in (req.wallets or []) if w and w.strip()]

    for w in wallets:
        try:
            from solders.pubkey import Pubkey
            Pubkey.from_string(w)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid wallet pubkey: {w[:16]}...")

    batch = await get_latest_trust_scores_batch(wallets)
    result = []
    for w in wallets:
        if w in batch:
            result.append(_record_to_b2b(w, batch[w]))
        else:
            result.append({"wallet": w, "status": "not_scored"})
    return result
