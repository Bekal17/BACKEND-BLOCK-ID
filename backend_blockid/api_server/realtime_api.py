"""
Real-time Risk Engine API.

POST /realtime/update_wallet/{wallet} — manual trigger for testing.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from solders.pubkey import Pubkey

from backend_blockid.oracle.realtime_risk_engine import update_wallet_risk

router = APIRouter(prefix="/realtime", tags=["Realtime Risk"])


class UpdateWalletResponse(BaseModel):
    """POST /realtime/update_wallet/{wallet} response."""

    wallet: str = Field(..., description="Wallet address")
    updated: bool = Field(..., description="True if risk was updated")
    new_score: int | None = Field(None, description="New trust score (0–100)")
    reasons: list[str] = Field(default_factory=list, description="Reason codes applied")


@router.post("/update_wallet/{wallet}", response_model=UpdateWalletResponse)
async def post_update_wallet(wallet: str) -> UpdateWalletResponse:
    """
    Manual trigger: update risk score for a wallet.
    Rate-limited to once per 5 minutes per wallet.
    For testing and on-demand refresh.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    from backend_blockid.oracle.realtime_risk_engine import update_wallet_risk

    result = await update_wallet_risk(wallet)
    if result is None:
        return UpdateWalletResponse(wallet=wallet, updated=False)
    return UpdateWalletResponse(
        wallet=result["wallet"],
        updated=True,
        new_score=result.get("new_score"),
        reasons=result.get("reasons", []),
    )
