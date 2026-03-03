"""
Helius Cost Report API for BlockID.

GET /helius/cost_report — latest usage stats from helius_usage.
"""
from __future__ import annotations

from fastapi import APIRouter

from backend_blockid.tools.helius_cost_monitor import run_report

router = APIRouter(prefix="/helius", tags=["Helius Cost"])


@router.get("/cost_report")
def get_helius_cost_report() -> dict:
    """
    Return latest Helius API usage stats: total calls today, estimated cost,
    top 10 wallets, and budget status.
    """
    return run_report()
