"""
Wallet Investigation Report API for BlockID.

GET /wallet/{wallet}/report
Returns PDF report for compliance and exchange review.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from backend_blockid.blockid_logging import get_logger
from backend_blockid.tools.generate_wallet_report import generate_wallet_report

logger = get_logger(__name__)

router = APIRouter(prefix="/wallet", tags=["report"])


@router.get("/{wallet}/report")
def get_wallet_report(wallet: str) -> FileResponse:
    """
    Generate and return wallet investigation PDF report.
    Returns 404 if wallet not found in trust_scores.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    path, meta = generate_wallet_report(wallet, output_path=None)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Wallet not found") from None

    score = meta.get("score", "?") if meta else "?"
    cluster = meta.get("cluster", "?") if meta else "?"
    tx_count = meta.get("tx_count", 0) if meta else 0
    logger.info(
        "report",
        wallet=wallet[:16] + "...",
        score=score,
        cluster=cluster,
        tx=tx_count,
        saved=str(path),
    )

    safe_name = wallet[:48].replace("/", "_") + ".pdf"
    return FileResponse(
        path=str(path),
        media_type="application/pdf",
        filename=f"blockid_report_{safe_name}",
    )
