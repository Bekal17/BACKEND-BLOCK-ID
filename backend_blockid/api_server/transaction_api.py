"""
Transaction-level risk check API for BlockID.

POST /transaction/check — analyzes transaction preview for Phantom plugin.
Checks: trust score of receiver, scam cluster, large transfer, drainer.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.connection import get_connection

logger = get_logger(__name__)

router = APIRouter(prefix="/transaction", tags=["transaction"])

LARGE_SOL_THRESHOLD = 10.0  # SOL
HIGH_RISK_SCORE_THRESHOLD = 40
EXTREME_RISK_SCORE_THRESHOLD = 20


class TransactionCheckRequest(BaseModel):
    """Transaction preview from Phantom plugin. Privacy: only public data."""

    from_: str = Field("", alias="from", description="Sender wallet (fee payer)")
    to: str = Field(..., description="Receiver wallet")
    token: str = Field(default="SOL", description="SOL or token mint")
    amount: float | None = Field(default=None, description="Amount (SOL or token units)")

    model_config = {"populate_by_name": True}


class TransactionCheckResponse(BaseModel):
    """Risk assessment for transaction."""

    risk_level: int = Field(..., description="0=Low, 1=Medium, 2=High, 3=Extreme")
    warning_reason: str | None = Field(None, description="Primary warning reason")
    confidence: float = Field(0.0, ge=0, le=1)
    to_score: float | None = Field(None, description="Trust score of receiver")
    to_badge: str | None = Field(None)
    top_reason: str | None = Field(None)
    cluster_link: bool = Field(False, description="Receiver in scam cluster")
    special_warning: str | None = Field(None, description="e.g. new_wallet_large_transfer, drainer_contract")


def _load_scam_wallets(cur) -> set[str]:
    out: set[str] = set()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM scam_wallets")
            for r in cur.fetchall():
                w = (r["wallet"] if hasattr(r, "keys") else r[0] or "").strip()
                if w:
                    out.add(w)
    except Exception:
        pass
    csv_path = Path(__file__).resolve().parent.parent / "data" / "scam_wallets.csv"
    if csv_path.exists():
        import csv
        with open(csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or list(row.values())[0] if row else "").strip()
                if w and len(w) >= 32:
                    out.add(w)
    return out


@router.post("/check", response_model=TransactionCheckResponse)
def check_transaction(req: TransactionCheckRequest) -> TransactionCheckResponse:
    """
    Check transaction risk for Phantom plugin.
    Privacy: Only public wallet + tx preview. Never private keys.
    """
    from_w = (req.from_ or "").strip()
    to_w = (req.to or "").strip()
    if not to_w or len(to_w) < 32:
        raise HTTPException(status_code=400, detail="to_wallet required")
    if not from_w or len(from_w) < 32:
        raise HTTPException(status_code=400, detail="from_wallet required")

    conn = get_connection()
    cur = conn.cursor()

    scam_wallets = _load_scam_wallets(cur)
    to_is_scam = to_w in scam_wallets

    cur.execute(
        "SELECT score, risk_level FROM trust_scores WHERE wallet = ? LIMIT 1",
        (to_w,),
    )
    row = cur.fetchone()
    to_score: float | None = None
    to_risk: int = 1
    if row:
        to_score = float((row["score"] if hasattr(row, "keys") else row[0]) or 50)
        to_risk = int((row["risk_level"] if hasattr(row, "keys") else row[1]) or 1)

    cur.execute(
        """
        SELECT reason_code FROM wallet_reasons
        WHERE wallet = ? AND reason_code IS NOT NULL AND reason_code != 'NO_RISK_DETECTED'
        ORDER BY ABS(weight) DESC LIMIT 1
        """,
        (to_w,),
    )
    reason_row = cur.fetchone()
    top_reason = (reason_row["reason_code"] if hasattr(reason_row, "keys") else reason_row[0]) if reason_row else None

    in_cluster = False
    for tbl in ("wallet_clusters", "wallet_cluster_members", "wallet_graph_clusters"):
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if cur.fetchone():
                cur.execute("SELECT 1 FROM " + tbl + " WHERE wallet = ? LIMIT 1", (to_w,))
                if cur.fetchone():
                    in_cluster = True
                    break
        except Exception:
            pass

    badge = "UNKNOWN"
    if to_score is not None:
        if to_score >= 70:
            badge = "TRUSTED"
        elif to_score >= 50:
            badge = "LOW_RISK"
        elif to_score >= 30:
            badge = "MEDIUM_RISK"
        elif to_score >= 10:
            badge = "HIGH_RISK"
        else:
            badge = "SCAM_SUSPECTED"

    risk_level = 0
    warning_reason: str | None = None
    confidence = 0.0
    special_warning: str | None = None

    if to_is_scam:
        risk_level = 3
        warning_reason = "Receiver is a known scam wallet"
        confidence = 1.0
        special_warning = "drainer_or_scam"
    elif to_score is not None:
        if to_score < EXTREME_RISK_SCORE_THRESHOLD:
            risk_level = 3
            warning_reason = top_reason or "Receiver has very low trust score"
            confidence = 0.9
        elif to_score < HIGH_RISK_SCORE_THRESHOLD:
            risk_level = 2
            warning_reason = top_reason or "Receiver has low trust score"
            confidence = 0.85
        elif to_score < 70:
            risk_level = 1
            warning_reason = "Receiver has moderate risk"
            confidence = 0.7

    if req.amount is not None and req.token == "SOL" and req.amount >= LARGE_SOL_THRESHOLD and risk_level > 0:
        special_warning = "large_transfer_to_risky"

    if in_cluster and risk_level < 3:
        risk_level = max(risk_level, 2)
        if not warning_reason:
            warning_reason = "Receiver linked to scam cluster"

    conn.close()

    logger.info(
        "transaction_check",
        from_w=from_w[:16] + "...",
        to_w=to_w[:16] + "...",
        risk_level=risk_level,
        to_score=to_score,
    )

    return TransactionCheckResponse(
        risk_level=risk_level,
        warning_reason=warning_reason,
        confidence=confidence,
        to_score=to_score,
        to_badge=badge,
        top_reason=top_reason,
        cluster_link=in_cluster,
        special_warning=special_warning,
    )
