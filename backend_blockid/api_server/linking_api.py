"""
BlockID Behavioral Linking API — Phase 3.
Suggestions require user confirmation; never auto-link.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ml.behavioral_linking import (
    CONFIDENCE_THRESHOLD_SUGGEST,
    calculate_link_confidence,
    detect_signals,
    run_linking_scan,
    save_suggestions,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/linking", tags=["Behavioral Linking"])

DEVNET_BYPASS_SIGNATURES = {"devtest_signature_bypass"}
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")
MANUAL_LINK_THRESHOLD = 0.50


def _verify_signature(wallet: str, signed_message: str, signature: str, expected_prefix: str) -> bool:
    if BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS_SIGNATURES:
        return True
    if not signed_message.strip().startswith(expected_prefix):
        return False
    return bool(signature and len(signature) >= 64)


async def aggregate_linked_trust_score(owner_wallet: str, conn) -> float | None:
    """Weighted average of trust scores of all linked wallets (by confidence). Update trust_scores for owner."""
    handle_row = await conn.fetchrow(
        "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
        owner_wallet,
    )
    if not handle_row:
        return None
    handle = handle_row.get("handle")
    links = await conn.fetch(
        """
        SELECT wallet, ai_confidence FROM handle_wallet_links
        WHERE handle = $1 AND link_status = 'VERIFIED' AND ai_confidence IS NOT NULL
        """,
        handle,
    )
    wallets_to_agg = [owner_wallet]
    weights = [1.0]
    for r in links:
        w = r.get("wallet")
        if w:
            wallets_to_agg.append(w)
            weights.append(float(r.get("ai_confidence") or 0.5))
    if len(wallets_to_agg) <= 1:
        return None
    scores: list[tuple[float, float]] = []
    for i, w in enumerate(wallets_to_agg):
        row = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
            w,
        )
        if row and row.get("score") is not None:
            scores.append((float(row["score"]), weights[i]))
    if not scores:
        return None
    total_w = sum(w for _, w in scores)
    if total_w <= 0:
        return None
    agg = sum(s * w for s, w in scores) / total_w
    try:
        await conn.execute(
            """
            UPDATE trust_scores SET score = $2, last_updated = CURRENT_TIMESTAMP
            WHERE wallet = $1
            """,
            owner_wallet,
            round(agg, 2),
        )
    except Exception:
        pass
    return round(agg, 2)


# --- Request models ---


class RespondRequest(BaseModel):
    suggestion_id: int = Field(..., description="Suggestion ID")
    wallet: str = Field(..., description="Owner wallet")
    response: str = Field(..., description="ACCEPTED or REJECTED")
    signed_message: str = Field(default="")
    signature: str = Field(default="")


class ScanRequest(BaseModel):
    wallet: str = Field(..., description="Wallet to scan")
    signed_message: str = Field(default="")
    signature: str = Field(default="")


class ManualLinkRequest(BaseModel):
    owner_wallet: str = Field(..., description="Owner wallet")
    link_wallet: str = Field(..., description="Wallet to link")
    signed_message: str = Field(default="")
    signature: str = Field(default="")


# --- GET /linking/suggestions/{wallet} ---


@router.get("/suggestions/{wallet}")
async def get_suggestions(wallet: str) -> dict[str, Any]:
    """Get pending linking suggestions for a wallet. Only owner sees their suggestions."""
    wallet = (wallet or "").strip()
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT id, suggested_wallet, confidence, signals, suggested_at, expires_at
            FROM wallet_link_suggestions
            WHERE owner_wallet = $1 AND status = 'PENDING'
            AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY confidence DESC
            """,
            wallet,
        )
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            wallet,
        )
        handle = f"@{handle_row['handle']}" if handle_row and handle_row.get("handle") else None
        suggestions = []
        for r in rows:
            tier = "STRONG" if (r.get("confidence") or 0) >= 0.90 else "MEDIUM"
            suggested_at = r.get("suggested_at")
            expires_at = r.get("expires_at")
            suggestions.append({
                "id": r["id"],
                "suggested_wallet": r["suggested_wallet"],
                "confidence": r["confidence"],
                "signals": list(r["signals"] or []),
                "tier": tier,
                "suggested_at": suggested_at.strftime("%Y-%m-%d") if suggested_at and getattr(suggested_at, "strftime", None) else str(suggested_at or ""),
                "expires_at": expires_at.strftime("%Y-%m-%d") if expires_at and getattr(expires_at, "strftime", None) else str(expires_at or ""),
            })
        return {
            "wallet": wallet,
            "suggestions": suggestions,
            "total": len(suggestions),
        }
    finally:
        await release_conn(conn)


# --- POST /linking/respond ---


@router.post("/respond")
async def respond_to_suggestion(body: RespondRequest) -> dict[str, Any]:
    """User accepts or rejects a linking suggestion."""
    wallet = (body.wallet or "").strip()
    suggestion_id = body.suggestion_id
    response = (body.response or "").strip().upper()
    if response not in ("ACCEPTED", "REJECTED"):
        raise HTTPException(400, detail="response must be ACCEPTED or REJECTED")

    expected_prefix = f"BlockID Link Accept: suggestion_id={suggestion_id} by {wallet}"
    if not _verify_signature(wallet, body.signed_message, body.signature, expected_prefix):
        raise HTTPException(401, detail="Invalid or missing signature")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT owner_wallet, suggested_wallet, confidence, status FROM wallet_link_suggestions WHERE id = $1",
            suggestion_id,
        )
        if not row:
            raise HTTPException(404, detail="Suggestion not found")
        if (row["owner_wallet"] or "").strip() != wallet:
            raise HTTPException(403, detail="Suggestion does not belong to this wallet")
        if (row["status"] or "").strip() != "PENDING":
            raise HTTPException(400, detail="Suggestion already responded")

        suggested_wallet = (row["suggested_wallet"] or "").strip()
        confidence = float(row["confidence"] or 0)

        if response == "ACCEPTED":
            handle_row = await conn.fetchrow(
                "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
                wallet,
            )
            if handle_row and handle_row.get("handle"):
                handle = handle_row["handle"]
                await conn.execute(
                    """
                    INSERT INTO handle_wallet_links (handle, wallet, is_primary, link_status, ai_confidence, verified_at)
                    VALUES ($1, $2, FALSE, 'VERIFIED', $3, $4)
                    ON CONFLICT (handle, wallet) DO UPDATE SET link_status = 'VERIFIED', ai_confidence = $3, verified_at = $4
                    """,
                    handle,
                    suggested_wallet,
                    confidence,
                    datetime.utcnow(),
                )
                await aggregate_linked_trust_score(wallet, conn)
                msg = f"Wallet linked successfully to @{handle}"
            else:
                msg = "Wallet link accepted (no handle; link recorded)."
            # Recalculate linking boost for owner_wallet
            try:
                from backend_blockid.ml.behavioral_linking import calculate_linking_boost
                boost, linking_reasons = await calculate_linking_boost(wallet, conn)
                logger.info(
                    "linking_boost_on_accept",
                    wallet=wallet[:16],
                    boost=boost,
                    reasons=linking_reasons,
                )
            except Exception as e:
                logger.debug("linking_boost_skip", error=str(e))
        else:
            msg = "Suggestion rejected."

        await conn.execute(
            "UPDATE wallet_link_suggestions SET status = $2, responded_at = $3 WHERE id = $1",
            suggestion_id,
            "ACCEPTED" if response == "ACCEPTED" else "REJECTED",
            datetime.utcnow(),
        )
        return {
            "success": True,
            "suggestion_id": suggestion_id,
            "response": response,
            "linked_wallet": suggested_wallet if response == "ACCEPTED" else None,
            "message": msg,
        }
    finally:
        await release_conn(conn)


# --- POST /linking/scan ---


@router.post("/scan")
async def trigger_scan(body: ScanRequest) -> dict[str, Any]:
    """Trigger manual linking scan for a wallet."""
    wallet = (body.wallet or "").strip()
    if not wallet:
        raise HTTPException(400, detail="wallet required")
    expected_prefix = f"BlockID Link Scan: {wallet}"
    if not _verify_signature(wallet, body.signed_message, body.signature, expected_prefix):
        raise HTTPException(401, detail="Invalid or missing signature")

    conn = await get_conn()
    try:
        suggestions = await run_linking_scan(wallet, conn)
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            wallet,
        )
        handle = handle_row["handle"] if handle_row and handle_row.get("handle") else None
        saved = await save_suggestions(wallet, suggestions, handle, conn)
        return {
            "wallet": wallet,
            "new_suggestions": saved,
            "suggestions": suggestions,
        }
    finally:
        await release_conn(conn)


# --- GET /linking/linked/{wallet} ---


@router.get("/linked/{wallet}")
async def get_linked(wallet: str, owner: bool = False) -> dict[str, Any]:
    """Public: linked count + handle. Owner (owner=true): full linked_wallets list."""
    wallet = (wallet or "").strip()
    conn = await get_conn()
    try:
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            wallet,
        )
        handle = f"@{handle_row['handle']}" if handle_row and handle_row.get("handle") else None
        links = []
        if handle_row and handle_row.get("handle"):
            links = await conn.fetch(
                """
                SELECT wallet, ai_confidence, verified_at FROM handle_wallet_links
                WHERE handle = $1 AND link_status = 'VERIFIED'
                """,
                handle_row["handle"],
            )
        linked_count = len(links)
        out = {
            "wallet": wallet,
            "linked_count": linked_count,
            "handle": handle,
        }
        if owner and handle_row and links:
            out["linked_wallets"] = [
                {
                    "wallet": r["wallet"],
                    "confidence": r.get("ai_confidence"),
                    "signals": [],
                    "linked_at": r.get("verified_at").strftime("%Y-%m-%d") if r.get("verified_at") and getattr(r.get("verified_at"), "strftime", None) else str(r.get("verified_at") or ""),
                }
                for r in links
            ]
        return out
    finally:
        await release_conn(conn)


# --- POST /linking/manual ---


@router.post("/manual")
async def manual_link_request(body: ManualLinkRequest) -> dict[str, Any]:
    """User manually requests to link a specific wallet. Lower threshold (0.50)."""
    owner = (body.owner_wallet or "").strip()
    link_w = (body.link_wallet or "").strip()
    if not owner or not link_w:
        raise HTTPException(400, detail="owner_wallet and link_wallet required")
    expected_prefix = f"BlockID Manual Link: {link_w} to {owner}"
    if not _verify_signature(owner, body.signed_message, body.signature, expected_prefix):
        raise HTTPException(401, detail="Invalid or missing signature")

    conn = await get_conn()
    try:
        signals = await detect_signals(owner, link_w, conn)
        confidence = calculate_link_confidence(signals)
        if confidence < MANUAL_LINK_THRESHOLD:
            raise HTTPException(400, detail=f"Confidence too low for manual link: {confidence:.2f} (min 0.50)")

        now = datetime.utcnow()
        expires_at = now + timedelta(days=int(os.getenv("LINKING_SUGGESTION_EXPIRY_DAYS", "30")))
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            owner,
        )
        handle = handle_row["handle"] if handle_row and handle_row.get("handle") else None
        await conn.execute(
            """
            INSERT INTO wallet_link_suggestions
            (owner_wallet, suggested_wallet, confidence, signals, status, handle, expires_at)
            VALUES ($1, $2, $3, $4, 'PENDING', $5, $6)
            ON CONFLICT (owner_wallet, suggested_wallet) DO UPDATE SET
                confidence = $3, signals = $4, status = 'PENDING', expires_at = $6
            """,
            owner,
            link_w,
            round(confidence, 2),
            signals,
            handle,
            expires_at,
        )
        return {
            "success": True,
            "owner_wallet": owner,
            "link_wallet": link_w,
            "confidence": round(confidence, 2),
            "message": "Suggestion created. Confirm via POST /linking/respond.",
        }
    finally:
        await release_conn(conn)
