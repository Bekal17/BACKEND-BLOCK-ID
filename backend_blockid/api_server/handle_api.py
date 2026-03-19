"""
BlockID Handle Registry API — Phase 2.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from backend_blockid.api_server.handle_antiskwat import (
    check_layer2_behavioral,
    run_anti_squatting_check,
)
from backend_blockid.api_server.signature_verify import verify_or_raise
from backend_blockid.api_server.handle_pricing import (
    get_handle_price,
    validate_handle_format,
)
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

router = APIRouter(prefix="/handle", tags=["Handle Registry"])

HANDLE_MINT_SERVICE_URL = (os.getenv("HANDLE_MINT_SERVICE_URL") or "http://localhost:3001/mint-handle").strip()
HANDLE_METADATA_BASE_URL = (os.getenv("HANDLE_METADATA_BASE_URL") or "https://api.blockidscore.fun/handle").rstrip("/")
CHALLENGE_PERIOD_DAYS = int(os.getenv("CHALLENGE_PERIOD_DAYS", "30").strip() or "30")
MIN_TRUST_SCORE_TO_CHALLENGE = int(os.getenv("MIN_TRUST_SCORE_TO_CHALLENGE", "50").strip() or "50")
MINT_TIMEOUT = float(os.getenv("MINT_TIMEOUT", "30"))
ADMIN_KEY = (os.getenv("ADMIN_KEY") or "").strip()


def _normalize_handle(handle: str) -> str:
    return (handle or "").strip().lstrip("@").lower()


def _require_admin(x_admin_key: str | None = Header(None, alias="X-Admin-Key")) -> None:
    if not ADMIN_KEY:
        raise HTTPException(503, detail="ADMIN_KEY not configured")
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(401, detail="Invalid or missing X-Admin-Key")


# --- Request/Response models ---


class ClaimRequest(BaseModel):
    wallet: str = Field(..., description="Solana wallet address")
    handle: str = Field(..., description="Handle with or without @")
    signed_message: str = Field(..., description="Message signed by wallet")
    signature: str = Field(..., description="Base58 signature")


class LinkWalletRequest(BaseModel):
    handle: str = Field(..., description="Handle with or without @")
    owner_wallet: str = Field(..., description="Primary owner wallet")
    link_wallet: str = Field(..., description="Wallet to link")
    signed_message: str = Field(default="")
    signature: str = Field(default="")


class ChallengeRequest(BaseModel):
    handle: str = Field(..., description="Handle with or without @")
    challenger_wallet: str = Field(..., description="Challenger wallet")
    reason: str = Field(..., description="Reason for challenge")
    evidence: str = Field(default="", description="Optional evidence")


class ReservedRequest(BaseModel):
    handle: str = Field(..., description="Handle without @")
    reserved_for: str = Field(..., description="Public figure or entity name")
    category: str = Field(default="", description="e.g. crypto_founder, influencer")
    can_claim_wallet: str | None = Field(default=None, description="Known wallet that may claim")


# --- GET /handle/price ---


@router.get("/price")
async def get_price(handle: str) -> dict[str, Any]:
    """Return price for claiming a handle."""
    valid, err = validate_handle_format(handle)
    if not valid:
        raise HTTPException(400, detail=err)
    h = _normalize_handle(handle)
    conn = await get_conn()
    try:
        reg = await conn.fetchrow("SELECT 1 FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'", h)
        res = await conn.fetchrow("SELECT 1 FROM handle_reserved WHERE LOWER(handle) = $1", h)
        available = reg is None
        reserved = res is not None
        return {
            "handle": h,
            "price_usd": get_handle_price(h),
            "length": len(h),
            "available": available,
            "reserved": reserved,
        }
    finally:
        await release_conn(conn)


# --- GET /handle/check ---


@router.get("/check")
async def check_handle(handle: str) -> dict[str, Any]:
    """Check handle availability and reserved status."""
    valid, err = validate_handle_format(handle)
    if not valid:
        raise HTTPException(400, detail=err)
    h = _normalize_handle(handle)
    conn = await get_conn()
    try:
        reg = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'",
            h,
        )
        res = await conn.fetchrow("SELECT reserved_for FROM handle_reserved WHERE LOWER(handle) = $1", h)
        available = reg is None
        reserved = res is not None
        return {
            "handle": h,
            "available": available,
            "reserved": reserved,
            "reserved_for": res["reserved_for"] if res else None,
            "price_usd": get_handle_price(h),
            "current_owner": reg["owner_wallet"] if reg else None,
        }
    finally:
        await release_conn(conn)


# --- POST /handle/claim ---


@router.post("/claim")
async def claim_handle(body: ClaimRequest) -> dict[str, Any]:
    """Claim a handle. Runs all 3 anti-squatting layers, then mints Handle NFT and starts 30-day challenge."""
    wallet = (body.wallet or "").strip()
    handle_raw = (body.handle or "").strip()
    valid, err = validate_handle_format(handle_raw)
    if not valid:
        raise HTTPException(400, detail=err)
    h = _normalize_handle(handle_raw)

    # High-value: individual signature required. Message: "Claim @{handle} on BlockID"
    expected_msg = f"Claim @{h} on BlockID"
    if not body.signed_message or (body.signed_message or "").strip() != expected_msg:
        raise HTTPException(400, detail=f"signed_message must be '{expected_msg}'")
    verify_or_raise(wallet, body.signed_message.strip(), body.signature, detail="Invalid claim signature")

    conn = await get_conn()
    try:
        reg = await conn.fetchrow("SELECT 1 FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'", h)
        if reg:
            raise HTTPException(400, detail="Handle is not available")

        price_usd = get_handle_price(h)
        anti = await run_anti_squatting_check(
            wallet, handle_raw, body.signed_message, body.signature, conn
        )
        if not anti["passed"]:
            return {
                "success": False,
                "handle": f"@{h}",
                "failed_layer": anti["failed_layer"],
                "reason": anti["reason"],
                "message": "Handle claim rejected",
            }

        metadata_uri = f"{HANDLE_METADATA_BASE_URL}/{h}"
        try:
            async with httpx.AsyncClient(timeout=MINT_TIMEOUT) as client:
                resp = await client.post(
                    HANDLE_MINT_SERVICE_URL,
                    json={"wallet": wallet, "handle": h, "metadata_uri": metadata_uri},
                )
                if resp.status_code in (503, 502):
                    raise HTTPException(503, detail="Mint service temporarily unavailable")
                resp.raise_for_status()
                data = resp.json()
        except httpx.ConnectError:
            raise HTTPException(503, detail="Mint service temporarily unavailable")
        except httpx.HTTPStatusError as e:
            logger.warning("handle_claim_mint_error", handle=h, status=e.response.status_code)
            raise HTTPException(502, detail="Mint service error")

        mint_address = data.get("mint_address", "")
        # Use naive UTC datetime — asyncpg expects naive UTC for TIMESTAMP WITH TIME ZONE
        now_utc = datetime.utcnow()
        challenge_expires_at = now_utc + timedelta(days=CHALLENGE_PERIOD_DAYS)
        await conn.execute(
            """
            INSERT INTO handle_registry (
                handle, owner_wallet, mint_address, status,
                price_paid_usd, challenge_expires_at, claimed_at, updated_at
            ) VALUES ($1, $2, $3, 'ACTIVE', $4, $5, $6, $6)
            """,
            h,
            wallet,
            mint_address,
            price_usd,
            challenge_expires_at,
            now_utc,
        )
        logger.info("handle_claimed", handle=h, wallet=wallet[:16])
        return {
            "success": True,
            "handle": f"@{h}",
            "mint_address": mint_address,
            "price_usd": price_usd,
            "challenge_expires_at": challenge_expires_at.isoformat(),
            "message": f"Handle @{h} claimed successfully. 30-day challenge period active.",
        }
    finally:
        await release_conn(conn)


# --- POST /handle/link-wallet ---


@router.post("/link-wallet")
async def link_wallet(body: LinkWalletRequest) -> dict[str, Any]:
    """Link additional wallet to handle (opt-in multi-wallet)."""
    h = _normalize_handle(body.handle)
    owner = (body.owner_wallet or "").strip()
    link_w = (body.link_wallet or "").strip()
    if not link_w or len(link_w) < 32:
        raise HTTPException(400, detail="Invalid link_wallet")

    conn = await get_conn()
    try:
        reg = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'",
            h,
        )
        if not reg or (reg["owner_wallet"] or "").strip() != owner:
            raise HTTPException(403, detail="Only the handle owner can link wallets")

        l2 = await check_layer2_behavioral(link_w, h, conn)
        confidence = l2.get("confidence", 0.0)
        if confidence >= 0.6:
            status = "VERIFIED"
        elif confidence >= 0.3:
            status = "PENDING"
        else:
            status = "REJECTED"

        await conn.execute(
            """
            INSERT INTO handle_wallet_links (handle, wallet, is_primary, link_status, ai_confidence, verified_at)
            VALUES ($1, $2, FALSE, $3, $4, $5)
            ON CONFLICT (handle, wallet) DO UPDATE SET
                link_status = $3, ai_confidence = $4, verified_at = $5
            """,
            h,
            link_w,
            status,
            confidence,
            datetime.utcnow() if status == "VERIFIED" else None,
        )
        return {
            "success": status != "REJECTED",
            "handle": f"@{h}",
            "link_wallet": link_w,
            "status": status,
            "ai_confidence": confidence,
            "message": f"Link {status.lower()}." if status != "REJECTED" else "Link rejected (low confidence).",
        }
    finally:
        await release_conn(conn)


# --- POST /handle/challenge ---


@router.post("/challenge")
async def challenge_handle(body: ChallengeRequest) -> dict[str, Any]:
    """Challenge a handle claim (community dispute). Only during 30-day challenge period."""
    h = _normalize_handle(body.handle)
    challenger = (body.challenger_wallet or "").strip()

    conn = await get_conn()
    try:
        reg = await conn.fetchrow(
            "SELECT challenge_expires_at FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'",
            h,
        )
        if not reg:
            raise HTTPException(404, detail="Handle not found")
        expires_at = reg["challenge_expires_at"]
        if not expires_at:
            raise HTTPException(400, detail="Challenge period has ended")
        if getattr(expires_at, "tzinfo", None) is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.utcnow():
            raise HTTPException(400, detail="Challenge period has ended")

        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
            challenger,
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0
        if score < MIN_TRUST_SCORE_TO_CHALLENGE:
            raise HTTPException(403, detail=f"Challenger trust score must be at least {MIN_TRUST_SCORE_TO_CHALLENGE}")

        existing = await conn.fetchval(
            "SELECT 1 FROM handle_challenges WHERE handle = $1 AND challenger_wallet = $2 AND status = 'OPEN'",
            h,
            challenger,
        )
        if existing:
            raise HTTPException(400, detail="You already have an open challenge for this handle")

        await conn.execute(
            """
            INSERT INTO handle_challenges (handle, challenger_wallet, reason, evidence, status, expires_at)
            VALUES ($1, $2, $3, $4, 'OPEN', $5)
            """,
            h,
            challenger,
            body.reason,
            body.evidence or None,
            expires_at,
        )
        row = await conn.fetchrow("SELECT id FROM handle_challenges WHERE handle = $1 AND challenger_wallet = $2 ORDER BY id DESC LIMIT 1", h, challenger)
        challenge_id = row["id"] if row else None
        return {
            "success": True,
            "challenge_id": challenge_id,
            "handle": f"@{h}",
            "expires_at": expires_at.isoformat() if hasattr(expires_at, "isoformat") else str(expires_at),
            "message": "Challenge submitted. BlockID will review within 30 days.",
        }
    finally:
        await release_conn(conn)


# --- GET /handle/{handle}/metadata (NFT URI) — declare before /{handle} ---


@router.get("/{handle}/metadata")
async def get_handle_metadata(handle: str) -> dict[str, Any]:
    """Returns metadata for handle NFT URI."""
    h = _normalize_handle(handle)
    conn = await get_conn()
    try:
        reg = await conn.fetchrow(
            "SELECT owner_wallet, claimed_at FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'",
            h,
        )
        if not reg:
            raise HTTPException(404, detail="Handle not found")
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM handle_wallet_links WHERE handle = $1 AND link_status = 'VERIFIED'",
            h,
        )
        linked_count = int(count or 0)
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
            reg["owner_wallet"],
        )
        trust_score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        claimed_at = reg["claimed_at"]
        claimed_str = claimed_at.strftime("%Y-%m-%d") if claimed_at and hasattr(claimed_at, "strftime") else str(claimed_at or "")
        return {
            "name": f"@{h}",
            "description": "BlockID Handle — Web3 Universal Identity",
            "handle": h,
            "owner_wallet": reg["owner_wallet"],
            "linked_wallets_count": linked_count,
            "trust_score": round(trust_score, 1),
            "claimed_at": claimed_str,
            "image": f"{HANDLE_METADATA_BASE_URL}/{h}/avatar",
        }
    finally:
        await release_conn(conn)


# --- GET /handle/{handle} (public profile) ---


@router.get("/{handle}")
async def get_handle_profile(handle: str) -> dict[str, Any]:
    """Get handle profile — public endpoint."""
    h = _normalize_handle(handle)
    conn = await get_conn()
    try:
        reg = await conn.fetchrow(
            "SELECT owner_wallet, mint_address, claimed_at, linked_wallets FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE'",
            h,
        )
        if not reg:
            raise HTTPException(404, detail="Handle not found")

        owner = reg["owner_wallet"] or ""
        linked = list(reg["linked_wallets"] or [])
        links = await conn.fetch(
            "SELECT wallet FROM handle_wallet_links WHERE handle = $1 AND link_status = 'VERIFIED'",
            h,
        )
        for row in links:
            w = row.get("wallet")
            if w and w not in linked:
                linked.append(w)
        if owner and owner not in linked:
            linked.insert(0, owner)

        ts = await conn.fetchrow(
            "SELECT score, risk_level, metadata_json FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
            owner,
        )
        trust_score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        risk_level = (ts.get("risk_level") or "UNKNOWN") if ts else "UNKNOWN"
        badges = []
        if ts and ts.get("metadata_json"):
            try:
                meta = json.loads(ts["metadata_json"]) if isinstance(ts["metadata_json"], str) else ts["metadata_json"]
                badges = meta.get("badges", [])[:10]
            except Exception:
                pass

        claimed_at = reg["claimed_at"]
        claimed_str = claimed_at.strftime("%Y-%m-%d") if claimed_at and hasattr(claimed_at, "strftime") else str(claimed_at or "")

        return {
            "handle": f"@{h}",
            "owner_wallet": owner,
            "linked_wallets": linked,
            "trust_score": round(trust_score, 1),
            "risk_level": risk_level,
            "badges": badges,
            "is_verified": bool(links),
            "claimed_at": claimed_str,
            "mint_address": reg.get("mint_address"),
        }
    finally:
        await release_conn(conn)


# --- POST /handle/reserved (Admin only) ---


@router.post("/reserved")
async def add_reserved(
    body: ReservedRequest,
    x_admin_key: str | None = Header(None, alias="X-Admin-Key"),
) -> dict[str, Any]:
    """Add handle to reserved list. Requires X-Admin-Key header."""
    _require_admin(x_admin_key)
    h = _normalize_handle(body.handle)
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO handle_reserved (handle, reserved_for, category, can_claim_wallet)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (handle) DO UPDATE SET reserved_for = $2, category = $3, can_claim_wallet = $4
            """,
            h,
            body.reserved_for,
            body.category or None,
            body.can_claim_wallet,
        )
        return {"ok": True, "handle": h, "reserved_for": body.reserved_for}
    finally:
        await release_conn(conn)
