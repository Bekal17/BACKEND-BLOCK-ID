"""
BlockID Handle Registry API — Phase 2.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Query
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
from backend_blockid.api_server.nft_mint_api import verify_payment_tx
from backend_blockid.api_server.session_auth import verify_session_token
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

RESERVED_HANDLES = {
    # Layer 1 & Chain
    "solana", "sol", "ethereum", "bitcoin", "btc", "eth", "polygon",
    "avalanche", "near", "cosmos", "aptos", "sui", "base", "arbitrum",
    "optimism", "ton", "tron", "bnb", "bsc",
    # DeFi & DEX
    "jupiter", "jup", "raydium", "ray", "orca", "drift", "mango",
    "kamino", "meteora", "marinade", "jito", "sanctum", "lifinity",
    "solend", "marginfi", "save", "tulip", "francium", "hawksight",
    "hubble", "port", "lulo", "credix", "maple", "jet", "sunny",
    "saber", "mercurial", "cashio", "atrix", "serum", "openbook",
    "zeta", "cypher", "parcl", "hxro", "entropy",
    # Wallet & Identity
    "phantom", "backpack", "solflare", "glow", "exodus", "ledger",
    "trezor", "brave", "metamask", "trustwallet", "coin98", "slope",
    "safepal", "xdefi", "tiplink", "squads", "sns", "bonfida",
    # NFT & Marketplace
    "magiceden", "tensor", "solanart", "mallow", "formfunction",
    "holaplex", "digitaleyes", "coral", "metaplex", "underdog",
    "cardinal", "degods", "okay", "bears", "smb", "monkedao",
    "aurory", "degenape", "primates", "claynosaurz", "stepn", "portals",
    # Infrastructure & Tools
    "helius", "quicknode", "alchemy", "triton", "chainstack", "pyth",
    "switchboard", "wormhole", "layerzero", "allbridge", "debridge",
    "celer", "socket", "rango", "clockwork", "genesysgo", "shadow",
    "neon", "realms", "dialect", "blink", "actions", "superteam",
    "colosseum", "solscan", "solanaexplorer", "explorer", "solanafm",
    "birdeye", "dexscreener", "bubblemaps", "rugcheck", "cyclops", "daemon",
    # Meme & Token
    "bonk", "wif", "dogwifhat", "popcat", "myro", "samo", "bome",
    "bookofmeme", "ponke", "slerf", "ai16z", "goat", "moodeng",
    "trump", "melania", "fartcoin", "retardio", "harambe", "wen",
    # Tokoh / Founder
    "anatoly", "toly", "raj", "tristan", "armani", "noah",
    "mert", "weremeow", "sbf", "ryan", "tyler", "elon",
    "vitalik", "satoshi", "cz", "hayden", "stani",
    # BlockID Internal & System
    "blockid", "sage", "admin", "support", "system", "api",
    "null", "undefined", "root", "me", "you", "user", "bot",
    "ai", "gpt", "claude", "openai", "anthropic", "help", "info",
    "team", "staff", "official", "verified", "test", "dev",
    "staging", "prod", "anonymous", "unknown", "wallet", "score",
    "trust", "identity", "nft", "token", "badge", "og",
    "moderator", "mod", "owner", "founder", "contact", "security",
    "abuse", "report", "feedback", "legal", "privacy", "tos", "terms",
}


def validate_block_handle(handle: str) -> str:
    """
    Validate and normalize a .Block handle.
    Returns normalized handle string or raises HTTPException with error code.
    Frontend handles all display messages via i18n error codes.
    """
    h = handle.lower().strip()
    if not h:
        raise HTTPException(
            status_code=400,
            detail={"code": "HANDLE_INVALID_FORMAT"}
        )
    if not re.match(r'^[a-z0-9_]{3,20}$', h):
        raise HTTPException(
            status_code=400,
            detail={"code": "HANDLE_INVALID_FORMAT"}
        )
    if h in RESERVED_HANDLES:
        raise HTTPException(
            status_code=409,
            detail={"code": "HANDLE_RESERVED", "handle": h}
        )
    return h


logger = get_logger(__name__)

router = APIRouter(prefix="/handle", tags=["Handle Registry"])

HANDLE_MINT_SERVICE_URL = (os.getenv("HANDLE_MINT_SERVICE_URL") or "http://localhost:3001/mint-handle").strip()
HANDLE_METADATA_BASE_URL = (os.getenv("HANDLE_METADATA_BASE_URL") or "https://api.blockidscore.fun/handle").rstrip("/")
CHALLENGE_PERIOD_DAYS = int(os.getenv("CHALLENGE_PERIOD_DAYS", "30").strip() or "30")
MIN_TRUST_SCORE_TO_CHALLENGE = int(os.getenv("MIN_TRUST_SCORE_TO_CHALLENGE", "50").strip() or "50")
MINT_TIMEOUT = float(os.getenv("MINT_TIMEOUT", "30"))
ADMIN_KEY = (os.getenv("ADMIN_KEY") or "").strip()
FOUNDER_WALLETS = {
    w.strip()
    for w in (os.getenv("FOUNDER_WALLETS") or "").split(",")
    if w.strip()
}


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
    signature: str = Field(default="", description="Base58 signature")
    tx_signature: str = Field(default="", description="On-chain payment transaction signature")
    payment_method: str = "SOL"


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

    is_founder = wallet in FOUNDER_WALLETS
    if not is_founder:
        # High-value: individual signature required.
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

        await verify_payment_tx(body.tx_signature, wallet, body.payment_method)

        conn2 = await get_conn()
        try:
            await conn2.execute(
                """
                INSERT INTO nft_mint_payments (wallet, tx_signature, amount_sol)
                VALUES ($1, $2, $3)
                ON CONFLICT (tx_signature) DO NOTHING
                """,
                wallet,
                body.tx_signature,
                0.0,
            )
        finally:
            await release_conn(conn2)

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
        # Sync handle to social_profiles
        try:
            await conn.execute(
                """
                INSERT INTO social_profiles (wallet, handle, handle_type, created_at, updated_at)
                VALUES ($1, $2, 'nft', $3, $3)
                ON CONFLICT (wallet) DO UPDATE SET
                    handle = EXCLUDED.handle,
                    handle_type = 'nft',
                    updated_at = EXCLUDED.updated_at
                """,
                wallet,
                h,
                now_utc,
            )
        except Exception as e:
            logger.warning("handle_social_profile_sync_failed", handle=h, error=str(e))

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


@router.get("/block/check/{handle}")
async def check_block_handle(handle: str):
    """Check if a .Block handle is available."""
    try:
        h = validate_block_handle(handle)
    except HTTPException as e:
        return {"available": False, "code": e.detail.get("code")}

    conn = await get_conn()
    try:
        nft_row = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry WHERE handle = $1 AND status = 'ACTIVE'",
            h
        )
        if nft_row:
            return {"available": False, "code": "HANDLE_TAKEN_NFT", "handle": h}

        block_row = await conn.fetchrow(
            "SELECT wallet FROM social_profiles WHERE handle = $1 AND handle_type = 'block'",
            h
        )
        if block_row:
            return {"available": False, "code": "HANDLE_TAKEN_BLOCK", "handle": h}

        return {"available": True, "handle": h}
    finally:
        await release_conn(conn)


@router.get("/search")
async def search_handles(
    q: str = Query("", min_length=1, max_length=20),
    wallet: str = Query(""),
    limit: int = Query(5, ge=1, le=10),
):
    """
    Search handles for @mention autocomplete.
    Returns handles matching prefix q.
    Following users appear first.
    Only returns users with a handle (nft, sns, or block).
    """
    q = q.strip().lower().lstrip("@")
    if not q:
        return {"results": []}

    wallet = (wallet or "").strip()

    conn = await get_conn()
    try:
        # Get following wallets for priority sorting
        following_wallets: set[str] = set()
        if wallet:
            follow_rows = await conn.fetch(
                "SELECT following_wallet FROM social_follows WHERE follower_wallet = $1",
                wallet,
            )
            following_wallets = {r["following_wallet"] for r in follow_rows}

        # Search NFT handles from handle_registry
        nft_rows = await conn.fetch(
            """
            SELECT
                hr.owner_wallet AS wallet,
                hr.handle,
                'nft' AS handle_type,
                COALESCE(ts.score, 0) AS trust_score,
                sp.avatar_url,
                sp.avatar_type,
                sp.avatar_is_animated
            FROM handle_registry hr
            LEFT JOIN trust_scores ts ON ts.wallet = hr.owner_wallet
            LEFT JOIN social_profiles sp ON sp.wallet = hr.owner_wallet
            WHERE hr.status = 'ACTIVE'
              AND LOWER(hr.handle) LIKE $1
            LIMIT $2
            """,
            f"{q}%",
            limit * 2,
        )

        # Search SNS and .Block handles from social_profiles
        sp_rows = await conn.fetch(
            """
            SELECT
                sp.wallet,
                sp.handle,
                sp.handle_type,
                COALESCE(ts.score, 0) AS trust_score,
                sp.avatar_url,
                sp.avatar_type,
                sp.avatar_is_animated
            FROM social_profiles sp
            LEFT JOIN trust_scores ts ON ts.wallet = sp.wallet
            WHERE sp.handle IS NOT NULL
              AND sp.handle_type IN ('sns', 'block')
              AND sp.handle_release_at IS NULL
              AND LOWER(sp.handle) LIKE $1
            LIMIT $2
            """,
            f"{q}%",
            limit * 2,
        )

        # Merge and deduplicate by wallet (NFT takes priority)
        seen_wallets: set[str] = set()
        seen_handles: set[str] = set()
        results = []

        all_rows = list(nft_rows) + list(sp_rows)

        # Sort: following first, then by trust_score desc
        def sort_key(r: dict) -> tuple:
            is_following = r["wallet"] in following_wallets
            return (not is_following, -(r["trust_score"] or 0))

        all_dicts = [dict(r) for r in all_rows]
        all_dicts.sort(key=sort_key)

        for r in all_dicts:
            w = r["wallet"]
            h = r["handle"]
            if w in seen_wallets or h in seen_handles:
                continue
            seen_wallets.add(w)
            seen_handles.add(h)

            # Build display handle with suffix
            ht = r["handle_type"]
            if ht == "block":
                display = f"@{h}.Block"
            elif ht == "sns":
                display = f"@{h}.sol"
            else:
                display = f"@{h}"

            results.append({
                "wallet": w,
                "handle": h,
                "handle_type": ht,
                "display": display,
                "trust_score": round(float(r["trust_score"] or 0), 1),
                "avatar_url": r.get("avatar_url"),
                "avatar_type": r.get("avatar_type"),
                "avatar_is_animated": r.get("avatar_is_animated", False),
                "is_following": w in following_wallets,
            })

            if len(results) >= limit:
                break

        return {"results": results}
    finally:
        await release_conn(conn)


class ClaimBlockHandleRequest(BaseModel):
    wallet: str
    handle: str
    signature: str = ""
    session_token: str = ""


@router.post("/block/claim")
async def claim_block_handle(request: ClaimBlockHandleRequest):
    """Claim a free @handle.Block."""
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(
            status_code=400,
            detail={"code": "WALLET_REQUIRED"}
        )

    if request.session_token:
        verified_wallet = verify_session_token(request.session_token)
        if verified_wallet != wallet:
            raise HTTPException(
                status_code=401,
                detail={"code": "UNAUTHORIZED"}
            )
    else:
        raise HTTPException(
            status_code=401,
            detail={"code": "SESSION_REQUIRED"}
        )

    h = validate_block_handle(request.handle)

    conn = await get_conn()
    try:
        # Check if wallet already has any handle
        existing = await conn.fetchrow(
            "SELECT handle, handle_type, handle_release_at FROM social_profiles WHERE wallet = $1",
            wallet
        )
        if existing and existing["handle"]:
            existing_type = existing["handle_type"]
            existing_handle = existing["handle"]
            existing_release_at = existing.get("handle_release_at")

            # Check if in 48h cooldown
            if existing_release_at:
                now = datetime.now(timezone.utc)
                if existing_release_at.tzinfo is None:
                    existing_release_at = existing_release_at.replace(tzinfo=timezone.utc)
                if now - existing_release_at < timedelta(hours=48):
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "WALLET_HAS_BLOCK_HANDLE", "existing": existing_handle}
                    )
                else:
                    # 48h passed — auto-clear and allow claim
                    await conn.execute(
                        """
                        UPDATE social_profiles
                        SET handle = NULL,
                            handle_type = NULL,
                            handle_release_at = NULL,
                            updated_at = NOW()
                        WHERE wallet = $1
                        """,
                        wallet,
                    )
            elif existing_type == "nft":
                raise HTTPException(
                    status_code=409,
                    detail={"code": "WALLET_HAS_NFT_HANDLE", "existing": existing_handle}
                )
            else:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "WALLET_HAS_BLOCK_HANDLE", "existing": existing_handle}
                )

        # Check handle not taken in handle_registry (NFT)
        nft_conflict = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry WHERE handle = $1 AND status = 'ACTIVE'",
            h
        )
        if nft_conflict:
            raise HTTPException(
                status_code=409,
                detail={"code": "HANDLE_TAKEN_NFT", "handle": h}
            )

        # Check handle not taken in social_profiles (.Block)
        block_conflict = await conn.fetchrow(
            "SELECT wallet FROM social_profiles WHERE handle = $1 AND handle_type = 'block'",
            h
        )
        if block_conflict:
            raise HTTPException(
                status_code=409,
                detail={"code": "HANDLE_TAKEN_BLOCK", "handle": h}
            )

        now_utc = datetime.utcnow()

        await conn.execute(
            """
            INSERT INTO social_profiles (wallet, handle, handle_type, created_at, updated_at)
            VALUES ($1, $2, 'block', $3, $3)
            ON CONFLICT (wallet) DO UPDATE SET
                handle = EXCLUDED.handle,
                handle_type = 'block',
                updated_at = EXCLUDED.updated_at
            """,
            wallet,
            h,
            now_utc,
        )

        logger.info("block_handle_claimed", handle=h, wallet=wallet[:16])

        return {
            "success": True,
            "handle": h,
            "display": f"@{h}.Block",
            "wallet": wallet,
            "handle_type": "block",
        }
    finally:
        await release_conn(conn)


class ReleaseBlockHandleRequest(BaseModel):
    wallet: str
    session_token: str = ""


@router.delete("/block/release")
async def release_block_handle(request: ReleaseBlockHandleRequest):
    """Release (delete) a free @handle.Block handle from a wallet."""
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(
            status_code=400,
            detail={"code": "WALLET_REQUIRED"}
        )

    if request.session_token:
        verified_wallet = verify_session_token(request.session_token)
        if verified_wallet != wallet:
            raise HTTPException(
                status_code=401,
                detail={"code": "UNAUTHORIZED"}
            )
    else:
        raise HTTPException(
            status_code=401,
            detail={"code": "SESSION_REQUIRED"}
        )

    conn = await get_conn()
    try:
        existing = await conn.fetchrow(
            "SELECT handle, handle_type FROM social_profiles WHERE wallet = $1",
            wallet
        )

        if not existing or not existing["handle"]:
            raise HTTPException(
                status_code=404,
                detail={"code": "NO_HANDLE_FOUND"}
            )

        if existing["handle_type"] != "block":
            raise HTTPException(
                status_code=403,
                detail={"code": "CANNOT_RELEASE_NFT_HANDLE"}
            )

        released_handle = existing["handle"]

        await conn.execute(
            """
            UPDATE social_profiles
            SET handle_release_at = NOW(),
                updated_at = NOW()
            WHERE wallet = $1
            """,
            wallet,
        )

        logger.info("block_handle_released", handle=released_handle, wallet=wallet[:16])

        release_time = datetime.now(timezone.utc)
        return {
            "success": True,
            "released_handle": released_handle,
            "handle_release_at": release_time.isoformat(),
            "wallet": wallet,
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
