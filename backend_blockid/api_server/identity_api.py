"""
BlockID Identity NFT API — Phase 1.

Mint soul-bound Identity NFT, get metadata, check eligibility.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from backend_blockid.api_server.identity_eligibility import check_eligibility
from backend_blockid.api_server.identity_metadata import build_metadata
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.oracle.realtime_wallet_pipeline import run_realtime_wallet_pipeline

logger = get_logger(__name__)

router = APIRouter(prefix="/identity", tags=["Identity NFT"])

MINT_SERVICE_URL = (os.getenv("MINT_SERVICE_URL") or "http://localhost:3001").strip()
METADATA_BASE_URL = (os.getenv("METADATA_BASE_URL") or "https://api.blockidscore.fun/identity").rstrip("/")
MINT_TIMEOUT = float(os.getenv("MINT_TIMEOUT", "30"))
ADMIN_KEY = (os.getenv("ADMIN_KEY") or "").strip()


class MintRequest(BaseModel):
    """POST /identity/mint request body."""

    wallet: str = Field(..., description="Solana wallet address")


def _require_admin(
    x_admin_key: str | None = Header(None, alias="X-Admin-Key"),
) -> None:
    """Require X-Admin-Key header to match ADMIN_KEY env. Admin-only endpoints."""
    if not ADMIN_KEY:
        raise HTTPException(503, detail="ADMIN_KEY not configured")
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(401, detail="Invalid or missing X-Admin-Key")


async def _ensure_identity_nft_table(conn) -> None:
    """Ensure identity_nft table exists (run migration)."""
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS identity_nft (
                id SERIAL PRIMARY KEY,
                wallet TEXT NOT NULL UNIQUE,
                mint_address TEXT UNIQUE,
                token_id TEXT UNIQUE,
                handle TEXT UNIQUE,
                trust_score DOUBLE PRECISION,
                risk_level TEXT,
                badges TEXT,
                wallet_age_days INTEGER,
                behavioral_fingerprint TEXT,
                is_sanctioned BOOLEAN DEFAULT FALSE,
                daemon_risk_score INTEGER,
                daemon_risk_level TEXT,
                mint_status TEXT DEFAULT 'PENDING',
                ineligible_reason TEXT,
                minted_at TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass


async def _get_top_positive_reasons(conn, wallet: str, limit: int = 5) -> list[str]:
    """Get top positive reason codes by weight."""
    rows = await conn.fetch(
        """
        SELECT reason_code
        FROM wallet_reasons
        WHERE wallet = $1 AND reason_code IS NOT NULL AND weight > 0
        ORDER BY weight DESC
        LIMIT $2
        """,
        wallet,
        limit,
    )
    return [r["reason_code"] for r in rows if r.get("reason_code")]


async def _get_trust_score_row(conn, wallet: str) -> dict | None:
    """Get latest trust_scores row for wallet. Enriches wallet_age_days from wallet_meta if trust_scores has 0."""
    row = await conn.fetchrow(
        """
        SELECT score, risk_level, wallet_age_days, metadata_json
        FROM trust_scores
        WHERE wallet = $1
        ORDER BY computed_at DESC NULLS LAST, last_updated DESC NULLS LAST
        LIMIT 1
        """,
        wallet,
    )
    out = dict(row) if row else None
    if out and (int(out.get("wallet_age_days") or 0) == 0):
        try:
            meta = await conn.fetchrow(
                "SELECT wallet_age_days FROM wallet_meta WHERE wallet = $1",
                wallet,
            )
            if meta and (meta.get("wallet_age_days") or 0) > 0:
                out["wallet_age_days"] = int(meta["wallet_age_days"])
        except Exception:
            pass  # wallet_meta may not exist
    return out


@router.delete("/burn")
async def burn_identity_record(
    wallet: str,
    x_admin_key: str | None = Header(None, alias="X-Admin-Key"),
) -> dict[str, Any]:
    """
    Delete identity_nft record for a wallet (admin only). For resetting test mints during development.
    Requires header: X-Admin-Key matching ADMIN_KEY env variable.
    """
    _require_admin(x_admin_key)
    wallet = (wallet or "").strip()
    if len(wallet) < 32 or len(wallet) > 44:
        raise HTTPException(400, detail="Invalid wallet address")
    conn = await get_conn()
    try:
        await _ensure_identity_nft_table(conn)
        result = await conn.execute(
            "DELETE FROM identity_nft WHERE wallet = $1",
            wallet,
        )
        if result == "DELETE 0":
            raise HTTPException(404, detail="Identity record not found")
        logger.info("identity_burn", wallet=wallet[:16])
        return {"ok": True, "wallet": wallet, "message": "Identity record deleted"}
    finally:
        await release_conn(conn)


@router.get("/{wallet}/eligibility")
async def get_eligibility(wallet: str) -> dict[str, Any]:
    """Check if wallet is eligible to mint."""
    wallet = (wallet or "").strip()
    conn = await get_conn()
    try:
        await _ensure_identity_nft_table(conn)
        elig = await check_eligibility(wallet, conn)
        return {
            "wallet": wallet,
            "eligible": elig["eligible"],
            "reasons": [elig["reason"]] if elig["reason"] else [],
            "trust_score": elig["trust_score"],
            "risk_level": elig["risk_level"],
        }
    finally:
        await release_conn(conn)


@router.get("/{wallet}/status")
async def get_mint_status(wallet: str) -> dict[str, Any]:
    """Check mint status for a wallet."""
    wallet = (wallet or "").strip()
    conn = await get_conn()
    try:
        await _ensure_identity_nft_table(conn)
        row = await conn.fetchrow(
            """
            SELECT mint_status, mint_address, minted_at
            FROM identity_nft
            WHERE wallet = $1
            """,
            wallet,
        )
        if not row:
            return {
                "wallet": wallet,
                "status": "NOT_FOUND",
                "mint_address": None,
                "minted_at": None,
            }

        status = (row.get("mint_status") or "PENDING").upper()
        minted_at = row.get("minted_at")
        minted_at_str = None
        if minted_at and hasattr(minted_at, "isoformat"):
            minted_at_str = minted_at.isoformat() + "Z" if "Z" not in minted_at.isoformat() else minted_at.isoformat()
        elif minted_at:
            minted_at_str = str(minted_at)

        return {
            "wallet": wallet,
            "status": status,
            "mint_address": row.get("mint_address"),
            "minted_at": minted_at_str,
        }
    finally:
        await release_conn(conn)


@router.post("/mint")
async def mint_identity_nft(body: MintRequest) -> dict[str, Any]:
    """
    Mint soul-bound BlockID Identity NFT.

    Eligibility: tx_count >= 1, risk_level not HIGH, not already minted.
    If wallet not scored, runs realtime_wallet_pipeline first.
    """
    wallet = (body.wallet or "").strip()
    if len(wallet) < 32 or len(wallet) > 44:
        raise HTTPException(400, detail="Invalid wallet address")

    conn = await get_conn()
    try:
        await _ensure_identity_nft_table(conn)

        # Upsert PENDING row
        await conn.execute(
            """
            INSERT INTO identity_nft (wallet, mint_status, last_updated, created_at)
            VALUES ($1, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (wallet) DO UPDATE SET
                mint_status = CASE
                    WHEN identity_nft.mint_status = 'MINTED' THEN identity_nft.mint_status
                    ELSE 'PENDING'
                END,
                last_updated = CURRENT_TIMESTAMP
            """,
            wallet,
        )

        # Check eligibility (may need pipeline first)
        elig = await check_eligibility(wallet, conn)

        if elig["already_minted"]:
            row = await conn.fetchrow(
                "SELECT mint_address, trust_score, risk_level, badges FROM identity_nft WHERE wallet = $1",
                wallet,
            )
            badges_raw = row.get("badges") or "[]"
            badges = json.loads(badges_raw) if isinstance(badges_raw, str) else (badges_raw or [])
            return {
                "success": False,
                "wallet": wallet,
                "reason": "ALREADY_MINTED",
                "message": "This wallet already has a BlockID Identity NFT",
                "mint_address": row.get("mint_address"),
            }

        if not elig["eligible"]:
            reason = elig["reason"] or "UNKNOWN"
            await conn.execute(
                """
                UPDATE identity_nft SET
                    mint_status = 'INELIGIBLE',
                    ineligible_reason = $2,
                    trust_score = $3,
                    risk_level = $4,
                    last_updated = CURRENT_TIMESTAMP
                WHERE wallet = $1
                """,
                wallet,
                reason,
                elig["trust_score"],
                elig["risk_level"],
            )
            logger.info("identity_mint_ineligible", wallet=wallet[:16], reason=reason)
            return {
                "success": False,
                "wallet": wallet,
                "reason": reason,
                "message": "This wallet is not eligible for BlockID Identity NFT",
            }

        # Wallet not in trust_scores → run pipeline
        ts_row = await _get_trust_score_row(conn, wallet)
        if not ts_row:
            logger.info("identity_mint_run_pipeline", wallet=wallet[:16])
            await release_conn(conn)
            try:
                await run_realtime_wallet_pipeline(wallet)
            except Exception as e:
                logger.warning("identity_mint_pipeline_failed", wallet=wallet[:16], error=str(e))
                raise HTTPException(503, detail="Wallet scoring failed. Please try again.")
            conn = await get_conn()
            elig = await check_eligibility(wallet, conn)
            if not elig["eligible"]:
                return {
                    "success": False,
                    "wallet": wallet,
                    "reason": elig["reason"] or "UNKNOWN",
                    "message": "This wallet is not eligible for BlockID Identity NFT",
                }
            ts_row = await _get_trust_score_row(conn, wallet) or {}

        # Build metadata and badges
        reasons = await _get_top_positive_reasons(conn, wallet)
        metadata = build_metadata(wallet, ts_row or {}, reasons)
        badges = metadata.get("badges", [])[:5]
        metadata_uri = f"{METADATA_BASE_URL}/{wallet}"

        # Call mint service
        try:
            async with httpx.AsyncClient(timeout=MINT_TIMEOUT) as client:
                resp = await client.post(
                    f"{MINT_SERVICE_URL}/mint",
                    json={"wallet": wallet, "metadata_uri": metadata_uri},
                )
                if resp.status_code == 503:
                    raise HTTPException(503, detail="Mint service temporarily unavailable")
                resp.raise_for_status()
                data = resp.json()
        except httpx.ConnectError as e:
            logger.warning("identity_mint_service_unavailable", wallet=wallet[:16], error=str(e))
            raise HTTPException(503, detail="Mint service temporarily unavailable")
        except httpx.HTTPStatusError as e:
            logger.warning("identity_mint_http_error", wallet=wallet[:16], status=e.response.status_code)
            await conn.execute(
                """
                UPDATE identity_nft SET
                    mint_status = 'FAILED',
                    ineligible_reason = $2,
                    last_updated = CURRENT_TIMESTAMP
                WHERE wallet = $1
                """,
                wallet,
                f"mint_http_error:{e.response.status_code}",
            )
            raise HTTPException(502, detail="Mint service error")

        mint_address = data.get("mint_address", "")
        signature = data.get("signature", "")

        # Store success
        await conn.execute(
            """
            UPDATE identity_nft SET
                mint_address = $2,
                token_id = $2,
                mint_status = 'MINTED',
                trust_score = $3,
                risk_level = $4,
                badges = $5,
                wallet_age_days = $6,
                behavioral_fingerprint = $7,
                is_sanctioned = $8,
                daemon_risk_score = $9,
                daemon_risk_level = $10,
                minted_at = CURRENT_TIMESTAMP,
                ineligible_reason = NULL,
                last_updated = CURRENT_TIMESTAMP
            WHERE wallet = $1
            """,
            wallet,
            mint_address,
            metadata.get("trust_score"),
            metadata.get("risk_level"),
            json.dumps(badges),
            metadata.get("wallet_age_days"),
            metadata.get("behavioral_fingerprint"),
            metadata.get("is_sanctioned", False),
            metadata.get("daemon_risk_score"),
            metadata.get("daemon_risk_level"),
        )

        logger.info("identity_mint_success", wallet=wallet[:16], mint=mint_address[:16])
        return {
            "success": True,
            "wallet": wallet,
            "mint_address": mint_address,
            "metadata_uri": metadata_uri,
            "trust_score": metadata.get("trust_score"),
            "risk_level": metadata.get("risk_level"),
            "badges": badges,
            "message": "BlockID Identity NFT minted successfully",
        }
    finally:
        await release_conn(conn)


@router.get("/{wallet}")
async def get_identity_metadata(wallet: str) -> dict[str, Any]:
    """
    Get Identity NFT metadata for a wallet (metadata URI endpoint).
    """
    wallet = (wallet or "").strip()
    conn = await get_conn()
    try:
        await _ensure_identity_nft_table(conn)
        row = await conn.fetchrow(
            """
            SELECT wallet, token_id, trust_score, risk_level, handle, badges,
                   wallet_age_days, behavioral_fingerprint, is_sanctioned,
                   daemon_risk_score, daemon_risk_level, last_updated
            FROM identity_nft
            WHERE wallet = $1 AND mint_status = 'MINTED'
            """,
            wallet,
        )
        if not row:
            raise HTTPException(404, detail="Identity NFT not found")

        badges_raw = row.get("badges") or "[]"
        badges = json.loads(badges_raw) if isinstance(badges_raw, str) else (badges_raw or [])
        last_updated = row.get("last_updated")
        last_updated_str = (
            last_updated.strftime("%Y-%m-%d") if hasattr(last_updated, "strftime") else str(last_updated or "")
        )

        wallet_age_days = int(row.get("wallet_age_days") or 0)
        if wallet_age_days == 0:
            try:
                meta = await conn.fetchrow(
                    "SELECT wallet_age_days FROM wallet_meta WHERE wallet = $1",
                    wallet,
                )
                if meta and (meta.get("wallet_age_days") or 0) > 0:
                    wallet_age_days = int(meta["wallet_age_days"])
            except Exception:
                pass  # wallet_meta may not exist

        return {
            "wallet": row["wallet"],
            "token_id": row.get("token_id"),
            "trust_score": float(row.get("trust_score") or 0),
            "risk_level": row.get("risk_level") or "",
            "handle": row.get("handle"),
            "badges": badges,
            "wallet_age_days": wallet_age_days,
            "behavioral_fingerprint": row.get("behavioral_fingerprint") or "",
            "is_sanctioned": bool(row.get("is_sanctioned") or False),
            "daemon_risk_score": row.get("daemon_risk_score"),
            "daemon_risk_level": row.get("daemon_risk_level"),
            "last_updated": last_updated_str,
        }
    finally:
        await release_conn(conn)
