"""BlockID authentication API — login with wallet signature, returns JWT session."""
from __future__ import annotations

import asyncio
import re
import time

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from solders.pubkey import Pubkey

from backend_blockid.api_server.session_auth import create_session_token
from backend_blockid.oracle.realtime_wallet_pipeline import run_realtime_wallet_pipeline
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.api_server.signature_verify import (
    BLOCKID_ENV,
    DEVNET_BYPASS,
    verify_wallet_signature,
)


async def _sync_sns_handle(wallet: str) -> None:
    """
    Check Bonfida for .sol domains owned by wallet.
    Sync primary domain to social_profiles if:
    - Wallet has no NFT handle in handle_registry
    - Wallet has no .Block handle (handle_type='block')
    If wallet had SNS but no longer owns it, clear the SNS handle.
    This runs as a background task — errors are swallowed.
    """
    try:
        conn = await get_conn()
        try:
            # Check if wallet already has NFT handle — SNS should not override
            nft_row = await conn.fetchrow(
                "SELECT handle FROM handle_registry "
                "WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
                wallet,
            )
            if nft_row and nft_row["handle"]:
                return  # NFT handle takes priority, skip SNS sync

            # Check current handle in social_profiles
            profile_row = await conn.fetchrow(
                "SELECT handle, handle_type FROM social_profiles WHERE wallet = $1",
                wallet,
            )
            current_type = profile_row["handle_type"] if profile_row else None

            # If wallet has .Block handle, do not override with SNS
            if current_type == "block":
                return

            # Fetch SNS domains from Bonfida
            sns_handle = None
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"https://sns-sdk-proxy.bonfida.workers.dev/domains/{wallet}"
                )
                if res.status_code == 200:
                    data = res.json()
                    domains = data.get("result", [])
                    if domains:
                        # Take first domain as primary
                        first = domains[0]
                        name = first if isinstance(first, str) else first.get("domain", "")
                        if name:
                            # Strip .sol suffix for storage (same as NFT handle pattern)
                            sns_handle = name.replace(".sol", "").strip().lower()

            if sns_handle:
                # Check if this SNS handle conflicts with another wallet's NFT or .Block
                conflict_nft = await conn.fetchrow(
                    "SELECT owner_wallet FROM handle_registry "
                    "WHERE handle = $1 AND status = 'ACTIVE' AND owner_wallet != $2 LIMIT 1",
                    sns_handle, wallet,
                )
                if conflict_nft:
                    return  # NFT handle of same name owned by someone else, skip

                # Remove SNS from any other wallet that previously had it
                await conn.execute(
                    """
                    UPDATE social_profiles
                    SET handle = NULL, handle_type = NULL, updated_at = NOW()
                    WHERE handle = $1 AND handle_type = 'sns' AND wallet != $2
                    """,
                    sns_handle, wallet,
                )

                # Also remove .Block conflict (SNS > .Block priority)
                await conn.execute(
                    """
                    UPDATE social_profiles
                    SET handle = NULL, handle_type = NULL, updated_at = NOW()
                    WHERE handle = $1 AND handle_type = 'block' AND wallet != $2
                    """,
                    sns_handle, wallet,
                )

                # Upsert SNS handle to this wallet
                await conn.execute(
                    """
                    INSERT INTO social_profiles (wallet, handle, handle_type, updated_at)
                    VALUES ($1, $2, 'sns', NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                        handle = EXCLUDED.handle,
                        handle_type = 'sns',
                        updated_at = NOW()
                    """,
                    wallet, sns_handle,
                )
            else:
                # Wallet no longer owns any .sol domain
                # If previously had SNS handle, clear it
                if current_type == "sns":
                    await conn.execute(
                        """
                        UPDATE social_profiles
                        SET handle = NULL, handle_type = NULL, updated_at = NOW()
                        WHERE wallet = $1 AND handle_type = 'sns'
                        """,
                        wallet,
                    )
        finally:
            await release_conn(conn)
    except Exception:
        pass  # Background task — never block login


router = APIRouter(prefix="/auth", tags=["Auth"])


class LoginRequest(BaseModel):
    wallet: str
    signed_message: str
    signature: str


class EmbeddedLoginRequest(BaseModel):
    wallet_address: str
    auth_provider: str


_SOLANA_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


@router.post("/login")
async def login(body: LoginRequest, background_tasks: BackgroundTasks):
    """
    Verify wallet signature and return JWT session token.

    Frontend signs: "BlockID Login:{wallet}:{timestamp}"
    where timestamp = Unix seconds (valid window: 5 minutes)

    This is a FREE signature — no funds are transferred.
    This only proves you own this wallet.

    Response:
    {
        "session_token": "eyJ...",
        "wallet": "9hXa...",
        "expires_in": 86400
    }
    """
    wallet = (body.wallet or "").strip()
    if not wallet:
        raise HTTPException(400, detail="wallet required")

    # In DEV mode accept bypass
    if BLOCKID_ENV == "DEV" and body.signature in DEVNET_BYPASS:
        token = create_session_token(wallet)
        background_tasks.add_task(run_realtime_wallet_pipeline, wallet)
        background_tasks.add_task(_sync_sns_handle, wallet)
        return {"session_token": token, "wallet": wallet, "expires_in": 86400}

    # Verify signature
    if not verify_wallet_signature(wallet, body.signed_message, body.signature):
        raise HTTPException(401, detail="Invalid signature")

    # Validate message format and timestamp
    # Expected: "BlockID Login:{wallet}:{timestamp}"
    try:
        parts = body.signed_message.split(":")
        if len(parts) < 3 or parts[0] != "BlockID Login":
            raise HTTPException(401, detail="Invalid message format")
        msg_timestamp = int(parts[-1])
        if abs(time.time() - msg_timestamp) > 300:  # 5 min window
            raise HTTPException(401, detail="Message expired — please try again")
    except (ValueError, IndexError):
        raise HTTPException(401, detail="Invalid message format")

    token = create_session_token(wallet)
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_profiles (wallet, auth_type, updated_at)
            VALUES ($1, 'wallet', NOW())
            ON CONFLICT (wallet) DO UPDATE SET
              updated_at = NOW()
            """,
            wallet,
        )
    finally:
        await release_conn(conn)
    background_tasks.add_task(run_realtime_wallet_pipeline, wallet)
    background_tasks.add_task(_sync_sns_handle, wallet)
    return {
        "session_token": token,
        "wallet": wallet,
        "expires_in": 86400,
        "message": "Login successful. Token valid for 24 hours.",
    }


@router.post("/embedded-login")
async def embedded_login(body: EmbeddedLoginRequest, background_tasks: BackgroundTasks):
    """
    Login/register wallet from embedded auth provider (Google/Apple).
    Creates a JWT session and stores latest session token server-side.
    """
    wallet_address = (body.wallet_address or "").strip()
    auth_provider = (body.auth_provider or "").strip().lower()

    if not wallet_address:
        raise HTTPException(400, detail="wallet_address required")
    if not _SOLANA_BASE58_RE.match(wallet_address):
        raise HTTPException(400, detail="Invalid Solana wallet address format")
    try:
        Pubkey.from_string(wallet_address)
    except Exception:
        raise HTTPException(400, detail="Invalid Solana wallet address")
    if not auth_provider:
        raise HTTPException(400, detail="auth_provider required")

    conn = await get_conn()
    try:
        has_score = await conn.fetchval(
            "SELECT 1 FROM trust_scores WHERE wallet=$1 LIMIT 1",
            wallet_address,
        )
        is_new_user = not bool(has_score)

        await conn.execute(
            """
            INSERT INTO social_profiles (wallet, auth_type, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
              auth_type = EXCLUDED.auth_type,
              updated_at = NOW()
            """,
            wallet_address,
            auth_provider,
        )

        session_token = create_session_token(wallet_address)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_sessions (
                wallet TEXT PRIMARY KEY,
                session_token TEXT NOT NULL,
                auth_provider TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO auth_sessions (wallet, session_token, auth_provider, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                session_token = EXCLUDED.session_token,
                auth_provider = EXCLUDED.auth_provider,
                updated_at = NOW()
            """,
            wallet_address,
            session_token,
            auth_provider,
        )
    finally:
        await release_conn(conn)

    background_tasks.add_task(run_realtime_wallet_pipeline, wallet_address)
    background_tasks.add_task(_sync_sns_handle, wallet_address)

    return {
        "success": True,
        "wallet_address": wallet_address,
        "session_token": session_token,
        "is_new_user": is_new_user,
    }
