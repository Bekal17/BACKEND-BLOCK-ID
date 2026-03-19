"""BlockID authentication API — login with wallet signature, returns JWT session."""
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend_blockid.api_server.session_auth import create_session_token
from backend_blockid.api_server.signature_verify import (
    BLOCKID_ENV,
    DEVNET_BYPASS,
    verify_wallet_signature,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


class LoginRequest(BaseModel):
    wallet: str
    signed_message: str
    signature: str


@router.post("/login")
async def login(body: LoginRequest):
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
    return {
        "session_token": token,
        "wallet": wallet,
        "expires_in": 86400,
        "message": "Login successful. Token valid for 24 hours.",
    }
