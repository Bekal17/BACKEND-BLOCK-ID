"""JWT session authentication for BlockID."""
from __future__ import annotations

import os
import time

import jwt
from fastapi import HTTPException, Request

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

JWT_SECRET = os.getenv("BLOCKID_JWT_SECRET", "blockid_dev_secret_change_in_prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")


def create_session_token(wallet: str) -> str:
    """Create JWT session token for wallet. Expires in 24 hours."""
    payload = {
        "wallet": wallet,
        "iat": int(time.time()),
        "exp": int(time.time()) + (JWT_EXPIRY_HOURS * 3600),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_session_token(token: str) -> str:
    """
    Verify JWT session token.
    Returns wallet address if valid.
    Raises HTTP 401 if invalid/expired.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload["wallet"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired — please reconnect wallet")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session token")


def get_wallet_from_request(request: Request) -> str:
    """
    Extract and verify wallet from request.
    Checks Authorization header: "Bearer {jwt_token}"
    In DEV mode: also accepts wallet directly from body.
    Returns wallet address.
    """
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.removeprefix("Bearer ").strip()
        return verify_session_token(token)

    # DEV mode fallback
    if BLOCKID_ENV == "DEV":
        return ""  # caller handles dev fallback

    raise HTTPException(status_code=401, detail="Missing Authorization header")
