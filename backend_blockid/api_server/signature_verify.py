"""Ed25519 wallet signature verification for BlockID."""
from __future__ import annotations

import base58
import base64
import os

import nacl.exceptions
import nacl.signing

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")
DEVNET_BYPASS = {"devtest_signature_bypass"}


def verify_wallet_signature(wallet: str, message: str, signature: str) -> bool:
    """
    Verify Ed25519 signature from Solana wallet.
    Returns True if valid, False otherwise.
    DEV mode: accepts "devtest_signature_bypass".
    NOTE: This only proves wallet ownership —
    NO funds are transferred or accessed.
    """
    if BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS:
        return True
    try:
        pubkey_bytes = base58.b58decode(wallet)
        if len(pubkey_bytes) != 32:
            return False
        try:
            sig_bytes = base58.b58decode(signature)
        except Exception:
            sig_bytes = base64.b64decode(signature)
        if len(sig_bytes) != 64:
            return False
        verify_key = nacl.signing.VerifyKey(pubkey_bytes)
        verify_key.verify(message.encode("utf-8"), sig_bytes)
        return True
    except nacl.exceptions.BadSignatureError:
        return False
    except Exception as e:
        logger.warning("signature_verify_error", error=str(e))
        return False


def verify_or_raise(wallet: str, message: str, signature: str, detail: str = "Invalid signature") -> None:
    """Verify signature or raise HTTP 401."""
    from fastapi import HTTPException

    if not verify_wallet_signature(wallet, message, signature):
        raise HTTPException(status_code=401, detail=detail)
