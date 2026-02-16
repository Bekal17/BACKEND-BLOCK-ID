#!/usr/bin/env python3
"""
BlockID automatic trust score reader. Input wallet pubkey → derive TrustScore PDA
(Anchor seeds) → read account → decode TrustScoreAccount.
Uses SOLANA_RPC_URL, ORACLE_PROGRAM_ID, ORACLE_PRIVATE_KEY from .env.
"""

from __future__ import annotations

import argparse
import base64
import os
import struct
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__" and not __package__:
    _root = os.path.abspath(os.path.dirname(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)

# TrustScoreAccount: 8 discriminator + 32 wallet + 1 score + 1 risk + 8 updated_at (i64 le)
DISCRIMINATOR_LEN = 8
WALLET_LEN = 32
MIN_ACCOUNT_LEN = DISCRIMINATOR_LEN + WALLET_LEN + 1 + 1 + 8  # 50


def _raw_bytes_from_account_data(data: object) -> bytes | None:
    """Normalize get_account_info() account.data to bytes."""
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception:
            return None
    if isinstance(data, (list, tuple)):
        if not data:
            return None
        first = data[0]
        if isinstance(first, str):
            try:
                return base64.b64decode(first)
            except Exception:
                return None
        if isinstance(first, int):
            return bytes(data)
        return None
    if hasattr(data, "data"):
        inner = getattr(data, "data", None)
        if isinstance(inner, bytes):
            return inner
        if isinstance(inner, str):
            try:
                return base64.b64decode(inner)
            except Exception:
                return None
        if isinstance(inner, (list, tuple)) and len(inner) > 0:
            if isinstance(inner[0], str):
                try:
                    return base64.b64decode(inner[0])
                except Exception:
                    return None
            if isinstance(inner[0], int):
                return bytes(inner)
        return _raw_bytes_from_account_data(inner)
    try:
        if hasattr(data, "__iter__") and not isinstance(data, (str, bytes)):
            arr = list(data)
            if arr and isinstance(arr[0], int):
                return bytes(arr)
    except Exception:
        pass
    return None


def _decode_trust_score_account(raw: bytes) -> tuple[object, int, int, int] | None:
    """Decode TrustScoreAccount: wallet (32), score u8, risk u8, updated_at i64. Returns (wallet_pubkey, score, risk, updated_at) or None."""
    if len(raw) < MIN_ACCOUNT_LEN:
        return None
    from solders.pubkey import Pubkey
    payload = raw[DISCRIMINATOR_LEN:]
    wallet_bytes = payload[:WALLET_LEN]
    score = payload[WALLET_LEN] & 0xFF
    risk = payload[WALLET_LEN + 1] & 0xFF
    updated_at = struct.unpack("<q", payload[WALLET_LEN + 2 : WALLET_LEN + 2 + 8])[0]
    wallet_pubkey = Pubkey(wallet_bytes)
    return (wallet_pubkey, score, risk, updated_at)


def _format_updated_at(ts: int) -> str:
    """Format updated_at as YYYY-MM-DD HH:MM:SS UTC."""
    if ts == 0:
        return "1970-01-01 00:00:00 UTC"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read BlockID trust score for a wallet (derives PDA from oracle + wallet).")
    parser.add_argument("wallet", help="Wallet pubkey")
    args = parser.parse_args()

    wallet_str = (args.wallet or "").strip()
    if not wallet_str:
        print("ERROR: Invalid wallet (empty)", file=sys.stderr)
        sys.exit(1)

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key or not program_id_str:
        print("ERROR: Set SOLANA_RPC_URL, ORACLE_PROGRAM_ID, and ORACLE_PRIVATE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    from solana.rpc.api import Client
    from solders.pubkey import Pubkey

    try:
        wallet_pubkey = Pubkey.from_string(wallet_str)
    except Exception as e:
        print("ERROR: Invalid wallet:", e, file=sys.stderr)
        sys.exit(1)

    try:
        from backend_blockid.oracle.solana_publisher import _load_keypair, get_trust_score_pda
        keypair = _load_keypair(oracle_key)
        oracle_pubkey = keypair.pubkey()
    except Exception as e:
        print("ERROR: Failed to load oracle keypair:", e, file=sys.stderr)
        sys.exit(1)

    program_id = Pubkey.from_string(program_id_str)
    # Same seeds as Anchor: [b"trust_score", oracle.key(), wallet.key()]
    pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    print("Wallet target:", wallet_str)
    print("Derived PDA:", pda)

    client = Client(rpc_url)
    try:
        resp = client.get_account_info(pda, encoding="base64")
    except Exception as e:
        print("ERROR: RPC error:", e, file=sys.stderr)
        sys.exit(1)

    value = getattr(resp, "value", None) or (getattr(resp, "result", None) and getattr(resp.result, "value", None))
    if value is None:
        print("ERROR: Account not found (no trust score on-chain for this wallet)", file=sys.stderr)
        sys.exit(1)

    data = getattr(value, "data", None)
    raw = _raw_bytes_from_account_data(data)
    if raw is None:
        print("ERROR: Could not decode account data", file=sys.stderr)
        sys.exit(1)

    decoded = _decode_trust_score_account(raw)
    if decoded is None:
        print("ERROR: Account data too short or invalid layout", file=sys.stderr)
        sys.exit(1)

    stored_wallet, score, risk, updated_at = decoded
    print("Stored wallet:", stored_wallet)
    print("Score:", score)
    print("Risk:", risk)
    print("Updated at:", _format_updated_at(updated_at))
    return 0


if __name__ == "__main__":
    sys.exit(main())
