#!/usr/bin/env python3
"""
BlockID on-chain trust score reader. Decodes Anchor TrustScoreAccount PDA data
from Solana devnet using solana-py + solders. Handles all get_account_info() data formats.
"""

import argparse
import base64
import os
import struct
import sys

from dotenv import load_dotenv

parser = argparse.ArgumentParser(description="Read trust score PDA")
parser.add_argument("pda", nargs="?", help="TrustScore PDA pubkey")
args = parser.parse_args()

load_dotenv()

# Layout: 8 discriminator + 32 wallet + 1 score + 1 risk + 8 updated_at (i64 le) = 50 bytes
DISCRIMINATOR_LEN = 8
WALLET_LEN = 32
MIN_ACCOUNT_LEN = DISCRIMINATOR_LEN + WALLET_LEN + 1 + 1 + 8  # 50


def _raw_bytes_from_account_data(data: object) -> bytes | None:
    """
    Extract raw bytes from whatever get_account_info() returned in account.data.
    Handles: bytes, base64 str, list (e.g. ["base64str"]), UiAccountData, list of ints.
    """
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception:
            return data.encode("utf-8") if data.isascii() else None
    if isinstance(data, (list, tuple)):
        if len(data) == 0:
            return None
        first = data[0]
        if isinstance(first, str):
            try:
                return base64.b64decode(first)
            except Exception:
                return None
        if isinstance(first, (list, tuple)):
            return bytes(first) if all(isinstance(x, int) for x in first) else None
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
    if hasattr(data, "__iter__") and not isinstance(data, (str, bytes)):
        try:
            arr = list(data)
            if arr and isinstance(arr[0], int):
                return bytes(arr)
        except Exception:
            pass
    return None


def main() -> int:
    from solana.rpc.api import Client
    from solders.pubkey import Pubkey

    pda = (args.pda or os.getenv("TRUST_SCORE_PDA") or "").strip()
    if not pda:
        print("ERROR: Provide PDA as argument or set TRUST_SCORE_PDA in .env", file=sys.stderr)
        sys.exit(1)
    print("PDA used:", pda)

    pda_pubkey = Pubkey.from_string(pda)
    rpc = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    client = Client(rpc)

    resp = client.get_account_info(pda_pubkey, encoding="base64")
    value = getattr(resp, "value", None) or (getattr(resp, "result", None) and getattr(resp.result, "value", None))
    if value is None:
        print("Account not found")
        return 1

    data = getattr(value, "data", None)
    print("type(resp.value.data) =", type(data).__name__)
    raw = _raw_bytes_from_account_data(data)
    if raw is None:
        print("Could not decode account data to bytes")
        return 1
    print("raw length =", len(raw))

    if len(raw) < MIN_ACCOUNT_LEN:
        print(f"Account data too short: need at least {MIN_ACCOUNT_LEN} bytes, got {len(raw)}")
        return 1

    # Skip 8-byte discriminator; then wallet 32, score u8, risk u8, updated_at i64 le
    raw = raw[DISCRIMINATOR_LEN:]
    wallet_bytes = raw[:WALLET_LEN]
    score = raw[WALLET_LEN] & 0xFF
    risk = raw[WALLET_LEN + 1] & 0xFF
    updated_at = struct.unpack("<q", raw[WALLET_LEN + 2 : WALLET_LEN + 2 + 8])[0]

    wallet_pubkey = Pubkey(wallet_bytes)

    print("wallet:", wallet_pubkey)
    print("score:", score)
    print("risk:", risk)
    print("updated_at:", updated_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
