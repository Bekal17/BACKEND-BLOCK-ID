#!/usr/bin/env python3
"""
Read wallets.csv (headers: wallet, score). For each wallet, derive TrustScore PDA,
read on-chain account, decode score. Compare CSV score vs on-chain score.
Prints per-wallet MATCH/MISMATCH and a summary.
"""

from __future__ import annotations

import argparse
import base64
import csv
import os
import struct
import sys

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


def _decode_on_chain_score(raw: bytes) -> int | None:
    """Decode TrustScoreAccount; return score (0-100) or None if invalid."""
    if raw is None or len(raw) < MIN_ACCOUNT_LEN:
        return None
    payload = raw[DISCRIMINATOR_LEN:]
    return payload[WALLET_LEN] & 0xFF


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare CSV scores vs on-chain trust scores.")
    parser.add_argument("csv_file", nargs="?", default="wallets.csv", help="CSV file with headers wallet, score")
    args = parser.parse_args()

    if not os.path.isfile(args.csv_file):
        print(f"ERROR: File not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key or not program_id_str:
        print("ERROR: Set ORACLE_PROGRAM_ID and ORACLE_PRIVATE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    from solana.rpc.api import Client
    from solders.pubkey import Pubkey

    try:
        from backend_blockid.oracle.solana_publisher import _load_keypair, get_trust_score_pda
        keypair = _load_keypair(oracle_key)
        oracle_pubkey = keypair.pubkey()
    except Exception as e:
        print("ERROR: Failed to load oracle keypair:", e, file=sys.stderr)
        sys.exit(1)

    program_id = Pubkey.from_string(program_id_str)
    client = Client(rpc_url)

    rows: list[tuple[str, int]] = []
    with open(args.csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and "wallet" in reader.fieldnames and "score" in reader.fieldnames:
            for row in reader:
                wallet = (row.get("wallet") or "").strip()
                score_str = (row.get("score") or "").strip()
                if not wallet:
                    continue
                try:
                    csv_score = int(score_str)
                except ValueError:
                    csv_score = -1
                rows.append((wallet, csv_score))
        else:
            print("ERROR: CSV must have headers: wallet, score", file=sys.stderr)
            sys.exit(1)

    total = len(rows)
    matches = 0
    mismatches = 0

    for wallet_str, csv_score in rows:
        try:
            wallet_pubkey = Pubkey.from_string(wallet_str)
        except Exception:
            on_chain_score = None
            match = False
        else:
            pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
            try:
                resp = client.get_account_info(pda, encoding="base64")
            except Exception:
                on_chain_score = None
                match = False
            else:
                value = getattr(resp, "value", None) or (
                    getattr(resp, "result", None) and getattr(resp.result, "value", None)
                )
                if value is None:
                    on_chain_score = None
                    match = False
                else:
                    data = getattr(value, "data", None)
                    raw = _raw_bytes_from_account_data(data)
                    on_chain_score = _decode_on_chain_score(raw) if raw else None
                    match = on_chain_score is not None and csv_score == on_chain_score

        if match:
            matches += 1
        else:
            mismatches += 1

        on_chain_str = str(on_chain_score) if on_chain_score is not None else "N/A"
        status = "MATCH" if match else "MISMATCH"
        print("Wallet:", wallet_str)
        print("CSV score:", csv_score)
        print("On-chain score:", on_chain_str)
        print(status)
        print()

    print("--- Summary ---")
    print("Total wallets:", total)
    print("Matches:", matches)
    print("Mismatches:", mismatches)
    return 0 if mismatches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
