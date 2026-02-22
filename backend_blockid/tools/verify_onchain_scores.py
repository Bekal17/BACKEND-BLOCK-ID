#!/usr/bin/env python3
"""
Compare wallet_scores.csv with on-chain PDA accounts.

For each wallet:
  - Derive trust score PDA
  - Fetch account data
  - Parse score + risk
  - Compare with CSV

Prints mismatches and total success rate.

Usage:
  py -m backend_blockid.tools.verify_onchain_scores
"""

from __future__ import annotations

import base64
import csv
import os
import sys
from pathlib import Path

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import get_oracle_program_id, load_blockid_env
from backend_blockid.oracle.solana_publisher import (
    _load_keypair,
    get_trust_score_pda,
    parse_trust_score_account_data,
)

logger = get_logger(__name__)

_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"

SEP = "=" * 60
SEP_THIN = "-" * 60


def _log(msg: str) -> None:
    print(f"[verify_onchain] {msg}")


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
    if isinstance(data, (list, tuple)) and data:
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
        return _raw_bytes_from_account_data(getattr(data, "data"))
    return None


def _csv_trust_score_and_risk(row: dict) -> tuple[int, int]:
    """
    Extract (trust_score 0-100, risk_level 0-3) from CSV row.
    Supports: final_score, ml_score, score, risk_score (legacy: trust = 100 - risk).
    """
    score = row.get("final_score") or row.get("ml_score") or row.get("score")
    if score is not None and str(score).strip():
        try:
            trust = max(0, min(100, int(float(score))))
        except (TypeError, ValueError):
            trust = 50
    elif "risk_score" in row:
        try:
            risk_val = int(float(row["risk_score"]))
            trust = max(0, min(100, 100 - risk_val))
        except (TypeError, ValueError):
            trust = 50
    else:
        trust = 50

    # Risk level from trust: Low=0, Medium=1, High=2, Critical=3
    if trust < 30:
        risk_level = 3
    elif trust < 50:
        risk_level = 2
    elif trust < 70:
        risk_level = 1
    else:
        risk_level = 0
    return trust, risk_level


def load_wallet_scores(path: Path) -> list[dict]:
    """Load wallet_scores.csv. Returns list of {wallet, trust_score, risk_level}."""
    rows: list[dict] = []
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            trust, risk = _csv_trust_score_and_risk(row)
            rows.append({
                "wallet": w,
                "trust_score": trust,
                "risk_level": risk,
                "reason_code": (row.get("reason_code") or "").strip(),
            })
    return rows


def fetch_onchain_score(
    client: object,
    program_id: object,
    oracle_pubkey: object,
    wallet: str,
) -> tuple[int, int] | None:
    """Fetch on-chain trust_score and risk_level for wallet. Returns (trust, risk) or None if missing/invalid."""
    from solders.pubkey import Pubkey

    try:
        wallet_pubkey = Pubkey.from_string(wallet)
    except Exception:
        return None
    pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    try:
        resp = client.get_account_info(pda, encoding="base64")
    except Exception as e:
        logger.warning("verify_fetch_failed", wallet=wallet[:16] + "...", error=str(e))
        return None

    value = getattr(resp, "value", None) or (
        getattr(resp, "result", None) and getattr(resp.result, "value", None)
    )
    if value is None:
        return None

    data = getattr(value, "data", None)
    raw = _raw_bytes_from_account_data(data)
    if raw is None:
        return None

    parsed = parse_trust_score_account_data(raw)
    return parsed


def main() -> int:
    load_blockid_env()
    _log(SEP)
    _log("Verify on-chain scores vs wallet_scores.csv")
    _log(SEP)

    if not WALLET_SCORES_CSV.exists():
        _log(f"ERROR: {WALLET_SCORES_CSV} not found")
        return 1

    csv_rows = load_wallet_scores(WALLET_SCORES_CSV)
    if not csv_rows:
        _log("ERROR: No wallet rows in CSV")
        return 1

    _log(f"Loaded {len(csv_rows)} wallets from {WALLET_SCORES_CSV}")
    _log("")

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if not rpc_url:
        from backend_blockid.config.env import get_solana_rpc_url
        rpc_url = get_solana_rpc_url()
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = get_oracle_program_id()
    if not oracle_key or not program_id_str:
        _log("ERROR: Set ORACLE_PRIVATE_KEY and ORACLE_PROGRAM_ID in .env")
        return 1

    try:
        keypair = _load_keypair(oracle_key)
        oracle_pubkey = keypair.pubkey()
    except Exception as e:
        _log(f"ERROR: Failed to load oracle keypair: {e}")
        return 1

    from solders.pubkey import Pubkey
    from solana.rpc.api import Client

    program_id = Pubkey.from_string(program_id_str)
    client = Client(rpc_url)

    matches = 0
    mismatches: list[dict] = []
    missing: list[str] = []

    for row in csv_rows:
        wallet = row["wallet"]
        csv_trust = row["trust_score"]
        csv_risk = row["risk_level"]

        onchain = fetch_onchain_score(client, program_id, oracle_pubkey, wallet)
        if onchain is None:
            missing.append(wallet)
            continue

        onchain_trust, onchain_risk = onchain
        if csv_trust == onchain_trust and csv_risk == onchain_risk:
            matches += 1
        else:
            mismatches.append({
                "wallet": wallet,
                "csv_trust": csv_trust,
                "csv_risk": csv_risk,
                "onchain_trust": onchain_trust,
                "onchain_risk": onchain_risk,
                "reason_code": row.get("reason_code", ""),
            })

    # Report
    total_checked = matches + len(mismatches)
    total_with_pda = total_checked + len(missing)

    _log(SEP_THIN)
    _log("MISMATCHES")
    _log(SEP_THIN)
    if mismatches:
        for m in mismatches:
            _log(
                f"  {m['wallet'][:20]}... "
                f"CSV: trust={m['csv_trust']} risk={m['csv_risk']} | "
                f"on-chain: trust={m['onchain_trust']} risk={m['onchain_risk']}"
            )
    else:
        _log("  (none)")
    _log("")

    if missing:
        _log(SEP_THIN)
        _log(f"MISSING PDA ({len(missing)} wallets)")
        _log(SEP_THIN)
        for w in missing[:20]:
            _log(f"  {w[:40]}...")
        if len(missing) > 20:
            _log(f"  ... and {len(missing) - 20} more")
        _log("")

    _log(SEP)
    _log("SUMMARY")
    _log(SEP)
    _log(f"  Total CSV rows:     {len(csv_rows)}")
    _log(f"  PDA not found:      {len(missing)}")
    _log(f"  Checked on-chain:   {total_checked}")
    _log(f"  Matches:            {matches}")
    _log(f"  Mismatches:         {len(mismatches)}")
    if total_checked > 0:
        rate = 100.0 * matches / total_checked
        _log(f"  Success rate:       {rate:.1f}%")
    else:
        _log("  Success rate:       N/A (no PDAs found)")
    _log(SEP)

    return 0 if not mismatches else 1


if __name__ == "__main__":
    raise SystemExit(main())
