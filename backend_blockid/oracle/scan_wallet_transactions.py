"""
Scan last N transactions for tracked wallets via Helius RPC; detect suspicious patterns;
insert evidence into wallet_reason_evidence.

Rules:
- counterparty in scam_wallets.csv → DRAINER_INTERACTION
- tx amount > 100 SOL (outflow) → HIGH_VALUE_OUTFLOW
- wallet age < 3 days (oldest tx in batch) → NEW_WALLET
- tx count < 5 → LOW_ACTIVITY

Usage:
  py -m backend_blockid.oracle.scan_wallet_transactions --limit 10
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any

import requests

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import (
    get_solana_rpc_url,
    load_blockid_env,
    print_blockid_startup,
)
from backend_blockid.api_server.db_wallet_tracking import (
    init_db,
    insert_reason_evidence,
    load_active_wallets,
)

logger = get_logger(__name__)

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

TX_LIMIT = 50
SOL_LAMPORTS = 1_000_000_000
HIGH_VALUE_SOL = 100
HIGH_VALUE_LAMPORTS = HIGH_VALUE_SOL * SOL_LAMPORTS
WALLET_AGE_DAYS = 3
WALLET_AGE_SEC = WALLET_AGE_DAYS * 24 * 3600
LOW_ACTIVITY_THRESHOLD = 5

DELAY_SEC = 0.25
RETRY_DELAY_SEC = 2.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"


def _rpc_url() -> str | None:
    load_blockid_env()
    return get_solana_rpc_url()


def _rpc_post(url: str, method: str, params: list[Any]) -> dict[str, Any] | None:
    payload = {"jsonrpc": "2.0", "id": "blockid-scan-tx", "method": method, "params": params}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                logger.warning("scan_wallet_tx_rate_limit", attempt=attempt + 1)
                time.sleep(RETRY_DELAY_SEC)
                continue
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.warning("scan_wallet_tx_request_error", error=str(e), attempt=attempt + 1)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            return None
        err = data.get("error")
        if err:
            logger.warning("scan_wallet_tx_rpc_error", error=str(err))
            return None
        return data
    return None


def _get_signatures(url: str, address: str, limit: int = TX_LIMIT) -> list[dict]:
    params = [address, {"limit": limit}]
    data = _rpc_post(url, "getSignaturesForAddress", params)
    if data is None:
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


def _get_transaction(url: str, signature: str) -> dict | None:
    params = [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    data = _rpc_post(url, "getTransaction", params)
    if data is None:
        return None
    return data.get("result")


def _account_keys_from_parsed_tx(tx: dict) -> list[str]:
    out: list[str] = []
    msg = (tx.get("transaction") or tx).get("message") or {}
    keys = msg.get("accountKeys") or msg.get("staticAccountKeys") or []
    for k in keys:
        if isinstance(k, str) and len(k) >= 32:
            out.append(k)
        elif isinstance(k, dict) and k.get("pubkey"):
            out.append(str(k["pubkey"]))
    loaded = msg.get("loadedAddresses") or {}
    for role in ("writable", "readonly"):
        for addr in loaded.get(role) or []:
            if isinstance(addr, str) and len(addr) >= 32:
                out.append(addr)
    return out


def _instructions_from_parsed_tx(tx: dict) -> list[dict]:
    msg = (tx.get("transaction") or tx).get("message") or {}
    return msg.get("instructions") or []


def _extract_transfers_with_amount(tx: dict) -> list[tuple[str, str, int]]:
    """
    Extract (from, to, lamports) for native SOL transfers.
    """
    edges: list[tuple[str, str, int]] = []
    keys = _account_keys_from_parsed_tx(tx)
    for ix in _instructions_from_parsed_tx(tx):
        prog = ix.get("programId") or ix.get("program")
        if prog != SYSTEM_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if not isinstance(parsed, dict) or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        src = (info.get("source") or info.get("from") or "").strip()
        dst = (info.get("destination") or info.get("to") or "").strip()
        lamports = 0
        if "lamports" in info:
            lamports = int(info["lamports"]) if info["lamports"] is not None else 0
        elif "amount" in info and info["amount"] is not None:
            lamports = int(info["amount"])
        if src and dst and src != dst:
            edges.append((src, dst, lamports))
    return edges


def load_scam_wallets() -> set[str]:
    """Load scam wallet addresses from scam_wallets.csv (normalized lowercase for lookup)."""
    out: set[str] = set()
    path = SCAM_WALLETS_CSV
    if not path.exists():
        logger.debug("scan_wallet_tx_scam_csv_missing", path=str(path))
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
                if w and len(w) >= 32:
                    out.add(w)
        logger.info("scan_wallet_tx_scam_loaded", count=len(out), path=str(path))
    except Exception as e:
        logger.warning("scan_wallet_tx_scam_load_failed", path=str(path), error=str(e))
    return out


def _scan_wallet(
    url: str,
    wallet: str,
    scam_set: set[str],
) -> list[dict[str, Any]]:
    """
    Fetch last TX_LIMIT transactions for wallet, apply rules, return list of evidence rows
    to insert. Each row: wallet, reason_code, tx_signature, counterparty, amount, token, timestamp.
    """
    evidence: list[dict[str, Any]] = []
    sigs = _get_signatures(url, wallet)
    time.sleep(DELAY_SEC)

    now = int(time.time())
    if not sigs:
        # 0 txs → LOW_ACTIVITY
        evidence.append({
            "wallet": wallet,
            "reason_code": "LOW_ACTIVITY",
            "tx_signature": None,
            "counterparty": None,
            "amount": None,
            "token": "SOL",
            "timestamp": now,
        })
        logger.debug("scan_wallet_tx_no_sigs", wallet=wallet[:16] + "...")
        return evidence

    oldest_ts: int | None = None
    txs_with_data: list[tuple[str, dict]] = []

    for sig_info in sigs:
        sig = sig_info.get("signature")
        if not sig or not isinstance(sig, str):
            continue
        tx = _get_transaction(url, sig)
        time.sleep(DELAY_SEC)
        if not tx:
            continue
        block_time = tx.get("blockTime")
        if block_time is not None:
            ts = int(block_time)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
        txs_with_data.append((sig, tx))

    # Rule: tx count < 5 → LOW_ACTIVITY
    if len(txs_with_data) < LOW_ACTIVITY_THRESHOLD:
        ref_sig, ref_tx = txs_with_data[-1] if txs_with_data else (None, {})
        ref_ts = ref_tx.get("blockTime") if isinstance(ref_tx.get("blockTime"), (int, float)) else now
        evidence.append({
            "wallet": wallet,
            "reason_code": "LOW_ACTIVITY",
            "tx_signature": ref_sig,
            "counterparty": None,
            "amount": None,
            "token": "SOL",
            "timestamp": int(ref_ts) if ref_ts else now,
        })
        logger.debug("scan_wallet_tx_low_activity", wallet=wallet[:16] + "...", tx_count=len(txs_with_data))

    # Rule: wallet age < 3 days
    if oldest_ts is not None and (now - oldest_ts) < WALLET_AGE_SEC:
        ref_sig, ref_tx = txs_with_data[0] if txs_with_data else (None, {})  # oldest first in desc order
        evidence.append({
            "wallet": wallet,
            "reason_code": "NEW_WALLET",
            "tx_signature": ref_sig,
            "counterparty": None,
            "amount": None,
            "token": "SOL",
            "timestamp": oldest_ts,
        })
        logger.debug("scan_wallet_tx_new_wallet", wallet=wallet[:16] + "...", oldest_age_days=round((now - oldest_ts) / 86400, 1))

    # Per-tx rules: DRAINER_INTERACTION, HIGH_VALUE_OUTFLOW
    for sig, tx in txs_with_data:
        block_time = tx.get("blockTime")
        ts = int(block_time) if block_time is not None else now

        for src, dst, lamports in _extract_transfers_with_amount(tx):
            # Determine counterparty from our wallet's perspective
            if src == wallet:
                counterparty = dst
                is_outflow = True
            elif dst == wallet:
                counterparty = src
                is_outflow = False
            else:
                continue

            # Rule: counterparty in scam_wallets → DRAINER_INTERACTION
            if counterparty in scam_set:
                evidence.append({
                    "wallet": wallet,
                    "reason_code": "DRAINER_INTERACTION",
                    "tx_signature": sig,
                    "counterparty": counterparty,
                    "amount": str(lamports),
                    "token": "SOL",
                    "timestamp": ts,
                })
                logger.info(
                    "scan_wallet_tx_drainer",
                    wallet=wallet[:16] + "...",
                    counterparty=counterparty[:16] + "...",
                    sig=sig[:16] + "...",
                )

            # Rule: outflow > 100 SOL → HIGH_VALUE_OUTFLOW
            if is_outflow and lamports >= HIGH_VALUE_LAMPORTS:
                evidence.append({
                    "wallet": wallet,
                    "reason_code": "HIGH_VALUE_OUTFLOW",
                    "tx_signature": sig,
                    "counterparty": counterparty,
                    "amount": str(lamports),
                    "token": "SOL",
                    "timestamp": ts,
                })
                logger.info(
                    "scan_wallet_tx_high_value",
                    wallet=wallet[:16] + "...",
                    sol=round(lamports / SOL_LAMPORTS, 2),
                    sig=sig[:16] + "...",
                )

    return evidence


def main() -> int:
    load_blockid_env()
    print_blockid_startup("scan_wallet_transactions")

    ap = argparse.ArgumentParser(description="Scan tracked wallets' tx; detect patterns; insert evidence")
    ap.add_argument("--limit", type=int, default=None, help="Max wallets to process (default: all)")
    args = ap.parse_args()

    url = _rpc_url()
    if not url:
        logger.error("scan_wallet_tx_no_rpc", message="Set HELIUS_API_KEY or SOLANA_RPC_URL in .env")
        print("[scan_wallet_tx] ERROR: set HELIUS_API_KEY or SOLANA_RPC_URL in .env", file=sys.stderr)
        return 1

    init_db()
    wallets = load_active_wallets()
    if not wallets:
        logger.warning("scan_wallet_tx_no_wallets")
        print("[scan_wallet_tx] No tracked wallets; run add_wallet or load from manual_wallets first.")
        return 0

    if args.limit is not None:
        wallets = wallets[: args.limit]
    logger.info("scan_wallet_tx_start", wallets=len(wallets), tx_limit=TX_LIMIT)

    scam_set = load_scam_wallets()

    inserted = 0
    errors = 0
    seen: set[tuple[str, str, str | None, str | None]] = set()

    for i, wallet in enumerate(wallets):
        try:
            evidence = _scan_wallet(url, wallet, scam_set)
            for row in evidence:
                key = (row["wallet"], row["reason_code"], row.get("tx_signature"), row.get("counterparty"))
                if key in seen:
                    continue
                seen.add(key)
                try:
                    insert_reason_evidence(
                        wallet=row["wallet"],
                        reason_code=row["reason_code"],
                        tx_signature=row.get("tx_signature"),
                        counterparty=row.get("counterparty"),
                        amount=row.get("amount"),
                        token=row.get("token"),
                        timestamp=row.get("timestamp"),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning("scan_wallet_tx_insert_failed", wallet=wallet[:16], reason=row["reason_code"], error=str(e))
                    errors += 1
        except Exception as e:
            logger.exception("scan_wallet_tx_scan_failed", wallet=wallet[:16] + "...", error=str(e))
            errors += 1
        if (i + 1) % 5 == 0:
            logger.debug("scan_wallet_tx_progress", processed=i + 1, total=len(wallets))

    logger.info("scan_wallet_tx_done", wallets_processed=len(wallets), evidence_inserted=inserted, errors=errors)
    print(f"[scan_wallet_tx] Done. wallets={len(wallets)} inserted={inserted} errors={errors}")
    return 1 if errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
