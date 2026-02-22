"""
Auto-detect scam-related transactions on devnet via Helius and store proofs in wallet_reasons.

Usage:
  py backend_blockid/tools/auto_detect_scam_tx_devnet.py WALLET_ADDRESS
  py backend_blockid/tools/auto_detect_scam_tx_devnet.py  # batch mode (scam_wallets table)

Run:
  py backend_blockid/tools/auto_detect_scam_tx_devnet.py WALLET
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any

import requests
import sqlite3
from dotenv import load_dotenv

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.reason_codes import REASON_WEIGHTS, DEFAULT_WEIGHT

logger = get_logger(__name__)

load_dotenv()
API_KEY = os.getenv("HELIUS_API_KEY")
if not API_KEY:
    raise RuntimeError("HELIUS_API_KEY not set. Add it to backend_blockid/.env")
print(f"[auto_detect] Using Helius key: {API_KEY[:4]}****")

DB_PATH = r"D:/BACKENDBLOCKID/blockid.db"
HELIUS_URL_TEMPLATE = "https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={key}"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2.0

# Simple heuristics
DRAIN_LAMPORTS_THRESHOLD = 10 * 1_000_000_000  # 10 SOL
SUSPICIOUS_PROGRAMS = {
    # Add known scam program IDs here if needed
}


def solscan_link(tx: str) -> str | None:
    if not tx:
        return None
    return f"https://solscan.io/tx/{tx}?cluster=devnet"


def _ensure_wallet_reasons_columns(cur: sqlite3.Cursor) -> None:
    try:
        cur.execute("ALTER TABLE wallet_reasons ADD COLUMN tx_hash TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE wallet_reasons ADD COLUMN tx_link TEXT")
    except sqlite3.OperationalError:
        pass


def _fetch_transactions(wallet: str, api_key: str) -> list[dict[str, Any]]:
    url = HELIUS_URL_TEMPLATE.format(wallet=wallet, key=api_key)
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                logger.warning("helius_rate_limited", wallet=wallet, attempt=attempt + 1)
                time.sleep(RETRY_DELAY_SEC)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except requests.RequestException as e:
            logger.warning("helius_request_failed", wallet=wallet, error=str(e))
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            print("[auto_detect] ERROR contacting Helius")
            return []
    return []


def _tx_signature(tx: dict[str, Any]) -> str | None:
    return (
        tx.get("signature")
        or tx.get("txHash")
        or tx.get("transactionSignature")
        or tx.get("hash")
    )


def _is_large_outgoing(tx: dict[str, Any], wallet: str) -> bool:
    for t in tx.get("nativeTransfers") or []:
        if (t.get("fromUserAccount") or "") == wallet:
            try:
                amt = int(t.get("amount") or 0)
            except (TypeError, ValueError):
                amt = 0
            if amt >= DRAIN_LAMPORTS_THRESHOLD:
                return True
    return False


def _is_suspicious_program(tx: dict[str, Any]) -> bool:
    for ix in tx.get("instructions") or []:
        prog = ix.get("programId") or ix.get("programIdIndex") or ix.get("program")
        if prog in SUSPICIOUS_PROGRAMS:
            return True
    return False


def _is_token_mint(tx: dict[str, Any]) -> bool:
    ttype = (tx.get("type") or "").upper()
    if ttype in {"TOKEN_MINT", "MINT"}:
        return True
    for ix in tx.get("instructions") or []:
        if (ix.get("type") or "").lower() == "mint":
            return True
    return False


def _is_rugpull_like(tx: dict[str, Any]) -> bool:
    desc = (tx.get("description") or "").lower()
    return "rug" in desc or "rugpull" in desc


def _interacts_with_scam_wallet(tx: dict[str, Any], scam_wallets: set[str]) -> bool:
    for t in tx.get("nativeTransfers") or []:
        if t.get("fromUserAccount") in scam_wallets or t.get("toUserAccount") in scam_wallets:
            return True
    for t in tx.get("tokenTransfers") or []:
        if t.get("fromUserAccount") in scam_wallets or t.get("toUserAccount") in scam_wallets:
            return True
    return False


def _detect_reasons(tx: dict[str, Any], wallet: str, scam_wallets: set[str]) -> set[str]:
    reasons: set[str] = set()
    if _is_rugpull_like(tx) and _is_token_mint(tx):
        reasons.add("RUG_PULL_DEPLOYER")
    if _is_large_outgoing(tx, wallet):
        reasons.add("DRAINER_TX")
    if _is_suspicious_program(tx):
        reasons.add("DRAINER_TX")
    if _is_token_mint(tx):
        reasons.add("SCAM_TOKEN_MINT")
    if _interacts_with_scam_wallet(tx, scam_wallets):
        reasons.add("DRAINER_TX")
    return reasons


def _load_scam_wallets(cur: sqlite3.Cursor) -> set[str]:
    try:
        cur.execute("SELECT wallet FROM scam_wallets")
        return {r[0] for r in cur.fetchall() if r and r[0]}
    except Exception:
        return set()


def _existing_tx_hashes(cur: sqlite3.Cursor, wallet: str) -> set[str]:
    try:
        cur.execute("SELECT tx_hash FROM wallet_reasons WHERE wallet=? AND tx_hash IS NOT NULL", (wallet,))
        return {r[0] for r in cur.fetchall() if r and r[0]}
    except Exception:
        return set()


def _insert_reason(cur: sqlite3.Cursor, wallet: str, code: str, tx_hash: str) -> None:
    weight = REASON_WEIGHTS.get(code, DEFAULT_WEIGHT)
    cur.execute(
        "INSERT OR IGNORE INTO wallet_reasons(wallet, reason_code, weight, created_at, tx_hash, tx_link) VALUES (?, ?, ?, ?, ?, ?)",
        (wallet, code, int(weight), int(time.time()), tx_hash, solscan_link(tx_hash)),
    )


def scan_wallet(wallet: str, api_key: str, cur: sqlite3.Cursor, scam_wallets: set[str]) -> int:
    txs = _fetch_transactions(wallet, api_key)
    if not txs:
        logger.info("scam_tx_no_transactions", wallet=wallet)
        return 0

    existing = _existing_tx_hashes(cur, wallet)
    inserted = 0

    for tx in txs:
        sig = _tx_signature(tx)
        if not sig or sig in existing:
            continue
        reasons = _detect_reasons(tx, wallet, scam_wallets)
        if not reasons:
            continue
        for code in reasons:
            _insert_reason(cur, wallet, code, sig)
            inserted += 1
        existing.add(sig)

    logger.info("scam_tx_scan_done", wallet=wallet, inserted=inserted)
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser(description="Detect scam tx on devnet via Helius and save wallet_reasons.")
    ap.add_argument("wallet", nargs="?", default=None, help="Wallet address (optional; batch mode if omitted)")
    ap.add_argument("--api-key", dest="api_key", default=None, help="Helius API key (overrides env)")
    args = ap.parse_args()

    api_key = args.api_key or (API_KEY or "").strip()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    _ensure_wallet_reasons_columns(cur)

    scam_wallets = _load_scam_wallets(cur)
    wallets: list[str]
    if args.wallet:
        wallets = [args.wallet.strip()]
    else:
        wallets = sorted(scam_wallets)

    if not wallets:
        print("[auto_detect] No wallets to scan.")
        return 0

    total = 0
    for wallet in wallets:
        try:
            total += scan_wallet(wallet, api_key, cur, scam_wallets)
        except Exception as e:
            logger.exception("scam_tx_scan_failed", wallet=wallet, error=str(e))

    conn.commit()
    conn.close()

    if len(wallets) == 1:
        print(f"[auto_detect] Detected {total} suspicious tx for wallet {wallets[0]}")
    else:
        print(f"[auto_detect] Detected {total} suspicious tx for wallet(s) {len(wallets)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
