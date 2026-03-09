"""
Incremental Helius transaction fetch for BlockID graph pipeline.

Fetches only NEW transactions per wallet using last saved signature.
Saves minimal fields for graph clustering and flow analysis.
Uses devnet when SOLANA_NETWORK=devnet.

Usage:
    py -m backend_blockid.tools.fetch_helius_transactions
    py -m backend_blockid.tools.fetch_helius_transactions --max-wallets 100 --days-back 30
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

# Ensure project root on path
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection
from backend_blockid.oracle.realtime_risk_engine import process_new_transaction
from backend_blockid.tools.helius_client import helius_request
from backend_blockid.oracle.wallet_scan_prioritizer import get_prioritized_wallets, update_scan_timestamp

# --- Config ---
API_KEY = os.getenv("HELIUS_API_KEY")
SOLANA_NETWORK = (os.getenv("SOLANA_NETWORK") or "devnet").strip().lower()
RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()

# Helius Enhanced Transactions API. For devnet, optionally set HELIUS_DEVNET_BASE.
_override = os.getenv("HELIUS_DEVNET_BASE") if SOLANA_NETWORK == "devnet" else None
HELIUS_BASE = (_override or os.getenv("HELIUS_API_BASE") or "https://api.helius.xyz").rstrip("/")

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2.0
RATE_LIMIT_SLEEP = 0.2
LIMIT_PER_CALL = 100

# Paths
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TEST_WALLETS_CSV = _DATA_DIR / "test_wallets.csv"


def _build_url(wallet: str, before_sig: str | None) -> str:
    url = f"{HELIUS_BASE}/v0/addresses/{wallet}/transactions?api-key={API_KEY}&limit={LIMIT_PER_CALL}"
    if before_sig:
        url += f"&before-signature={before_sig}"
    return url


def _get_last_signature(cur: Any, wallet: str) -> str | None:
    cur.execute(
        "SELECT signature FROM transactions WHERE wallet = ? ORDER BY timestamp DESC LIMIT 1",
        (wallet,),
    )
    row = cur.fetchone()
    return row["signature"] if row else None


def _load_wallets(conn: Any, max_wallets: int | None) -> list[str]:
    """Use prioritizer when not in TEST_MODE; TEST_MODE → test_wallets.csv only."""
    test_mode = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
    wallets = get_prioritized_wallets(max_wallets=max_wallets, test_mode=test_mode)
    if wallets:
        return wallets
    # Fallback: legacy load if prioritizer returns empty (no candidates)
    wallets = []
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tracked_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM tracked_wallets ORDER BY created_at ASC")
            for r in cur.fetchall():
                w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip() if r else ""
                if w and w not in wallets:
                    wallets.append(w)
    except Exception:
        pass
    if not wallets and TEST_WALLETS_CSV.exists():
        import csv
        with open(TEST_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or row.get("address") or "").strip()
                if w and w not in wallets:
                    wallets.append(w)
    if max_wallets is not None and max_wallets > 0:
        wallets = wallets[:max_wallets]
    return wallets


def _parse_tx_to_record(tx: dict[str, Any], queried_wallet: str) -> dict[str, Any] | None:
    """Extract first SOL or SPL transfer into a single record. One row per tx (signature PK)."""
    sig = (
        tx.get("signature")
        or tx.get("transactionSignature")
        or tx.get("txHash")
        or tx.get("hash")
        or ""
    )
    if not sig:
        return None
    ts = tx.get("timestamp") or tx.get("blockTime") or 0
    program_id = ""
    for ix in tx.get("instructions") or []:
        pid = ix.get("programId") or ix.get("programIdIndex") or ""
        if pid:
            program_id = str(pid)
            break

    # Prefer native SOL first
    for t in tx.get("nativeTransfers") or []:
        frm = (t.get("fromUserAccount") or "").strip()
        to = (t.get("toUserAccount") or "").strip()
        if not frm or not to:
            continue
        try:
            amt = float(t.get("amount") or 0) / 1e9
        except (TypeError, ValueError):
            amt = 0.0
        return {
            "signature": sig,
            "wallet": queried_wallet,
            "from_wallet": frm,
            "to_wallet": to,
            "amount": amt,
            "token": "SOL",
            "timestamp": int(ts) if ts else 0,
            "program_id": program_id or "11111111111111111111111111111111",
        }

    # Fallback to SPL token
    for t in tx.get("tokenTransfers") or []:
        frm = (t.get("fromUserAccount") or t.get("fromTokenAccount") or "").strip()
        to = (t.get("toUserAccount") or t.get("toTokenAccount") or "").strip()
        if not frm or not to:
            continue
        mint = (t.get("mint") or t.get("tokenMint") or "unknown").strip()
        try:
            raw = t.get("tokenAmount") or t.get("amount") or 0
            if isinstance(raw, dict):
                amt = float(raw.get("amount", 0) or 0)
                dec = int(raw.get("decimals", 6) or 6)
                amt = amt / (10**dec)
            else:
                amt = float(raw)
        except (TypeError, ValueError):
            amt = 0.0
        return {
            "signature": sig,
            "wallet": queried_wallet,
            "from_wallet": frm,
            "to_wallet": to,
            "amount": amt,
            "token": mint,
            "timestamp": int(ts) if ts else 0,
            "program_id": program_id or "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        }

    return None


def _fetch_transactions(wallet: str, before_sig: str | None) -> list[dict[str, Any]]:
    url = _build_url(wallet, before_sig)
    endpoint = "addresses/transactions"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                time.sleep(RETRY_DELAY_SEC)
                continue
            resp.raise_for_status()
            data = resp.json()
            helius_request(endpoint, wallet, request_count=1)
            return data if isinstance(data, list) else []
        except requests.RequestException:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            return []
    return []


def _filter_by_days_back(records: list[dict[str, Any]], days_back: int) -> list[dict[str, Any]]:
    if days_back <= 0:
        return records
    cutoff = int(time.time()) - (days_back * 86400)
    return [r for r in records if (r.get("timestamp") or 0) >= cutoff]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Incremental Helius transaction fetch for BlockID graph pipeline."
    )
    ap.add_argument("--max-wallets", type=int, default=None, help="Limit number of wallets to scan")
    ap.add_argument("--days-back", type=int, default=0, help="Only save txs within last N days (0=all)")
    args = ap.parse_args()

    if not API_KEY:
        print("[helius_fetch] ERROR: HELIUS_API_KEY not set in .env")
        return 1

    conn = get_connection()
    cur = conn.cursor()
    wallets = _load_wallets(conn, args.max_wallets)
    if not wallets:
        print("[helius_fetch] No wallets (tracked_wallets or test_wallets.csv)")
        conn.close()
        return 0

    total_new = 0
    total_skipped = 0
    helius_calls = 0

    for i, wallet in enumerate(wallets):
        try:
            last_sig = _get_last_signature(cur, wallet)
            raw = _fetch_transactions(wallet, last_sig)
            helius_calls += 1

            all_records: list[dict[str, Any]] = []
            for tx in raw:
                r = _parse_tx_to_record(tx, wallet)
                if r:
                    all_records.append(r)

            if args.days_back > 0:
                all_records = _filter_by_days_back(all_records, args.days_back)

            new_count = 0
            skip_count = 0
            for r in all_records:
                try:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO transactions
                        (signature, wallet, from_wallet, to_wallet, amount, token, timestamp, program_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            r["signature"],
                            r["wallet"],
                            r["from_wallet"],
                            r["to_wallet"],
                            r["amount"],
                            r["token"],
                            r["timestamp"],
                            r["program_id"],
                        ),
                    )
                    if cur.rowcount > 0:
                        new_count += 1
                        try:
                            process_new_transaction(r)
                        except Exception as e:
                            print(f"[helius_fetch] realtime_risk skip: {e}")
                    else:
                        skip_count += 1
                except Exception:
                    skip_count += 1
                    continue

            total_new += new_count
            total_skipped += skip_count
            try:
                import asyncio
                asyncio.run(update_scan_timestamp(wallet))
            except Exception:
                pass
            print(f"[helius_fetch] wallet={wallet[:8]}... new_tx={new_count} skipped={skip_count}")

        except Exception as e:
            print(f"[helius_fetch] ERROR wallet={wallet[:8]}... {e}")
            continue

        if i < len(wallets) - 1:
            time.sleep(RATE_LIMIT_SLEEP)

    conn.commit()
    conn.close()

    print("---")
    print(f"Total wallets scanned: {len(wallets)}")
    print(f"New transactions saved: {total_new}")
    print(f"Helius calls made: {helius_calls}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
