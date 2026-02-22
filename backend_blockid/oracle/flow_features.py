"""
STEP 2 Flow Analysis for BlockID — behavioral flow features from wallet transaction history.

Reads wallets from backend_blockid/data/wallets.csv, fetches recent transactions via Helius RPC,
computes per-wallet flow features (total_tx, unique_destinations, rapid_tx_count, avg_tx_interval,
percent_to_new_wallets, tx_chain_length_estimate). Writes backend_blockid/data/flow_features.csv.

When BLOCKID_USE_DUMMY_DATA=1 or Helius/RPC unavailable, uses devnet dummy dataset.
"""

from __future__ import annotations

import csv
import shutil
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from backend_blockid.config.env import (
    get_devnet_dummy_dir,
    get_solana_rpc_url,
    load_blockid_env,
    use_devnet_dummy_data,
)
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Paths: script in backend_blockid/oracle/, data in backend_blockid/data/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
WALLETS_CSV = _DATA_DIR / "wallets.csv"
OUTPUT_CSV = _DATA_DIR / "flow_features.csv"

# RPC
SIGS_LIMIT = 50
MAX_TX_PER_WALLET = 50
DELAY_SEC = 0.25
RETRY_DELAY_SEC = 2.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# Feature definitions
RAPID_TX_WINDOW_SEC = 30
TX_CHAIN_SLOT_DELTA = 2
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"


def _rpc_url() -> str | None:
    load_blockid_env()
    return get_solana_rpc_url()


def _rpc_post(url: str, method: str, params: list[Any]) -> dict[str, Any] | None:
    payload = {"jsonrpc": "2.0", "id": "blockid-flow", "method": method, "params": params}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                print("[flow] rate limit (429), waiting", RETRY_DELAY_SEC, "s...")
                time.sleep(RETRY_DELAY_SEC)
                continue
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print("[flow] request error:", e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            return None
        if data.get("error"):
            print("[flow] RPC error:", data.get("error"))
            return None
        return data
    return None


def get_signatures(url: str, address: str, before: str | None = None) -> list[dict]:
    params: list[Any] = [address, {"limit": SIGS_LIMIT}]
    if before:
        params[1]["before"] = before
    data = _rpc_post(url, "getSignaturesForAddress", params)
    if data is None:
        return []
    result = data.get("result")
    return result if isinstance(result, list) else []


def get_transaction(url: str, signature: str) -> dict | None:
    params = [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    data = _rpc_post(url, "getTransaction", params)
    if data is None:
        return None
    return data.get("result")


def _account_keys_from_tx(tx: dict) -> list[str]:
    """Extract account keys from parsed tx (legacy or versioned message)."""
    out: list[str] = []
    inner = tx.get("transaction") or tx
    msg = inner.get("message") or {}
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


def _transfer_destination_from_tx(tx: dict, wallet: str) -> str | None:
    """Extract destination (to) of native transfer from wallet; None if not found."""
    inner = tx.get("transaction") or tx
    msg = inner.get("message") or {}
    keys = _account_keys_from_tx(tx)
    if len(keys) < 2:
        return None
    instructions = msg.get("instructions") or []
    for ix in instructions:
        if (ix.get("programId") or ix.get("program")) != SYSTEM_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if not isinstance(parsed, dict) or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        src = (info.get("source") or info.get("from") or "").strip()
        dst = (info.get("destination") or info.get("to") or "").strip()
        if src == wallet and dst:
            return dst
    if keys[0] == wallet and len(keys) > 1:
        return keys[1]
    if keys[1] == wallet and len(keys) > 0:
        return keys[0]
    return None


def load_wallets(path: Path) -> list[str]:
    out: list[str] = []
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w:
                out.append(w)
    return out


def fetch_tx_records(url: str, wallet: str, max_tx: int) -> list[dict]:
    """Fetch up to max_tx recent transactions; each record has blockTime, slot, to_address."""
    records: list[dict] = []
    before: str | None = None
    while len(records) < max_tx:
        sigs = get_signatures(url, wallet, before)
        time.sleep(DELAY_SEC)
        if not sigs:
            break
        for s in sigs:
            if len(records) >= max_tx:
                break
            sig = s.get("signature")
            if not sig or not isinstance(sig, str):
                continue
            block_time = s.get("blockTime")
            slot = s.get("slot")
            tx = get_transaction(url, sig)
            time.sleep(DELAY_SEC)
            to_addr = None
            if tx:
                to_addr = _transfer_destination_from_tx(tx, wallet)
            records.append({
                "blockTime": block_time if isinstance(block_time, (int, float)) else None,
                "slot": slot if isinstance(slot, (int, float)) else None,
                "to": to_addr or "",
            })
        if len(sigs) < SIGS_LIMIT:
            break
        before = sigs[-1].get("signature") if sigs else None
        if not before:
            break
    return records


def compute_rapid_tx_count(records: list[dict]) -> int:
    """Count transactions that have at least one other tx within RAPID_TX_WINDOW_SEC."""
    times = [r["blockTime"] for r in records if r.get("blockTime") is not None]
    if not times:
        return 0
    times = sorted(times)
    count = 0
    for i, t in enumerate(times):
        for j, u in enumerate(times):
            if i != j and abs(u - t) <= RAPID_TX_WINDOW_SEC:
                count += 1
                break
    return count


def compute_avg_tx_interval(records: list[dict]) -> float:
    """Mean time in seconds between consecutive transactions (by blockTime)."""
    times = sorted([r["blockTime"] for r in records if r.get("blockTime") is not None])
    if len(times) < 2:
        return 0.0
    intervals = [times[i + 1] - times[i] for i in range(len(times) - 1)]
    return sum(intervals) / len(intervals)


def compute_percent_to_new_wallets(records: list[dict]) -> float:
    """Percent of tx whose destination was never seen before (order by blockTime)."""
    if not records:
        return 0.0
    by_time = [r for r in records if r.get("blockTime") is not None]
    by_time.sort(key=lambda x: x["blockTime"] or 0)
    seen: set[str] = set()
    new_count = 0
    for r in by_time:
        to = (r.get("to") or "").strip()
        if not to:
            continue
        if to not in seen:
            new_count += 1
            seen.add(to)
    total_with_to = sum(1 for r in by_time if (r.get("to") or "").strip())
    if total_with_to == 0:
        return 0.0
    return 100.0 * new_count / total_with_to


def compute_tx_chain_length_estimate(records: list[dict]) -> int:
    """Max length of a run of consecutive txs where each is within TX_CHAIN_SLOT_DELTA blocks of the previous."""
    slots = [r["slot"] for r in records if r.get("slot") is not None]
    if not slots:
        return 0
    slots = sorted(slots)
    best = 1
    run = 1
    for i in range(1, len(slots)):
        if slots[i] - slots[i - 1] <= TX_CHAIN_SLOT_DELTA:
            run += 1
            best = max(best, run)
        else:
            run = 1
    return best


def flow_features_for_wallet(url: str, wallet: str, max_tx: int) -> dict[str, Any]:
    """Fetch tx history and compute all flow features for one wallet."""
    records = fetch_tx_records(url, wallet, max_tx)
    total_tx = len(records)
    to_addrs = [r.get("to") or "" for r in records if (r.get("to") or "").strip()]
    unique_destinations = len(set(to_addrs))
    rapid_tx_count = compute_rapid_tx_count(records)
    avg_tx_interval = compute_avg_tx_interval(records)
    percent_to_new = compute_percent_to_new_wallets(records)
    tx_chain_length = compute_tx_chain_length_estimate(records)

    return {
        "wallet": wallet,
        "total_tx": total_tx,
        "unique_destinations": unique_destinations,
        "rapid_tx_count": rapid_tx_count,
        "avg_tx_interval": round(avg_tx_interval, 2),
        "percent_to_new_wallets": round(percent_to_new, 2),
        "tx_chain_length_estimate": tx_chain_length,
    }


def _use_dummy_and_exit() -> int:
    """Use devnet dummy flow_features.csv when RPC unavailable. Return 0 on success."""
    dummy_dir = get_devnet_dummy_dir()
    src = dummy_dir / "flow_features.csv"
    if not src.exists():
        print("[flow] ERROR: devnet dummy flow_features.csv not found:", src)
        return 1
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, OUTPUT_CSV)
    print("[flow] Using devnet dummy dataset (BLOCKID_USE_DUMMY_DATA=1 or RPC unavailable)")
    print("[flow] saved dummy to", OUTPUT_CSV)
    return 0


def main() -> int:
    load_blockid_env()
    logger.info("module_start", module="flow_features")

    if use_devnet_dummy_data():
        return _use_dummy_and_exit()

    url = _rpc_url()
    if not url:
        print("[flow] ERROR: set HELIUS_API_KEY or SOLANA_RPC_URL in .env; or BLOCKID_USE_DUMMY_DATA=1 for dummy")
        return 1

    if not WALLETS_CSV.exists():
        print("[flow] ERROR: wallets not found:", WALLETS_CSV)
        print("[flow] Example format: CSV with header 'wallet' and one base58 address per line.")
        return 1

    wallets = load_wallets(WALLETS_CSV)
    if not wallets:
        print("[flow] ERROR: no wallets in", WALLETS_CSV)
        return 1

    print("[flow] wallets:", len(wallets), "| max tx per wallet:", MAX_TX_PER_WALLET)
    rows = []
    for i, wallet in enumerate(wallets):
        short = (wallet[:20] + "...") if len(wallet) > 20 else wallet
        print("[flow]", i + 1, "/", len(wallets), short)
        try:
            row = flow_features_for_wallet(url, wallet, MAX_TX_PER_WALLET)
            rows.append(row)
        except Exception as e:
            print("[flow] error for", short, ":", e)
            rows.append({
                "wallet": wallet,
                "total_tx": 0,
                "unique_destinations": 0,
                "rapid_tx_count": 0,
                "avg_tx_interval": 0.0,
                "percent_to_new_wallets": 0.0,
                "tx_chain_length_estimate": 0,
            })

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)
    print("[flow] saved", len(rows), "rows to", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
