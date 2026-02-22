"""
STEP 3 Approval / Drainer Detection for BlockID — behavioral fingerprint for drainer patterns.

Reads wallets from backend_blockid/data/wallets.csv, fetches recent transactions via Helius RPC,
computes per-wallet drainer heuristics (approval_like_count, rapid_outflow_count, multi_victim_pattern,
new_contract_interaction_count, swap_then_transfer_pattern, percent_to_same_cluster).
Writes backend_blockid/data/drainer_features.csv. No ML; heuristic-only.

When BLOCKID_USE_DUMMY_DATA=1 or Helius/RPC unavailable, uses devnet dummy dataset.
"""

from __future__ import annotations

import csv
import os
import shutil
import time
from collections import Counter
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

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
WALLETS_CSV = _DATA_DIR / "wallets.csv"
OUTPUT_CSV = _DATA_DIR / "drainer_features.csv"

SIGS_LIMIT = 50
MAX_TX_PER_WALLET = 50
DELAY_SEC = 0.25
RETRY_DELAY_SEC = 2.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# Heuristic windows
APPROVAL_LIKE_WINDOW_SEC = 30
RAPID_OUTFLOW_WINDOW_SEC = 60
SWAP_THEN_TRANSFER_SLOT_DELTA = 2
MULTI_VICTIM_MIN_SENDERS = 3
MULTI_VICTIM_MAX_RECEIVERS = 2

SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
# Known DEX / swap program IDs (mainnet)
SWAP_PROGRAM_IDS = frozenset({
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter v6
    "JUP5cHjnnCx2DppVsufsLrXs8EBZeEZz2j1o2HvLF4n4",   # Jupiter v4
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",   # Raydium AMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",   # Raydium CLMM
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",   # Serum
})


def _rpc_url() -> str:
    load_blockid_env()
    return get_solana_rpc_url()


def _rpc_post(url: str, method: str, params: list[Any]) -> dict[str, Any] | None:
    payload = {"jsonrpc": "2.0", "id": "blockid-drainer", "method": method, "params": params}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                print("[drainer] rate limit (429), waiting", RETRY_DELAY_SEC, "s")
                time.sleep(RETRY_DELAY_SEC)
                continue
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print("[drainer] request error:", e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            return None
        if data.get("error"):
            print("[drainer] RPC error:", data.get("error"))
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
    out: list[str] = []
    inner = tx.get("transaction") or tx
    msg = inner.get("message") or {}
    for k in msg.get("accountKeys") or msg.get("staticAccountKeys") or []:
        if isinstance(k, str) and len(k) >= 32:
            out.append(k)
        elif isinstance(k, dict) and k.get("pubkey"):
            out.append(str(k["pubkey"]))
    for role in ("writable", "readonly"):
        for addr in (msg.get("loadedAddresses") or {}).get(role) or []:
            if isinstance(addr, str) and len(addr) >= 32:
                out.append(addr)
    return out


def _program_ids_from_tx(tx: dict) -> set[str]:
    ids: set[str] = set()
    msg = (tx.get("transaction") or tx).get("message") or {}
    for ix in msg.get("instructions") or []:
        pid = ix.get("programId") or ix.get("program")
        if pid and isinstance(pid, str):
            ids.add(pid)
    return ids


def _has_token_transfer_or_authority(tx: dict) -> bool:
    msg = (tx.get("transaction") or tx).get("message") or {}
    for ix in msg.get("instructions") or []:
        if (ix.get("programId") or ix.get("program")) != TOKEN_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if not isinstance(parsed, dict):
            continue
        t = (parsed.get("type") or "").strip().lower()
        if t in ("transfer", "transferChecked", "approve", "approveChecked", "setAuthority"):
            return True
    return False


def _has_swap_instruction(tx: dict) -> bool:
    return bool(_program_ids_from_tx(tx) & SWAP_PROGRAM_IDS)


def _transfers_from_tx(tx: dict, wallet: str) -> list[tuple[str, str]]:
    """List of (source, destination) for native transfers where wallet is source."""
    msg = (tx.get("transaction") or tx).get("message") or {}
    keys = _account_keys_from_tx(tx)
    out: list[tuple[str, str]] = []
    for ix in msg.get("instructions") or []:
        if (ix.get("programId") or ix.get("program")) != SYSTEM_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if not isinstance(parsed, dict) or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        src = (info.get("source") or info.get("from") or "").strip()
        dst = (info.get("destination") or info.get("to") or "").strip()
        if src == wallet and dst:
            out.append((src, dst))
    if not out and len(keys) >= 2 and keys[0] == wallet:
        out.append((keys[0], keys[1]))
    return out


def _inbound_sender_from_tx(tx: dict, wallet: str) -> str | None:
    """If wallet is destination of a native transfer, return source."""
    msg = (tx.get("transaction") or tx).get("message") or {}
    keys = _account_keys_from_tx(tx)
    for ix in msg.get("instructions") or []:
        if (ix.get("programId") or ix.get("program")) != SYSTEM_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if not isinstance(parsed, dict) or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        src = (info.get("source") or info.get("from") or "").strip()
        dst = (info.get("destination") or info.get("to") or "").strip()
        if dst == wallet and src:
            return src
    if len(keys) >= 2 and keys[1] == wallet:
        return keys[0]
    return None


def fetch_tx_records(url: str, wallet: str, max_tx: int) -> list[dict]:
    """Fetch recent txs; each record has blockTime, slot, program_ids, approval_like, is_outgoing, to_addr, from_addr (inbound)."""
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
            if not tx:
                records.append({
                    "blockTime": block_time if isinstance(block_time, (int, float)) else None,
                    "slot": slot if isinstance(slot, (int, float)) else None,
                    "program_ids": set(),
                    "approval_like": False,
                    "has_swap": False,
                    "outgoing_to": [],
                    "inbound_from": None,
                })
                continue
            program_ids = _program_ids_from_tx(tx)
            approval_like = _has_token_transfer_or_authority(tx)
            has_swap = _has_swap_instruction(tx)
            transfers = _transfers_from_tx(tx, wallet)
            outgoing_to = [dst for _s, dst in transfers]
            inbound_from = _inbound_sender_from_tx(tx, wallet)
            records.append({
                "blockTime": block_time if isinstance(block_time, (int, float)) else None,
                "slot": slot if isinstance(slot, (int, float)) else None,
                "program_ids": program_ids,
                "approval_like": approval_like,
                "has_swap": has_swap,
                "outgoing_to": outgoing_to,
                "inbound_from": inbound_from,
            })
        if len(sigs) < SIGS_LIMIT:
            break
        before = sigs[-1].get("signature") if sigs else None
        if not before:
            break
    return records


def compute_approval_like_count(records: list[dict]) -> int:
    """Repeated token transfers or authority changes within short time."""
    times = sorted([r["blockTime"] for r in records if r.get("approval_like") and r.get("blockTime") is not None])
    if len(times) < 2:
        return 0
    count = 0
    for i, t in enumerate(times):
        for j, u in enumerate(times):
            if i != j and 0 < abs(u - t) <= APPROVAL_LIKE_WINDOW_SEC:
                count += 1
                break
    return count


def compute_rapid_outflow_count(records: list[dict]) -> int:
    """Multiple outgoing transfers within RAPID_OUTFLOW_WINDOW_SEC."""
    outgoing = [(r["blockTime"], r["outgoing_to"]) for r in records if r.get("blockTime") is not None and r.get("outgoing_to")]
    if not outgoing:
        return 0
    outgoing.sort(key=lambda x: x[0])
    count = 0
    for i, (t, _) in enumerate(outgoing):
        for j, (u, _) in enumerate(outgoing):
            if i != j and 0 <= u - t <= RAPID_OUTFLOW_WINDOW_SEC:
                count += 1
                break
    return count


def compute_multi_victim_pattern(records: list[dict]) -> int:
    """1 if wallet receives from many senders then sends to single/few receivers; else 0."""
    unique_senders: set[str] = set()
    unique_receivers: set[str] = set()
    for r in records:
        inbound = r.get("inbound_from")
        if inbound is not None and str(inbound).strip():
            unique_senders.add(str(inbound).strip())
        for dst in r.get("outgoing_to") or []:
            if dst and str(dst).strip():
                unique_receivers.add(str(dst).strip())
    if len(unique_senders) >= MULTI_VICTIM_MIN_SENDERS and len(unique_receivers) <= MULTI_VICTIM_MAX_RECEIVERS:
        return 1
    return 0


def compute_new_contract_interaction_count(records: list[dict]) -> int:
    """Program IDs seen in second half of window that were not in first half."""
    with_slot = [r for r in records if r.get("slot") is not None]
    if len(with_slot) < 2:
        return 0
    with_slot.sort(key=lambda x: x["slot"])
    mid = len(with_slot) // 2
    known = set()
    for r in with_slot[:mid]:
        known.update(r.get("program_ids") or set())
    new_count = 0
    for r in with_slot[mid:]:
        for pid in (r.get("program_ids") or set()):
            if pid not in known:
                new_count += 1
                known.add(pid)
    return new_count


def compute_swap_then_transfer_pattern(records: list[dict]) -> int:
    """Count: swap in a tx then outgoing transfer within 2 blocks."""
    with_slot = [r for r in records if r.get("slot") is not None]
    if not with_slot:
        return 0
    by_slot = sorted(with_slot, key=lambda x: x["slot"])
    count = 0
    for i, r in enumerate(by_slot):
        if not r.get("has_swap"):
            continue
        slot_i = float(r["slot"]) if r["slot"] is not None else 0
        for j in range(i + 1, min(i + 1 + SWAP_THEN_TRANSFER_SLOT_DELTA + 1, len(by_slot))):
            other = by_slot[j]
            slot_j = float(other["slot"]) if other.get("slot") is not None else 0
            if slot_j - slot_i > SWAP_THEN_TRANSFER_SLOT_DELTA:
                break
            if other.get("outgoing_to"):
                count += 1
                break
    return count


def compute_percent_to_same_cluster(records: list[dict]) -> float:
    """Percent of outgoing transfers (by tx count) to the single most common destination."""
    all_dests: list[str] = []
    for r in records:
        all_dests.extend(r.get("outgoing_to") or [])
    if not all_dests:
        return 0.0
    cnt = Counter(all_dests)
    most_common_count = cnt.most_common(1)[0][1] if cnt else 0
    return round(100.0 * most_common_count / len(all_dests), 2)


def drainer_features_for_wallet(url: str, wallet: str, max_tx: int) -> dict[str, Any]:
    """Fetch tx history and compute all drainer heuristic features."""
    records = fetch_tx_records(url, wallet, max_tx)
    return {
        "wallet": wallet,
        "approval_like_count": compute_approval_like_count(records),
        "rapid_outflow_count": compute_rapid_outflow_count(records),
        "multi_victim_pattern": compute_multi_victim_pattern(records),
        "new_contract_interaction_count": compute_new_contract_interaction_count(records),
        "swap_then_transfer_pattern": compute_swap_then_transfer_pattern(records),
        "percent_to_same_cluster": compute_percent_to_same_cluster(records),
    }


def load_wallets(path: Path) -> list[str]:
    out: list[str] = []
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w and row:
                vals = list(row.values())
                w = (str(vals[0]).strip() if vals else "")
            if w and w.lower() != "wallet":
                out.append(w)
    return out


def _use_dummy_and_exit() -> int:
    """Use devnet dummy drainer_features.csv when RPC unavailable. Return 0 on success."""
    dummy_dir = get_devnet_dummy_dir()
    src = dummy_dir / "drainer_features.csv"
    if not src.exists():
        print("[drainer] ERROR: devnet dummy drainer_features.csv not found:", src)
        return 1
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, OUTPUT_CSV)
    print("[drainer] Using devnet dummy dataset (BLOCKID_USE_DUMMY_DATA=1 or RPC unavailable)")
    print("[drainer] saved dummy to", OUTPUT_CSV)
    return 0


def main() -> int:
    load_blockid_env()
    logger.info("module_start", module="drainer_detection")

    if use_devnet_dummy_data():
        return _use_dummy_and_exit()

    url = _rpc_url()

    if not WALLETS_CSV.exists():
        print("[drainer] ERROR: wallets not found:", WALLETS_CSV)
        print("[drainer] Example: CSV with header 'wallet' and one base58 address per line.")
        return 1

    wallets = load_wallets(WALLETS_CSV)
    if not wallets:
        print("[drainer] ERROR: no wallets in", WALLETS_CSV)
        return 1

    print("[drainer] wallets:", len(wallets), "| max tx per wallet:", MAX_TX_PER_WALLET)
    rows = []
    for i, wallet in enumerate(wallets):
        short = (wallet[:20] + "...") if len(wallet) > 20 else wallet
        print("[drainer]", i + 1, "/", len(wallets), short)
        try:
            row = drainer_features_for_wallet(url, wallet, MAX_TX_PER_WALLET)
            rows.append(row)
        except Exception as e:
            print("[drainer] error for", short, ":", e)
            rows.append({
                "wallet": wallet,
                "approval_like_count": 0,
                "rapid_outflow_count": 0,
                "multi_victim_pattern": 0,
                "new_contract_interaction_count": 0,
                "swap_then_transfer_pattern": 0,
                "percent_to_same_cluster": 0.0,
            })

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)
    print("[drainer] saved", len(rows), "rows to", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
