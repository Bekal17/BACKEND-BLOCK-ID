"""
Download transaction edges (from, to) for a list of wallets using Helius RPC.

Uses getSignaturesForAddress + getTransaction (jsonParsed), extracts native transfer
(sender, receiver) edges. Writes backend_blockid/data/transactions.csv with columns from, to.
When BLOCKID_USE_DUMMY_DATA=1 or Helius/RPC unavailable, uses devnet dummy dataset.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import time
from pathlib import Path
from typing import Any

import requests

from backend_blockid.config.env import (
    get_devnet_dummy_dir,
    get_solana_rpc_url,
    load_blockid_env,
    print_blockid_startup,
    use_devnet_dummy_data,
)

# Paths: script in backend_blockid/oracle/, data in backend_blockid/data/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
ROOT = _SCRIPT_DIR.parents[2]
DEFAULT_WALLETS_CSV = _DATA_DIR / "manual_wallets.csv"
OUTPUT_CSV = _DATA_DIR / "transactions.csv"

# RPC
SIGS_LIMIT = 50
TX_LIMIT_PER_WALLET = 200  # max transactions to fetch per wallet (SIGS_LIMIT * pages)
DELAY_SEC = 0.25
RETRY_DELAY_SEC = 2.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"


def _rpc_url() -> str | None:
    load_blockid_env()
    return get_solana_rpc_url()


def _rpc_post(url: str, method: str, params: list[Any]) -> dict[str, Any] | None:
    payload = {"jsonrpc": "2.0", "id": "blockid-fetch-edges", "method": method, "params": params}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                print("[fetch_edges] rate limit (429), waiting", RETRY_DELAY_SEC, "s...")
                time.sleep(RETRY_DELAY_SEC)
                continue
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print("[fetch_edges] request error:", e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
                continue
            return None
        err = data.get("error")
        if err:
            print("[fetch_edges] RPC error:", err)
            return None
        return data
    return None


def _get_signatures(url: str, address: str, before: str | None) -> list[dict] | None:
    params: list[Any] = [address, {"limit": SIGS_LIMIT}]
    if before:
        params[1]["before"] = before
    data = _rpc_post(url, "getSignaturesForAddress", params)
    if data is None:
        return None
    result = data.get("result")
    return result if isinstance(result, list) else []


def _get_transaction(url: str, signature: str) -> dict | None:
    # jsonParsed to get decoded instructions
    params = [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    data = _rpc_post(url, "getTransaction", params)
    if data is None:
        return None
    return data.get("result")


def _account_keys_from_parsed_tx(tx: dict) -> list[str]:
    """Extract account keys from parsed tx (legacy or versioned message)."""
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


def _extract_transfer_edges(tx: dict) -> list[tuple[str, str]]:
    """
    Extract (from, to) edges from native SOL transfer instructions (System Program).
    """
    edges: list[tuple[str, str]] = []
    keys = _account_keys_from_parsed_tx(tx)
    if len(keys) < 2:
        return edges
    for ix in _instructions_from_parsed_tx(tx):
        prog = ix.get("programId") or ix.get("program")
        if prog != SYSTEM_PROGRAM_ID:
            continue
        parsed = ix.get("parsed") or ix
        if isinstance(parsed, dict) and parsed.get("type") == "transfer":
            info = parsed.get("info") or {}
            src = (info.get("source") or info.get("from") or "").strip()
            dst = (info.get("destination") or info.get("to") or "").strip()
            if src and dst and src != dst:
                edges.append((src, dst))
    # Fallback: if no parsed transfer, use first account as from and second as to (common for simple transfer)
    if not edges and len(keys) >= 2:
        edges.append((keys[0], keys[1]))
    return edges


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


def main() -> int:
    load_blockid_env()
    print_blockid_startup("fetch_tx_edges_helius")

    ap = argparse.ArgumentParser(description="Fetch transaction edges (from, to) via Helius RPC")
    ap.add_argument("--wallets", type=str, default=str(DEFAULT_WALLETS_CSV), help="CSV with wallet column")
    args = ap.parse_args()

    if use_devnet_dummy_data():
        dummy_dir = get_devnet_dummy_dir()
        src = dummy_dir / "transactions.csv"
        if not src.exists():
            print("[fetch_edges] ERROR: devnet dummy transactions.csv not found:", src)
            return 1
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, OUTPUT_CSV)
        print("[fetch_edges] Using devnet dummy dataset (BLOCKID_USE_DUMMY_DATA=1)")
        print("[fetch_edges] saved dummy to", OUTPUT_CSV)
        return 0

    url = _rpc_url()
    if not url:
        print("[fetch_edges] ERROR: set HELIUS_API_KEY or SOLANA_RPC_URL; or BLOCKID_USE_DUMMY_DATA=1 for dummy")
        return 1

    wallets_path = Path(args.wallets)
    if not wallets_path.is_absolute():
        wallets_path = ROOT / wallets_path
    wallets = load_wallets(wallets_path)
    if not wallets:
        print("[fetch_edges] ERROR: no wallets in", wallets_path)
        return 1

    print("[fetch_edges] wallets:", len(wallets), "| output:", OUTPUT_CSV)
    seen_edges: set[tuple[str, str]] = set()
    total_txs = 0

    for i, wallet in enumerate(wallets):
        print("[fetch_edges] wallet", i + 1, "/", len(wallets), wallet[:16] + "...")
        before: str | None = None
        n_sigs = 0
        while n_sigs < TX_LIMIT_PER_WALLET:
            sigs = _get_signatures(url, wallet, before)
            time.sleep(DELAY_SEC)
            if not sigs:
                break
            for sig_info in sigs:
                sig = sig_info.get("signature")
                if not sig or not isinstance(sig, str):
                    continue
                tx = _get_transaction(url, sig)
                time.sleep(DELAY_SEC)
                if not tx:
                    continue
                total_txs += 1
                for a, b in _extract_transfer_edges(tx):
                    if a and b:
                        seen_edges.add((a, b))
            n_sigs += len(sigs)
            if len(sigs) < SIGS_LIMIT:
                break
            before = sigs[-1].get("signature") if sigs else None
            if not before:
                break

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to"])
        for a, b in sorted(seen_edges):
            w.writerow([a, b])

    print("[fetch_edges] transactions considered:", total_txs, "| unique edges:", len(seen_edges), "| saved:", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
