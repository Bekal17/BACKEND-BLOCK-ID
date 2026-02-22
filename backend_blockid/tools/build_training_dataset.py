"""
Build BlockID ML training dataset by automatically collecting active wallets.

How to run:
    From project root (with .env configured for RPC):

        python -m backend_blockid.tools.build_training_dataset --good 500 --scam 500 --days 30

    or:

        python backend_blockid/tools/build_training_dataset.py --good 500 --scam 500 --days 30

Required env vars:
    - HELIUS_API_KEY       (preferred; e.g. https://mainnet.helius-rpc.com/?api-key=...)
    OR
    - QUICKNODE_RPC_URL    (full Solana JSON-RPC URL)
    OR
    - SOLANA_RPC_URL       (fallback RPC URL, e.g. https://api.mainnet-beta.solana.com)

Optional:
    - LOG_LEVEL / LOG_FORMAT as used by backend_blockid.blockid_logging

Expected runtime:
    - Depends on RPC speed and the --good / --days parameters.
      For ~500 good wallets over the last 30 days, expect several minutes,
      as the script scans recent blocks and transactions with conservative
      rate-limiting and retry logic.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

# Load .env early
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)


DEFAULT_GOOD_LIMIT = 500
DEFAULT_SCAM_LIMIT = 500
DEFAULT_DAYS = 30

REQUEST_TIMEOUT = 20.0
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5
RPC_CONCURRENCY = 4

# We do NOT try to scan the entire last 30 days (too many blocks).
# Instead, we scan a fixed window of recent slots and rely on blockTime
# cutoffs and tx_count>=20 filter to surface active wallets.
SLOT_WINDOW = int(os.getenv("BLOCKID_SLOT_WINDOW", "500000"))
logger.info("slot_window", value=SLOT_WINDOW)


def _rpc_url() -> str:
    """
    Resolve the RPC URL in priority order:
      1. QUICKNODE_RPC_URL
      2. HELIUS_API_KEY (mainnet.helius-rpc.com)
      3. SOLANA_RPC_URL
    """
    quicknode = (os.getenv("QUICKNODE_RPC_URL") or "").strip()
    if quicknode:
        return quicknode
    helius_key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if helius_key:
        # Helius mainnet RPC
        return f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
    solana = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if solana:
        return solana
    raise RuntimeError(
        "No RPC URL configured. Set QUICKNODE_RPC_URL, HELIUS_API_KEY, or SOLANA_RPC_URL in your environment/.env."
    )


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code == 429:
                # Rate-limited; exponential backoff
                await asyncio.sleep(RETRY_BACKOFF * (2 ** attempt))
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:  # noqa: BLE001
            last_err = e
            await asyncio.sleep(RETRY_BACKOFF * (2 ** attempt))
    raise last_err or RuntimeError("RPC request failed after retries")


async def _rpc(
    client: httpx.AsyncClient,
    rpc_url: str,
    method: str,
    params: List[Any],
) -> Dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = await _request_with_retry(client, "POST", rpc_url, json=body)
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data.get("result")  # type: ignore[no-any-return]


async def _get_current_slot(client: httpx.AsyncClient, rpc_url: str) -> int:
    result = await _rpc(client, rpc_url, "getSlot", [])
    return int(result)


async def _get_blocks(
    client: httpx.AsyncClient,
    rpc_url: str,
    start_slot: int,
    end_slot: int,
) -> List[int]:
    try:
        result = await _rpc(client, rpc_url, "getBlocks", [start_slot, end_slot])
        return [int(s) for s in (result or [])]
    except Exception as e:  # noqa: BLE001
        logger.warning("build_dataset_get_blocks_failed", start=start_slot, end=end_slot, error=str(e))
        return []


async def _get_block(
    client: httpx.AsyncClient,
    rpc_url: str,
    slot: int,
) -> Dict[str, Any] | None:
    try:
        result = await _rpc(
            client,
            rpc_url,
            "getBlock",
            [
                slot,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "transactionDetails": "full",
                    "rewards": False,
                },
            ],
        )
        return result if isinstance(result, dict) else None
    except Exception as e:  # noqa: BLE001
        logger.debug("build_dataset_get_block_failed", slot=slot, error=str(e))
        return None


async def _scan_recent_blocks_for_wallets(
    client: httpx.AsyncClient,
    rpc_url: str,
    good_limit: int,
    days: int,
) -> Dict[str, Dict[str, int]]:
    """
    Scan a sliding window of recent blocks and build per-wallet stats:

        stats[wallet] = {\"tx_count\": int, \"first_seen\": ts, \"last_seen\": ts}

    We scan newest blocks first, enforce a cutoff based on `days`, and stop
    when we've likely collected enough active wallets.
    """
    now_ts = int(time.time())
    cutoff_ts = now_ts - days * 86400

    current_slot = await _get_current_slot(client, rpc_url)
    start_slot = max(current_slot - SLOT_WINDOW, 0)
    logger.info(
        "build_dataset_scan_slots_start",
        start_slot=start_slot,
        end_slot=current_slot,
        days=days,
        cutoff_ts=cutoff_ts,
    )

    blocks = await _get_blocks(client, rpc_url, start_slot, current_slot)
    if not blocks:
        return {}

    # Most recent blocks last in list; scan from newest backwards
    blocks_to_scan = list(reversed(blocks))

    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"tx_count": 0, "first_seen": now_ts, "last_seen": 0})

    sem = asyncio.Semaphore(RPC_CONCURRENCY)

    async def process_block(slot: int) -> None:
        async with sem:
            block = await _get_block(client, rpc_url, slot)
        if not block:
            return
        block_time = block.get("blockTime")
        if not block_time or block_time < cutoff_ts:
            return
        txs = block.get("transactions") or []
        for tx_entry in txs:
            try:
                tx = tx_entry.get("transaction") or {}
                meta = tx_entry.get("meta") or {}
                # Skip failed transactions
                if meta.get("err") is not None:
                    continue
                msg = tx.get("message") or {}
                account_keys = msg.get("accountKeys") or []
                if not account_keys:
                    continue
                fee_payer = account_keys[0]
                if isinstance(fee_payer, dict):
                    wallet = str(fee_payer.get("pubkey") or "")
                else:
                    wallet = str(fee_payer)
                wallet = wallet.strip()
                if not wallet:
                    continue
                # Update stats
                s = stats[wallet]
                s["tx_count"] += 1
                s["first_seen"] = min(s["first_seen"], block_time)
                s["last_seen"] = max(s["last_seen"], block_time)
            except Exception:  # noqa: BLE001
                continue

    # Process blocks sequentially (but each block fetch is limited by semaphore)
    for slot in blocks_to_scan:
        block_time_ok = True  # we check in process_block
        if not block_time_ok:
            continue
        await process_block(slot)
        # Heuristic: once we have a lot of wallets, we can stop early
        if len(stats) > good_limit * 5:
            break

    logger.info("build_dataset_scan_slots_done", wallets=len(stats))
    return stats


def _filter_active_wallets(
    stats: Dict[str, Dict[str, int]],
    good_limit: int,
    min_tx: int = 20,
    min_age_days: int = 7,
) -> List[str]:
    now = int(time.time())
    candidates: List[Tuple[str, int]] = []
    for wallet, s in stats.items():
        tx_count = s.get("tx_count", 0)
        if tx_count < min_tx:
            continue
        first_seen = s.get("first_seen") or now
        age_days = max(0, (now - first_seen) // 86400)
        if age_days < min_age_days:
            continue
        candidates.append((wallet, tx_count))
    # Sort by tx_count desc, then take top N
    candidates.sort(key=lambda x: -x[1])
    good_wallets = [w for w, _ in candidates[:good_limit]]
    logger.info(
        "build_dataset_filter_active_wallets",
        total=len(stats),
        eligible=len(candidates),
        selected=len(good_wallets),
        min_tx=min_tx,
        min_age_days=min_age_days,
    )
    return good_wallets


async def collect_active_wallets_async(
    limit: int = DEFAULT_GOOD_LIMIT,
    days: int = DEFAULT_DAYS,
) -> List[str]:
    """
    Collect `limit` active wallets from recent blocks:
      - Scan recent blocks via JSON-RPC
      - Count wallets with >=20 transactions
      - Only consider activity within the last `days`
    """
    rpc_url = _rpc_url()
    logger.info("build_dataset_collect_active_wallets_start", limit=limit, days=days, rpc_url=rpc_url)
    async with httpx.AsyncClient() as client:
        stats = await _scan_recent_blocks_for_wallets(client, rpc_url, good_limit=limit, days=days)
    good_wallets = _filter_active_wallets(stats, good_limit=limit, min_tx=20, min_age_days=7)
    logger.info("build_dataset_collect_active_wallets_done", count=len(good_wallets))
    return good_wallets


def collect_active_wallets(limit: int = DEFAULT_GOOD_LIMIT, days: int = DEFAULT_DAYS) -> List[str]:
    """Synchronous wrapper for collect_active_wallets_async."""
    try:
        return asyncio.run(collect_active_wallets_async(limit=limit, days=days))
    except Exception as e:  # noqa: BLE001
        logger.exception("build_dataset_collect_active_wallets_error", error=str(e))
        return []


def load_known_scam_wallets() -> List[Tuple[str, str]]:
    """
    Load known scam wallets from scam_wallets.csv, removing duplicates.

    Expected CSV format:
        wallet,label
        wallet1,rug_pull_deployer
        wallet2,phishing_drain

    Returns:
        List of (wallet, label)
    """
    # Place scam_wallets.csv under backend_blockid/data by convention
    base_dir = Path(__file__).resolve().parent.parent
    csv_path = base_dir / "data" / "scam_wallets.csv"
    if not csv_path.is_file():
        logger.warning("build_dataset_scam_csv_missing", path=str(csv_path))
        return []

    seen: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if "wallet" not in (reader.fieldnames or []):
                logger.warning("build_dataset_scam_csv_no_wallet_header", path=str(csv_path))
                return []
            for row in reader:
                wallet = (row.get("wallet") or "").strip()
                if not wallet:
                    continue
                label = (row.get("label") or "scam").strip() or "scam"
                if wallet not in seen:
                    seen[wallet] = label
    except Exception as e:  # noqa: BLE001
        logger.exception("build_dataset_scam_csv_failed", path=str(csv_path), error=str(e))
        return []

    scams = [(w, lbl) for w, lbl in seen.items()]
    logger.info("build_dataset_scam_wallets_loaded", count=len(scams), path=str(csv_path))
    return scams


def save_dataset(
    good_wallets: Iterable[str],
    scam_wallets: Iterable[Tuple[str, str]],
    out_path: Path,
) -> None:
    """
    Save combined dataset:

        wallet,label
        wallet1,good
        wallet2,good
        wallet3,rug_pull_deployer
        wallet4,phishing_drain
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "label"])
        good_count = 0
        for w in good_wallets:
            w = (w or "").strip()
            if not w:
                continue
            writer.writerow([w, "good"])
            good_count += 1
        scam_count = 0
        for w, lbl in scam_wallets:
            w = (w or "").strip()
            if not w:
                continue
            label = (lbl or "scam").strip() or "scam"
            writer.writerow([w, label])
            scam_count += 1

    logger.info(
        "build_dataset_saved",
        path=str(out_path),
        good_count=good_count,
        scam_count=scam_count,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build BlockID ML training dataset from active and known scam wallets.",
    )
    parser.add_argument(
        "--good",
        type=int,
        default=DEFAULT_GOOD_LIMIT,
        help=f"Number of good (active) wallets to collect (default: {DEFAULT_GOOD_LIMIT})",
    )
    parser.add_argument(
        "--scam",
        type=int,
        default=DEFAULT_SCAM_LIMIT,
        help=f"Maximum number of known scam wallets to include (default: {DEFAULT_SCAM_LIMIT})",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Activity window in days for good wallets (default: {DEFAULT_DAYS})",
    )
    args = parser.parse_args()

    try:
        good_wallets = collect_active_wallets(limit=args.good, days=args.days)
        scam_wallets_all = load_known_scam_wallets()
        scam_wallets = scam_wallets_all[: args.scam] if args.scam > 0 else []

        # Place wallet_labels_auto.csv next to backend_blockid (project root-level)
        project_root = Path(__file__).resolve().parents[2]
        out_path = project_root / "wallet_labels_auto.csv"
        save_dataset(good_wallets, scam_wallets, out_path)

        print(f"Training dataset saved to: {out_path}")
        print(f"  good wallets: {len(good_wallets)}")
        print(f"  scam wallets: {len(scam_wallets)}")
        return 0
    except Exception as e:  # noqa: BLE001
        logger.exception("build_dataset_main_failed", error=str(e))
        print("ERROR:", e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

