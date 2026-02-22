"""
Collect unique Solana wallets active in the last N days via CoinGecko + Helius/QuickNode.

How to run:
    From project root (with .env configured):
        py -m backend_blockid.tools.find_active_wallets --limit 500 --days 30
    Or:
        py backend_blockid/tools/find_active_wallets.py --limit 200 --days 30

Required env vars:
    HELIUS_API_KEY or QUICKNODE_RPC_URL  (at least one for RPC; Helius preferred for token transfers)
    COINGECKO_API_KEY                    (optional; improves rate limits for CoinGecko)

Expected runtime:
    ~5–15 minutes for 500 wallets depending on RPC and rate limits (CoinGecko ~50 tokens,
    then RPC calls per mint with retries and backoff).

Output CSV (backend_blockid/data/active_wallets.csv):
    wallet, tx_count, first_seen, last_seen  (first_seen/last_seen are Unix timestamps)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

# Load .env before other imports that may use os.getenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DEFAULT_LIMIT = 500
DEFAULT_DAYS = 30
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0
RATE_LIMIT_DELAY = 0.5
COINGECKO_TOP_N = 50
MIN_TX_COUNT = 20
MIN_WALLET_AGE_DAYS = 7
BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# Known program / system accounts (not wallets)
KNOWN_PROGRAM_IDS = frozenset({
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "MetaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "SysvarRent111111111111111111111111111111111",
})


def _rpc_url() -> str | None:
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return (os.getenv("QUICKNODE_RPC_URL") or "").strip() or None


def _coingecko_headers() -> dict[str, str]:
    key = (os.getenv("COINGECKO_API_KEY") or "").strip()
    if key:
        return {"x-cg-demo-api-key": key}
    return {}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _valid_base58(s: str) -> bool:
    return bool(s and BASE58_RE.match(s.strip()) and s.strip() not in KNOWN_PROGRAM_IDS)


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = await client.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 429:
                await asyncio.sleep(RETRY_BACKOFF * (2 ** attempt))
                continue
            return r
        except Exception as e:
            last_err = e
            await asyncio.sleep(RETRY_BACKOFF * (2 ** attempt))
    raise last_err or RuntimeError("request failed")


async def _rpc(client: httpx.AsyncClient, rpc_url: str, method: str, params: list[Any]) -> dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = await _request_with_retry(client, "POST", rpc_url, json=body)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data.get("result")


# -----------------------------------------------------------------------------
# Step 1 — Get popular Solana token mints (CoinGecko)
# -----------------------------------------------------------------------------
async def get_popular_solana_mints(client: httpx.AsyncClient, top_n: int = COINGECKO_TOP_N) -> list[str]:
    mints: list[str] = []
    base = "https://api.coingecko.com/api/v3"
    headers = _coingecko_headers()

    # Markets: top by market cap (we'll filter Solana by platform later)
    try:
        r = await _request_with_retry(
            client, "GET",
            f"{base}/coins/markets",
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1},
            headers=headers,
        )
        r.raise_for_status()
        markets = r.json()
    except Exception as e:
        logger.warning("find_active_wallets_coingecko_markets_failed", error=str(e))
        return []

    for coin in markets[:top_n * 2]:  # fetch more to allow for non-Solana
        if len(mints) >= top_n:
            break
        cid = coin.get("id")
        if not cid:
            continue
        try:
            detail_r = await _request_with_retry(client, "GET", f"{base}/coins/{cid}", headers=headers)
            detail_r.raise_for_status()
            detail = detail_r.json()
            platforms = detail.get("platforms") or {}
            solana = platforms.get("solana")
            if isinstance(solana, str) and solana and _valid_base58(solana):
                mints.append(solana.strip())
                logger.debug("find_active_wallets_mint_added", coin_id=cid, mint=solana[:20] + "...")
        except Exception as e:
            logger.debug("find_active_wallets_coin_detail_failed", coin_id=cid, error=str(e))
        await asyncio.sleep(RATE_LIMIT_DELAY)

    logger.info("find_active_wallets_mints_collected", count=len(mints), requested=top_n)
    return mints[:top_n]


# -----------------------------------------------------------------------------
# Step 2 — Get recent transfers per mint (RPC: getSignaturesForAddress + getTransaction)
# -----------------------------------------------------------------------------
async def get_signatures_for_address(
    client: httpx.AsyncClient,
    rpc_url: str,
    address: str,
    limit: int = 1000,
    until_ts: int | None = None,
) -> list[str]:
    params: list[Any] = [address, {"limit": limit}]
    if until_ts is not None:
        params[1]["until"] = until_ts  # optional; not all RPCs support
    try:
        result = await _rpc(client, rpc_url, "getSignaturesForAddress", params)
    except Exception as e:
        logger.debug("find_active_wallets_get_sigs_failed", address=address[:20], error=str(e))
        return []
    if not result:
        return []
    sigs = []
    for item in result:
        sig = item.get("signature") if isinstance(item, dict) else getattr(item, "signature", None)
        if sig:
            sigs.append(str(sig))
    return sigs


async def get_transaction(client: httpx.AsyncClient, rpc_url: str, sig: str) -> dict[str, Any] | None:
    try:
        result = await _rpc(
            client, rpc_url, "getTransaction",
            [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        )
        return result if isinstance(result, dict) else None
    except Exception:
        return None


def _account_keys_from_tx(tx: dict[str, Any]) -> list[str]:
    out: list[str] = []
    try:
        msg = (tx.get("transaction") or {}).get("message") or {}
        keys = msg.get("accountKeys") or msg.get("account_keys") or []
        for k in keys:
            if isinstance(k, str):
                out.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if pk:
                    out.append(str(pk))
    except Exception:
        pass
    return out


async def collect_wallets_from_mint(
    client: httpx.AsyncClient,
    rpc_url: str,
    mint: str,
    sem: asyncio.Semaphore,
    days: int,
) -> dict[str, tuple[int, int, int]]:
    """Return dict wallet -> (tx_count, first_ts, last_ts) for wallets seen in this mint's txs."""
    async with sem:
        sigs = await get_signatures_for_address(client, rpc_url, mint, limit=500)
        await asyncio.sleep(RATE_LIMIT_DELAY)
    wallet_ts: dict[str, list[int]] = {}
    for sig in sigs[:200]:
        tx = await get_transaction(client, rpc_url, sig)
        if not tx:
            continue
        block_time = tx.get("blockTime")
        if not block_time:
            continue
        try:
            ts = int(block_time)
        except (TypeError, ValueError):
            continue
        keys = _account_keys_from_tx(tx)
        for k in keys:
            k = k.strip()
            if not _valid_base58(k):
                continue
            if k not in wallet_ts:
                wallet_ts[k] = []
            wallet_ts[k].append(ts)
        await asyncio.sleep(RATE_LIMIT_DELAY * 0.5)
    out: dict[str, tuple[int, int, int]] = {}
    now_ts = int(time.time())
    cutoff_ts = now_ts - days * 86400
    for w, tss in wallet_ts.items():
        tss = [t for t in tss if t >= cutoff_ts]
        if not tss:
            continue
        out[w] = (len(tss), min(tss), max(tss))
    return out


# -----------------------------------------------------------------------------
# Step 3 — Aggregate and filter
# -----------------------------------------------------------------------------
def merge_wallet_stats(
    acc: dict[str, tuple[int, int, int]],
    new: dict[str, tuple[int, int, int]],
) -> None:
    for w, (count, first, last) in new.items():
        if w in acc:
            c, f, l = acc[w]
            acc[w] = (c + count, min(f, first), max(l, last))
        else:
            acc[w] = (count, first, last)


def filter_wallets(
    stats: dict[str, tuple[int, int, int]],
    min_tx: int = MIN_TX_COUNT,
    min_age_days: int = MIN_WALLET_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
) -> list[tuple[str, int, int, int]]:
    now = int(time.time())
    out: list[tuple[str, int, int, int]] = []
    for w, (count, first_ts, last_ts) in stats.items():
        if count < min_tx:
            continue
        age_days = (now - first_ts) // 86400
        if age_days < min_age_days:
            continue
        if not _valid_base58(w):
            continue
        out.append((w, count, first_ts, last_ts))
    out.sort(key=lambda x: -x[1])
    return out[:limit]


# -----------------------------------------------------------------------------
# Step 4 — Save CSV
# -----------------------------------------------------------------------------
def save_csv(rows: list[tuple[str, int, int, int]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wallet", "tx_count", "first_seen", "last_seen"])
        for r in rows:
            w.writerow(list(r))
    logger.info("find_active_wallets_csv_saved", path=str(path), rows=len(rows))


# -----------------------------------------------------------------------------
# Optional: run_wallet_analysis for metrics
# -----------------------------------------------------------------------------
def run_metrics_optional(wallets: list[str], max_run: int = 50) -> None:
    try:
        from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
    except ImportError:
        logger.debug("find_active_wallets_run_metrics_skip", reason="import_failed")
        return
    for i, w in enumerate(wallets[:max_run]):
        try:
            data = run_wallet_analysis(w)
            logger.info(
                "find_active_wallets_metrics",
                wallet=w[:20] + "...",
                score=data.get("score"),
                risk_label=data.get("risk_label"),
            )
        except Exception as e:
            logger.debug("find_active_wallets_metrics_failed", wallet=w[:20], error=str(e))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def run(limit: int = DEFAULT_LIMIT, days: int = DEFAULT_DAYS, with_metrics: bool = False) -> Path:
    rpc_url = _rpc_url()
    if not rpc_url:
        raise RuntimeError("Set HELIUS_API_KEY or QUICKNODE_RPC_URL in .env")

    data_dir = Path(__file__).resolve().parent.parent / "data"
    out_path = data_dir / "active_wallets.csv"

    async with httpx.AsyncClient() as client:
        # Step 1
        mints = await get_popular_solana_mints(client, top_n=COINGECKO_TOP_N)
        if not mints:
            logger.warning("find_active_wallets_no_mints", hint="check COINGECKO_API_KEY or network")
            # Fallback: use a known Solana mint so we still get some data
            mints = ["So11111111111111111111111111111111111111112"]
        sem = asyncio.Semaphore(3)
        aggregated: dict[str, tuple[int, int, int]] = {}
        # Step 2
        for i, mint in enumerate(mints):
            if len(aggregated) >= limit * 2:
                break
            new = await collect_wallets_from_mint(client, rpc_url, mint, sem, days)
            merge_wallet_stats(aggregated, new)
            logger.info("find_active_wallets_mint_done", mint=mint[:20], wallets=len(aggregated))
        # Step 3
        filtered = filter_wallets(aggregated, min_tx=MIN_TX_COUNT, min_age_days=MIN_WALLET_AGE_DAYS, limit=limit)
        # Step 4
        save_csv(filtered, out_path)

    if with_metrics:
        run_metrics_optional([r[0] for r in filtered])

    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect unique Solana wallets active in the last N days (CoinGecko + Helius/QuickNode).",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help=f"Max wallets to collect (default: {DEFAULT_LIMIT})")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"Activity window in days (default: {DEFAULT_DAYS})")
    parser.add_argument("--with-metrics", action="store_true", help="Optionally run run_wallet_analysis on first 50 wallets")
    args = parser.parse_args()
    try:
        path = asyncio.run(run(limit=args.limit, days=args.days, with_metrics=args.with_metrics))
        print("OUTPUT:", path)
        return 0
    except Exception as e:
        logger.exception("find_active_wallets_failed", error=str(e))
        print("ERROR:", e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
