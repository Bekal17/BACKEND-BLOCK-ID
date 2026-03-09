import os
import requests
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from backend_blockid.database.repositories import (
    save_wallet_meta,
    get_wallet_meta,
    get_cluster_wallets,
)
from backend_blockid.tools.helius_client import helius_request
from backend_blockid.oracle.wallet_scan_prioritizer import update_scan_timestamp

load_dotenv()
API_KEY = os.getenv("HELIUS_API_KEY")


def fetch_wallet_txs(wallet):
    url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={API_KEY}"
    res = requests.get(url)
    data = res.json()

    if not isinstance(data, list):
        print("Helius error:", data)
        return []

    helius_request("addresses/transactions", wallet, request_count=1)
    return data


def compute_wallet_age_days(first_tx_ts):
    if not first_tx_ts:
        return 0
    now = datetime.now(timezone.utc)
    tx_time = datetime.fromtimestamp(first_tx_ts, tz=timezone.utc)
    return (now - tx_time).days


async def scan_wallet(wallet):
    meta = await get_wallet_meta(wallet)

    txs = fetch_wallet_txs(wallet)
    if not txs:
        return

    timestamps = [
        tx.get("timestamp") or tx.get("blockTime")
        for tx in txs
        if isinstance(tx, dict)
    ]

    if not timestamps:
        return

    first_tx = min(timestamps)
    last_tx = max(timestamps)

    if meta and meta.get("last_tx_ts") == last_tx:
        print(f"[SKIP] No new tx for {wallet}")
        return

    wallet_age_days = compute_wallet_age_days(first_tx)

    await save_wallet_meta({
        "wallet": wallet,
        "first_tx_ts": first_tx,
        "last_tx_ts": last_tx,
        "wallet_age_days": wallet_age_days,
        "last_scan_time": int(time.time()),
    })
    await update_scan_timestamp(wallet)

    print(f"[UPDATED] {wallet} age={wallet_age_days}")


async def scan_cluster(cluster_id):
    wallets = await get_cluster_wallets(cluster_id)

    print(f"Scanning cluster {cluster_id}, wallets={len(wallets)}")

    for w in wallets:
        await scan_wallet(w)
