import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

API_KEY = os.getenv("HELIUS_API_KEY")
if not API_KEY:
    raise Exception("❌ HELIUS_API_KEY not found")

WALLET = "8X35rQUK2u9hfn8rMPwwr6ZSEUhbmfDPEapp589XyoM1"

URL = f"https://api.helius.xyz/v0/addresses/{WALLET}/transactions?api-key={API_KEY}&page=1"

print("🚀 Fetching wallet tx history...")

res = requests.get(URL)
data = res.json()

print("\n===== RAW RESPONSE =====")
print(data)
print("========================\n")

txs = data if isinstance(data, list) else data.get("transactions", [])

print("TYPE:", type(txs))

if not txs:
    print("No transactions found")
    exit()

timestamps = []

for tx in txs:
    if not isinstance(tx, dict):
        print("⚠ Skip non-dict tx:", tx)
        continue

    ts = tx.get("timestamp") or tx.get("blockTime")
    if ts:
        timestamps.append(ts)

if not timestamps:
    print("❌ No valid timestamps found")
    print("Raw response:", txs[:3])
    exit()

first_tx = min(timestamps)
last_tx = max(timestamps)

now = datetime.now(timezone.utc)
wallet_age_days = (now - datetime.fromtimestamp(first_tx, tz=timezone.utc)).days

print("First tx:", first_tx)
print("Last tx:", last_tx)
print("Wallet age days:", wallet_age_days)

from backend_blockid.database.repositories import save_wallet_meta
import time

meta = {
    "wallet": WALLET,
    "first_tx_ts": first_tx,
    "last_tx_ts": last_tx,
    "wallet_age_days": wallet_age_days,
    "last_scam_tx_ts": None,
    "last_scan_time": int(time.time()),
    "cluster_id": None,
}

save_wallet_meta(meta)

print("✅ wallet_meta saved")
