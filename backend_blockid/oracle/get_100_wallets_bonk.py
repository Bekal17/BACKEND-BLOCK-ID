import os, csv, requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HELIUS_API_KEY")
RPC = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}"

TARGET = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"  # USDC mint

wallets = set()
before = None

print("🚀 Fetching wallets from transaction history...")

while len(wallets) < 100:
    payload = {
        "jsonrpc":"2.0",
        "id":1,
        "method":"getSignaturesForAddress",
        "params":[TARGET, {"limit":50, "before":before}]
    }

    r = requests.post(RPC, json=payload).json()
    txs = r.get("result", [])

    if not txs:
        break

    for tx in txs:
        wallets.add(tx.get("signature"))
    before = txs[-1]["signature"]

print("wallet count:", len(wallets))

os.makedirs("backend_blockid/data", exist_ok=True)
with open("backend_blockid/data/test_wallets_100.csv","w",newline="") as f:
    w = csv.writer(f)
    w.writerow(["wallet"])
    for wlt in wallets:
        w.writerow([wlt])

print("✅ Saved wallets")
