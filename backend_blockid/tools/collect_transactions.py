import csv
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.devnet.solana.com")

DATA_DIR = Path("backend_blockid/data")
OUTPUT_FILE = DATA_DIR / "transactions.csv"

INPUT_FILES = [
    DATA_DIR / "manual_wallets.csv",
    DATA_DIR / "scam_wallets.csv",
]


def get_wallets():
    wallets = set()
    for file in INPUT_FILES:
        if file.exists():
            with open(file, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    wallets.add(row["wallet"])
    return list(wallets)


def fetch_signatures(wallet):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [wallet, {"limit": 50}],
    }

    try:
        r = requests.post(RPC_URL, json=payload, timeout=10)
        data = r.json()
        return data.get("result", [])
    except Exception:
        return []


def main():
    wallets = get_wallets()
    print(f"Wallets loaded: {len(wallets)}")

    rows = []

    for w in wallets:
        sigs = fetch_signatures(w)
        print(f"{w[:6]}... → {len(sigs)} tx")
        for s in sigs:
            rows.append({
                "wallet": w,
                "signature": s.get("signature"),
                "slot": s.get("slot"),
                "err": s.get("err"),
            })

    if not rows:
        print("❌ No transactions found.")
        print("👉 Pastikan wallet DEVNET pernah transaksi / airdrop / transfer")
        return 1

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["wallet", "signature", "slot", "err"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Saved {len(rows)} tx → {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())