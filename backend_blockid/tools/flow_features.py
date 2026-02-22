import csv
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path("backend_blockid/data")
TX_FILE = DATA_DIR / "transactions.csv"
OUT_FILE = DATA_DIR / "flow_features.csv"


def load_transactions():
    if not TX_FILE.exists():
        print("❌ transactions.csv tidak ditemukan")
        return []

    with open(TX_FILE, newline="") as f:
        return list(csv.DictReader(f))


def compute_features(transactions):
    tx_count = defaultdict(int)
    failed_tx = defaultdict(int)

    for tx in transactions:
        wallet = tx["wallet"]
        tx_count[wallet] += 1
        if tx["err"] not in ("None", "", None):
            failed_tx[wallet] += 1

    rows = []
    for w in tx_count:
        rows.append({
            "wallet": w,
            "total_tx": tx_count[w],
            "failed_tx": failed_tx[w],
            "failure_ratio": round(failed_tx[w] / tx_count[w], 3)
        })

    return rows


def main():
    txs = load_transactions()
    print(f"[flow_features] tx loaded: {len(txs)}")

    if not txs:
        print("❌ Tidak ada transaksi")
        return 1

    rows = compute_features(txs)

    with open(OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["wallet", "total_tx", "failed_tx", "failure_ratio"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ flow_features.csv dibuat ({len(rows)} wallet)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())