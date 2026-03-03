import csv
from pathlib import Path

DATA_DIR = Path("backend_blockid/data")
WALLETS_CSV = DATA_DIR / "wallets.csv"
CLUSTER_CSV = DATA_DIR / "cluster_features.csv"


def generate_cluster_features() -> None:
    if CLUSTER_CSV.exists():
        print("[generate_cluster_features] cluster_features.csv already exists")
        return

    if not WALLETS_CSV.exists():
        print("[generate_cluster_features] wallets.csv not found")
        return

    rows = []
    with open(WALLETS_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            wallet = r.get("wallet", "").strip()
            if not wallet:
                continue
            rows.append({
                "wallet": wallet,
                "cluster_size": 1,
                "scam_neighbor_count": 0,
                "distance_to_scam": 999,
                "percent_to_same_cluster": 0,
                "is_scam_cluster_member": 0,
                "wallet_age_days": 0,
                "last_scam_days": 9999,
                "graph_distance": 999,
            })

    if not rows:
        print("[generate_cluster_features] no wallets found in wallets.csv")
        return

    CLUSTER_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(CLUSTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"[generate_cluster_features] Created cluster_features.csv with {len(rows)} wallets")


if __name__ == "__main__":
    generate_cluster_features()
