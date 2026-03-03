import csv
from pathlib import Path

INPUT = Path("backend_blockid/data/test_wallets.csv")
OUTPUT = Path("backend_blockid/data/test_wallets_scored.csv")

LABEL_MAP = {
    "NEW_WALLET": (80, 1),
    "HIGH_OUTFLOW": (60, 2),
    "DRAINER_INTERACTION": (40, 3),
    "SCAM_CLUSTER_MEMBER": (10, 5),
}

rows = []

with open(INPUT) as f:
    reader = csv.DictReader(f)
    for r in reader:
        label = r["label"].strip()
        score, risk = LABEL_MAP.get(label, (50, 2))
        rows.append({
            "wallet": r["wallet"],
            "score": score,
            "risk": risk,
        })

with open(OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["wallet","score","risk"])
    writer.writeheader()
    writer.writerows(rows)

print("Saved:", OUTPUT)