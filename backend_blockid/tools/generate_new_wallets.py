from solders.keypair import Keypair
import csv
from pathlib import Path

OUTPUT = Path("backend_blockid/data/new_wallets.csv")

rows = []

for _ in range(20):  # jumlah wallet
    kp = Keypair()
    rows.append({
        "wallet": str(kp.pubkey()),
        "score": 80,
        "risk": 1
    })

with open(OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["wallet","score","risk"])
    writer.writeheader()
    writer.writerows(rows)

print("Saved to:", OUTPUT)