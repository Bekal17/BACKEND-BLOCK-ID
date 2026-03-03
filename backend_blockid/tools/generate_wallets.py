from solders.keypair import Keypair
import csv
from pathlib import Path

OUT = Path("backend_blockid/data/generated_wallets.csv")

N = 100

with open(OUT, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["wallet", "score", "risk"])

    for i in range(N):
        kp = Keypair()
        wallet = str(kp.pubkey())
        writer.writerow([wallet, 80, 1])

print("Generated", N, "wallets →", OUT)