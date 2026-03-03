from __future__ import annotations

import csv
from pathlib import Path

from backend_blockid.ai_engine.dynamic_risk_v2 import compute_dynamic_risk

SCAM_WALLETS_CSV = Path("backend_blockid/data/scam_wallets.csv")


def main() -> None:
    wallet = None
    if SCAM_WALLETS_CSV.exists():
        with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                wallet = (row.get("wallet") or "").strip()
                if wallet:
                    break

    if not wallet:
        print("No scam wallet found in CSV")
        return

    details = compute_dynamic_risk(wallet)
    print("ML score:", details.get("ml_score"))
    print("Graph penalty:", details.get("graph_penalty"))
    print("Decay:", details.get("decay"))
    print("Activity boost:", details.get("activity_boost"))
    print("Final score:", details.get("dynamic_risk"))


if __name__ == "__main__":
    main()
