import csv
import json
from pathlib import Path

DATA_DIR = Path("backend_blockid/data")
WALLETS_CSV = DATA_DIR / "wallets.csv"
REASONS_CSV = DATA_DIR / "wallet_reason_codes.csv"

DEFAULT_POSITIVE_REASONS = [
    "NO_SCAM_HISTORY",
    "LOW_RISK_CLUSTER",
    "FAR_FROM_SCAM_CLUSTER",
    "LONG_HISTORY",
]


def generate_wallet_reason_codes() -> None:
    if REASONS_CSV.exists():
        with open(REASONS_CSV, newline="", encoding="utf-8") as f:
            if sum(1 for _ in f) > 1:
                print("[generate_wallet_reason_codes] already exists")
                return

    if not WALLETS_CSV.exists():
        print("[generate_wallet_reason_codes] wallets.csv not found")
        return

    rows = []
    with open(WALLETS_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            wallet = r.get("wallet", "").strip()
            if not wallet:
                continue

            reasons = DEFAULT_POSITIVE_REASONS

            rows.append({
                "wallet": wallet,
                "reason_codes": json.dumps(reasons),
                "reason_freq": json.dumps({code: 1 for code in reasons}),
                "top_3_reasons": ",".join(reasons[:3]),
                "weighted_risk_score": 0,
            })

    REASONS_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(REASONS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "wallet",
                "reason_codes",
                "reason_freq",
                "top_3_reasons",
                "weighted_risk_score",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[generate_wallet_reason_codes] Created with {len(rows)} wallets")


if __name__ == "__main__":
    generate_wallet_reason_codes()
