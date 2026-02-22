"""
BlockID Reason Aggregator — minimal safe version.

Collect wallet reasons and write reason_codes.csv.
"""

from pathlib import Path
import csv
import sqlite3

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.config import DB_PATH
from backend_blockid.ml.reason_codes import REASON_WEIGHTS

logger = get_logger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT = DATA_DIR / "reason_codes.csv"


def main():
    logger.info("reason_aggregator_start")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    reasons_by_wallet: dict[str, dict[str, int]] = {}
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT wallet, reason_code, weight
        FROM wallet_reasons
        WHERE wallet IS NOT NULL AND reason_code IS NOT NULL
        """
    )
    for wallet, reason_code, weight in cur.fetchall():
        if wallet not in reasons_by_wallet:
            reasons_by_wallet[wallet] = {}
        if reason_code not in reasons_by_wallet[wallet]:
            reasons_by_wallet[wallet][reason_code] = int(weight) if weight is not None else 0

    cur.execute(
        """
        SELECT wallet
        FROM trust_scores
        WHERE wallet IS NOT NULL
        """
    )
    for (wallet,) in cur.fetchall():
        if wallet not in reasons_by_wallet:
            reasons_by_wallet[wallet] = {}
    conn.close()

    for wallet, reasons in reasons_by_wallet.items():
        has_negative = any(
            (reasons.get(code) if code in reasons else REASON_WEIGHTS.get(code, 0)) < 0
            for code in reasons
        )
        if not has_negative:
            reasons.setdefault("CLEAN_HISTORY", 10)
        if not reasons:
            reasons["NO_RISK_DETECTED"] = 0
        if has_negative:
            reasons.pop("NO_RISK_DETECTED", None)

    # minimal dummy output
    with open(OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "reason_code", "weight"])
        for wallet, reasons in reasons_by_wallet.items():
            for reason_code in sorted(reasons):
                writer.writerow([wallet, reason_code, reasons.get(reason_code, 0)])

    logger.info("reason_aggregator_done", output=str(OUTPUT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
