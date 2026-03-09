"""
BlockID Reason Aggregator — minimal safe version.

Collect wallet reasons and write reason_codes.csv.
"""

import asyncio
from pathlib import Path
import csv

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ml.reason_codes import get_reason_weights
from backend_blockid.tools.time_utils import days_since

logger = get_logger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT = DATA_DIR / "reason_codes.csv"


async def main_async():
    logger.info("reason_aggregator_start")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    reasons_by_wallet: dict[str, list[dict]] = {}

    conn = await get_conn()
    try:
        try:
            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                        WHERE table_name='wallet_reasons' AND column_name='created_at') THEN
                        ALTER TABLE wallet_reasons ADD COLUMN created_at INTEGER;
                    END IF;
                END $$;
            """)
        except Exception:
            pass

        rows = await conn.fetch(
            """
            SELECT wallet, reason_code, confidence_score, tx_hash, created_at
            FROM wallet_reasons
            WHERE wallet IS NOT NULL AND reason_code IS NOT NULL
            """
        )
        for row in rows:
            wallet = row["wallet"]
            reason_code = row["reason_code"]
            confidence_score = row["confidence_score"]
            tx_hash = row["tx_hash"]
            created_at = row["created_at"]
            days_old_val = days_since(created_at) if created_at is not None else 0

            if wallet not in reasons_by_wallet:
                reasons_by_wallet[wallet] = []
            reasons_by_wallet[wallet].append(
                {
                    "code": reason_code,
                    "confidence": confidence_score,
                    "tx_hash": tx_hash,
                    "days_old": days_old_val,
                }
            )

        trust_rows = await conn.fetch(
            """
            SELECT wallet
            FROM trust_scores
            WHERE wallet IS NOT NULL
            """
        )
        for row in trust_rows:
            wallet = row["wallet"]
            if wallet not in reasons_by_wallet:
                reasons_by_wallet[wallet] = []
    finally:
        await release_conn(conn)

    for wallet, reasons in reasons_by_wallet.items():
        has_negative = any(get_reason_weights().get(r.get("code"), 0) < 0 for r in reasons)
        if not has_negative:
            reasons.append({"code": "CLEAN_HISTORY", "confidence": 1.0, "tx_hash": None, "days_old": 0})
        if not reasons:
            reasons.append({"code": "NO_RISK_DETECTED", "confidence": 1.0, "tx_hash": None, "days_old": 0})
        if has_negative:
            reasons = [r for r in reasons if r.get("code") != "NO_RISK_DETECTED"]
            reasons_by_wallet[wallet] = reasons

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "reason_code", "confidence", "tx_hash", "days_old"])
        for wallet, reasons in reasons_by_wallet.items():
            for reason in reasons:
                writer.writerow(
                    [
                        wallet,
                        reason.get("code"),
                        reason.get("confidence"),
                        reason.get("tx_hash"),
                        reason.get("days_old", 0),
                    ]
                )

    logger.info("reason_aggregator_done", output=str(OUTPUT))
    return 0


def main():
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
