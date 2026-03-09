from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pandas as pd

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ml.reason_codes import get_reason_weights

logger = get_logger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
REASONS_CSV = DATA_DIR / "wallet_reason_codes.csv"


async def main_async() -> int:
    """
    Load wallet_reason_codes.csv and insert into wallet_reasons with weights.
    """
    logger.info("reason_weight_engine_start")
    weights = get_reason_weights()
    if not REASONS_CSV.exists():
        logger.info("reason_weight_engine_skip_missing", path=str(REASONS_CSV))
        return 0

    df = pd.read_csv(REASONS_CSV)

    conn = await get_conn()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_reasons (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                reason_code TEXT,
                weight DOUBLE PRECISION,
                confidence_score DOUBLE PRECISION,
                tx_hash TEXT,
                created_at BIGINT
            )
            """
        )
        await conn.execute("DELETE FROM wallet_reasons")

        inserted = 0
        for _, row in df.iterrows():
            wallet = str(row.get("wallet", "")).strip()
            if not wallet:
                continue

            raw = row.get("reason_codes", "[]")

            try:
                codes = json.loads(raw)
            except Exception:
                codes = []

            if not isinstance(codes, list):
                continue

            for code in codes:
                code = str(code).strip()
                weight = float(weights.get(code, 0))
                now_ts = int(time.time())

                await conn.execute(
                    """
                    INSERT INTO wallet_reasons
                    (wallet, reason_code, weight, confidence_score, created_at)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    wallet, code, weight, 1.0, now_ts,
                )
                inserted += 1
    finally:
        await release_conn(conn)

    logger.info("reason_weight_engine_done", inserted=inserted, source=str(REASONS_CSV))
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
