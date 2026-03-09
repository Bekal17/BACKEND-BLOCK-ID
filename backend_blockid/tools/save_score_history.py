"""
Save trust score snapshot to wallet_score_history for chart visualization.

Run after aggregate_reason_codes.py.

Usage:
    py -m backend_blockid.tools.save_score_history
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.pg_connection import get_conn, release_conn


async def main_async() -> int:
    conn = await get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_score_history (
                wallet TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                risk INTEGER NOT NULL,
                PRIMARY KEY (wallet, timestamp)
            )
        """)
        now_ts = int(time.time())
        rows = await conn.fetch(
            """
            SELECT wallet, score, risk_level
            FROM trust_scores
            WHERE wallet IS NOT NULL
            """
        )

        def _risk_int(r) -> int:
            val = r.get("risk_level")
            if val is None:
                return 1
            try:
                return int(val)
            except (ValueError, TypeError):
                return 1

        count = 0
        for r in rows:
            wallet = (r.get("wallet") or "").strip()
            score = float(r.get("score") or 50)
            risk = _risk_int(r)
            if not wallet:
                continue
            try:
                await conn.execute(
                    """
                    INSERT INTO wallet_score_history (wallet, timestamp, score, risk)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (wallet, timestamp) DO NOTHING
                    """,
                    wallet, now_ts, score, risk,
                )
                count += 1
            except Exception:
                continue
    finally:
        await release_conn(conn)

    print(f"[history] snapshots_saved={count}")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
