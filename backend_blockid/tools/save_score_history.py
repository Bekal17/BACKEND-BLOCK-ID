"""
Save trust score snapshot to wallet_score_history for chart visualization.

Run after aggregate_reason_codes.py.

Usage:
    py -m backend_blockid.tools.save_score_history
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection


def main() -> int:
    conn = get_connection()
    cur = conn.cursor()

    now_ts = int(time.time())
    cur.execute(
        """
        SELECT wallet, score, risk_level
        FROM trust_scores
        WHERE wallet IS NOT NULL
        """
    )
    rows = cur.fetchall()

    def _risk_int(r) -> int:
        val = r["risk_level"] if hasattr(r, "keys") else r[2]
        if val is None:
            return 1
        try:
            return int(val)
        except (ValueError, TypeError):
            return 1

    count = 0
    for r in rows:
        wallet = (r["wallet"] if hasattr(r, "keys") else r[0]).strip()
        score = float((r["score"] if hasattr(r, "keys") else r[1]) or 50)
        risk = _risk_int(r)
        if not wallet:
            continue
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO wallet_score_history (wallet, timestamp, score, risk)
                VALUES (?, ?, ?, ?)
                """,
                (wallet, now_ts, score, risk),
            )
            if cur.rowcount > 0:
                count += 1
        except Exception:
            continue

    conn.commit()
    conn.close()

    print(f"[history] snapshots_saved={count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
