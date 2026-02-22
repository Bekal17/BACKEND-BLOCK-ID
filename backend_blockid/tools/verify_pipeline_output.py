"""
Verify pipeline outputs and DB integrity after run_full_pipeline.
"""

from __future__ import annotations

import sqlite3
from typing import Tuple

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.config import DB_PATH

logger = get_logger(__name__)


def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def verify_pipeline_output() -> bool:
    ok = True

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. wallet_reasons table exists
    exists_wallet_reasons = _table_exists(cur, "wallet_reasons")
    print(f"[verify] wallet_reasons table exists: {'PASS' if exists_wallet_reasons else 'FAIL'}")
    ok = ok and exists_wallet_reasons

    # 2. no duplicate (wallet, reason_code, tx_hash)
    dup_count = 0
    if exists_wallet_reasons:
        cur.execute(
            """
            SELECT COUNT(*) AS c FROM (
                SELECT wallet, reason_code, tx_hash, COUNT(*) AS n
                FROM wallet_reasons
                GROUP BY wallet, reason_code, tx_hash
                HAVING n > 1
            )
            """
        )
        dup_count = int(cur.fetchone()[0] or 0)
    print(f"[verify] duplicate reasons: {'PASS' if dup_count == 0 else 'FAIL'} ({dup_count})")
    ok = ok and dup_count == 0

    # 3. trust_scores table has rows
    exists_trust_scores = _table_exists(cur, "trust_scores")
    trust_count = 0
    if exists_trust_scores:
        cur.execute("SELECT COUNT(*) FROM trust_scores")
        trust_count = int(cur.fetchone()[0] or 0)
    print(f"[verify] trust_scores rows: {'PASS' if trust_count > 0 else 'FAIL'} ({trust_count})")
    ok = ok and trust_count > 0

    # 4. scores in range 0..100
    out_of_range = 0
    if exists_trust_scores:
        cur.execute("SELECT COUNT(*) FROM trust_scores WHERE score < 0 OR score > 100")
        out_of_range = int(cur.fetchone()[0] or 0)
    print(f"[verify] score range 0..100: {'PASS' if out_of_range == 0 else 'FAIL'} ({out_of_range})")
    ok = ok and out_of_range == 0

    # 5. wallets with reasons have scores
    missing_scores = 0
    if exists_wallet_reasons and exists_trust_scores:
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT wallet FROM wallet_reasons
            ) r
            LEFT JOIN trust_scores t ON t.wallet = r.wallet
            WHERE t.wallet IS NULL
            """
        )
        missing_scores = int(cur.fetchone()[0] or 0)
    print(f"[verify] reasons have scores: {'PASS' if missing_scores == 0 else 'FAIL'} ({missing_scores})")
    ok = ok and missing_scores == 0

    # 6. tx_hash links valid format
    bad_links = 0
    if exists_wallet_reasons:
        cur.execute(
            """
            SELECT COUNT(*) FROM wallet_reasons
            WHERE tx_hash IS NOT NULL
              AND tx_hash != ''
              AND (tx_link IS NULL OR tx_link NOT LIKE 'https://solscan.io/tx/%')
            """
        )
        bad_links = int(cur.fetchone()[0] or 0)
    print(f"[verify] tx_link format: {'PASS' if bad_links == 0 else 'FAIL'} ({bad_links})")
    ok = ok and bad_links == 0

    # 7. positive reason exists for clean wallet
    positive_count = 0
    if exists_wallet_reasons:
        cur.execute(
            "SELECT COUNT(*) FROM wallet_reasons WHERE reason_code = 'NO_RISK_DETECTED'"
        )
        positive_count = int(cur.fetchone()[0] or 0)
    print(f"[verify] positive reason exists: {'PASS' if positive_count > 0 else 'FAIL'} ({positive_count})")
    ok = ok and positive_count > 0

    conn.close()
    logger.info("pipeline_verification_done")
    return ok


if __name__ == "__main__":
    verify_pipeline_output()
