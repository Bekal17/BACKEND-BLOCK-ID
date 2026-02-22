"""
Mark all scam_wallets as high risk in trust_scores.

For each wallet in scam_wallets:
  - Set trust_scores.score = 0
  - Set trust_scores.risk_level = 'HIGH'
  - Append 'scam_wallet' to reason_codes (or set if empty)

Usage:
    py -m backend_blockid.tools.mark_scam_high_risk
"""
import sqlite3
import time
from pathlib import Path

DB = Path(r"D:/BACKENDBLOCKID/blockid.db")


def main() -> int:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT wallet FROM scam_wallets")
    rows = cur.fetchall()
    wallets = [r[0] for r in rows]

    if not wallets:
        print("[mark_scam_high_risk] No wallets in scam_wallets")
        conn.close()
        return 0

    now = int(time.time())
    for wallet in wallets:
        cur.execute(
            """
            INSERT INTO trust_scores (wallet, score, risk_level, reason_codes, updated_at)
            VALUES (?, 0, 'HIGH', 'scam_wallet', ?)
            ON CONFLICT(wallet) DO UPDATE SET
                score = 0,
                risk_level = 'HIGH',
                reason_codes = CASE
                    WHEN COALESCE(TRIM(reason_codes), '') = '' THEN 'scam_wallet'
                    ELSE reason_codes || ',scam_wallet'
                END,
                updated_at = ?
            """,
            (wallet, now, now),
        )

    conn.commit()
    conn.close()

    print(f"[mark_scam_high_risk] Processed {len(wallets)} scam wallet(s) → score=0, risk_level=HIGH, reason_codes+=scam_wallet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
