"""
Insert wallet_scores.csv into SQLite trust_scores table.

Reads backend_blockid/data/wallet_scores.csv, upserts into blockid.db.
Uses wallet as unique key: new wallets → INSERT, existing → UPDATE score.

Usage:
  py -m backend_blockid.tools.insert_wallet_scores
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
CSV_PATH = _PROJECT_ROOT / "backend_blockid" / "data" / "wallet_scores.csv"
DB_PATH = Path("D:/BACKENDBLOCKID/blockid.db")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trust_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT UNIQUE,
    score REAL,
    risk_level TEXT,
    reason_codes TEXT,
    updated_at INTEGER
)
"""


def main() -> int:
    if not CSV_PATH.exists():
        print(f"[insert_wallet_scores] ERROR: {CSV_PATH} not found")
        return 1

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        print("[insert_wallet_scores] CSV is empty")
        return 0

    # Normalize column names: accept score, final_score, or ml_score
    if "score" not in df.columns and "final_score" in df.columns:
        df = df.rename(columns={"final_score": "score"})
    elif "score" not in df.columns and "ml_score" in df.columns:
        df = df.rename(columns={"ml_score": "score"})

    if "wallet" not in df.columns or "score" not in df.columns:
        print(f"[insert_wallet_scores] ERROR: need wallet and score columns, got {list(df.columns)}")
        return 1

    if "risk_level" not in df.columns:
        df["risk_level"] = ""
    if "reason_codes" not in df.columns:
        df["reason_codes"] = ""

    df = df[["wallet", "score", "risk_level", "reason_codes"]].dropna(subset=["wallet"])
    df["wallet"] = df["wallet"].astype(str).str.strip()
    df = df[df["wallet"] != ""]

    if df.empty:
        print("[insert_wallet_scores] No valid wallet rows")
        return 0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(CREATE_TABLE_SQL)

    cur.execute("SELECT wallet FROM trust_scores")
    existing = {row[0] for row in cur.fetchall()}

    now = int(time.time())
    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        wallet = str(row["wallet"]).strip()
        score = float(row["score"]) if pd.notna(row["score"]) else 0.0
        risk_level = str(row["risk_level"]).strip() if pd.notna(row["risk_level"]) else ""
        reason_codes = str(row["reason_codes"]).strip() if pd.notna(row["reason_codes"]) else ""

        cur.execute(
            """
            INSERT INTO trust_scores (wallet, score, risk_level, reason_codes, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                score = excluded.score,
                risk_level = excluded.risk_level,
                reason_codes = excluded.reason_codes,
                updated_at = excluded.updated_at
            """,
            (wallet, score, risk_level, reason_codes, now),
        )
        if wallet in existing:
            updated += 1
        else:
            inserted += 1
        existing.add(wallet)

    conn.commit()
    conn.close()

    print(f"[insert_wallet_scores] inserted: {inserted}, updated: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
