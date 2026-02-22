"""
Minimal FastAPI wallet API.

Endpoint:
  GET /wallet/{address}

Reads from SQLite at D:/BACKENDBLOCKID/blockid.db and returns:
  wallet, score, risk_level, reason_codes, updated_at
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException

DB_PATH = Path(r"D:/BACKENDBLOCKID/blockid.db")

app = FastAPI()


def _split_reason_codes(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


@app.get("/wallet/{address}")
def get_wallet(address: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT wallet, score, risk_level, reason_codes, updated_at FROM trust_scores WHERE wallet = ?",
        (address,),
    )
    row = cur.fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Wallet not found")

    cur.execute("SELECT reason_code FROM wallet_reasons WHERE wallet = ?", (address,))
    reason_rows = [r[0] for r in cur.fetchall()]

    reason_codes = _split_reason_codes(row["reason_codes"])
    for code in reason_rows:
        if code and code not in reason_codes:
            reason_codes.append(code)

    conn.close()

    return {
        "wallet": row["wallet"],
        "score": row["score"],
        "risk_level": row["risk_level"],
        "reason_codes": ",".join(reason_codes),
        "updated_at": row["updated_at"],
    }
