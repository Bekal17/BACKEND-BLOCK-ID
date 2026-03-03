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

    conn.close()

    score_val = row["score"]
    if score_val is not None:
        score_val = round(float(score_val), 2)
    return {
        "wallet": row["wallet"],
        "score": score_val,
        "risk_level": row["risk_level"],
        "reason_codes": row["reason_codes"] or "",
        "updated_at": row["updated_at"],
    }
