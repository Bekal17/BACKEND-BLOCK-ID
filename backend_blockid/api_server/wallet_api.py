"""
Minimal FastAPI wallet API.

Endpoint:
  GET /wallet/{address}

Reads from database via PostgreSQL async connection.
Returns: wallet, score, risk_level, reason_codes, updated_at
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException

from backend_blockid.database.pg_connection import get_conn, release_conn

app = FastAPI()


@app.get("/wallet/{address}")
async def get_wallet(address: str) -> dict:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT wallet, score, risk_level, reason_codes, updated_at FROM trust_scores WHERE wallet = $1",
            address,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Wallet not found")

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
    finally:
        await release_conn(conn)
