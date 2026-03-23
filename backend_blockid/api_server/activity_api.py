"""
Wallet activity API — fetch tx activity grouped by period from DB.
GET /wallet/{wallet}/activity?range=1D|1W|30D|1Y
"""
from __future__ import annotations

import time

from fastapi import APIRouter, Query

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter(tags=["Activity"])


async def _detect_tx_columns(conn) -> tuple[str, str, str]:
    """Return (from_col, to_col, amount_col) for transactions table."""
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
    )
    cols = {r["column_name"] for r in rows}
    if "from_wallet" in cols and "to_wallet" in cols:
        from_col, to_col = "from_wallet", "to_wallet"
    else:
        from_col, to_col = "sender", "receiver"
    amount_col = "amount" if "amount" in cols else "amount_lamports"
    return from_col, to_col, amount_col


@router.get("/wallet/{wallet}/activity")
async def get_wallet_activity(
    wallet: str,
    range: str = Query(default="30D", pattern="^(1D|1W|30D|1Y)$"),
) -> dict:
    wallet = wallet.strip()
    now = int(time.time())

    if range == "1D":
        cutoff = now - 86400
        group_sql = "to_char(to_timestamp(timestamp), 'HH24:00')"
        labels = ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00", "24:00"]
    elif range == "1W":
        cutoff = now - 7 * 86400
        group_sql = "to_char(to_timestamp(timestamp), 'Dy')"
        labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    elif range == "1Y":
        cutoff = now - 365 * 86400
        group_sql = "to_char(to_timestamp(timestamp), 'Mon')"
        labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    else:  # 30D
        cutoff = now - 30 * 86400
        group_sql = """
            CASE
                WHEN EXTRACT(DAY FROM to_timestamp(timestamp)) <= 7 THEN 'W1'
                WHEN EXTRACT(DAY FROM to_timestamp(timestamp)) <= 14 THEN 'W2'
                WHEN EXTRACT(DAY FROM to_timestamp(timestamp)) <= 21 THEN 'W3'
                ELSE 'W4'
            END
        """
        labels = ["W1", "W2", "W3", "W4"]

    conn = await get_conn()
    try:
        from_col, to_col, amount_col = await _detect_tx_columns(conn)
        divisor = 1e9 if amount_col == "amount_lamports" else 1

        rows = await conn.fetch(
            f"""
            SELECT {group_sql} AS period,
                   COUNT(*) AS tx_count,
                   COALESCE(SUM(CASE WHEN {to_col}=$1 THEN {amount_col} ELSE 0 END), 0) AS inflow_raw,
                   COALESCE(SUM(CASE WHEN {from_col}=$1 THEN {amount_col} ELSE 0 END), 0) AS outflow_raw
            FROM transactions
            WHERE ({from_col}=$1 OR {to_col}=$1)
              AND timestamp IS NOT NULL
              AND timestamp >= $2
            GROUP BY period
            ORDER BY MIN(timestamp) ASC
            """,
            wallet,
            cutoff,
        )

        # Build result map
        data_map: dict[str, dict] = {}
        for row in rows:
            period = str(row["period"]).strip()
            data_map[period] = {
                "inflow": round(float(row["inflow_raw"] or 0) / divisor, 6),
                "outflow": round(float(row["outflow_raw"] or 0) / divisor, 6),
                "tx": int(row["tx_count"] or 0),
            }

        # Fill all labels (0 if no data)
        result = []
        for label in labels:
            d = data_map.get(label, {"inflow": 0, "outflow": 0, "tx": 0})
            result.append({
                "time": label,
                "inflow": d["inflow"],
                "outflow": d["outflow"],
                "tx": d["tx"],
            })

        return {"wallet": wallet, "range": range, "data": result}

    finally:
        await release_conn(conn)
