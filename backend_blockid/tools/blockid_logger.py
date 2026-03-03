from __future__ import annotations

import time
from typing import Any

from backend_blockid.database.connection import get_connection


def _ensure_blockid_logs() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blockid_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            stage TEXT,
            status TEXT,
            message TEXT,
            latency_ms INTEGER,
            wallet TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def log_event(
    stage: str,
    status: str,
    message: str,
    wallet: str | None = None,
    latency_ms: int | None = None,
) -> None:
    _ensure_blockid_logs()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO blockid_logs(timestamp, stage, status, message, latency_ms, wallet)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            stage,
            status,
            message,
            int(latency_ms) if latency_ms is not None else None,
            wallet,
        ),
    )
    conn.commit()
    conn.close()

    try:
        from backend_blockid.tools.telegram_alert import send_alert

        if status == "error":
            send_alert(f"❌ BlockID Error in {stage}: {message}")
        if stage == "pda_publish" and status == "error":
            send_alert("🚨 PDA publish failed")
        if stage == "helius_fetch" and latency_ms is not None and latency_ms > 5000:
            send_alert("⚠️ Helius RPC slow")
    except Exception:
        pass
