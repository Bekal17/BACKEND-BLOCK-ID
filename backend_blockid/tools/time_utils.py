"""
Time utilities for block/transaction timestamps.

Used by reason decay, time-weighted risk, and anywhere we need days-since semantics.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def extract_tx_timestamp(tx: dict[str, Any] | None) -> int | None:
    """
    Extract Unix timestamp (seconds) from a Helius/Solana transaction.

    Tries tx["timestamp"] (Helius parsed), then tx["blockTime"] (raw RPC).
    Returns None if missing or invalid.
    """
    if not tx or not isinstance(tx, dict):
        return None
    raw = tx.get("timestamp") or tx.get("blockTime")
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def days_since(timestamp: datetime | int | float | str | None) -> int:
    """
    Compute whole days elapsed since a given timestamp (for time decay, freshness, etc).

    Supports:
        - datetime (naive or aware; naive assumed UTC)
        - unix seconds (int or float)
        - ISO 8601 string (e.g. "2024-01-15T12:00:00Z" or "2024-01-15T12:00:00+00:00")

    Returns 0 for None, empty string, or future timestamps.
    """
    if timestamp is None:
        return 0
    if isinstance(timestamp, str) and not timestamp.strip():
        return 0

    if isinstance(timestamp, (int, float)):
        tx_time = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    elif isinstance(timestamp, str):
        normalized = timestamp.strip().replace("Z", "+00:00")
        tx_time = datetime.fromisoformat(normalized)
        if tx_time.tzinfo is None:
            tx_time = tx_time.replace(tzinfo=timezone.utc)
    else:
        tx_time = timestamp
        if tx_time.tzinfo is None:
            tx_time = tx_time.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    return max(0, (now - tx_time).days)
