"""
Wallet metadata utilities — compute wallet age and scam recency for reputation decay.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Union


def _to_datetime(value: Union[datetime, int, float, None]) -> datetime | None:
    """Convert Unix timestamp or datetime to timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    try:
        ts = int(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def compute_wallet_age(first_tx_time: Union[datetime, int, float, None]) -> int:
    """
    Age of wallet in days since first transaction.

    Args:
        first_tx_time: First tx timestamp (datetime or Unix seconds).

    Returns:
        Days since first tx, or 0 if unknown/invalid.
    """
    dt = _to_datetime(first_tx_time)
    if dt is None:
        return 0
    now = datetime.now(timezone.utc)
    delta = now - dt
    return max(0, delta.days)


def compute_last_scam_days(last_scam_time: Union[datetime, int, float, None]) -> int:
    """
    Days since last scam-related transaction.

    Args:
        last_scam_time: Last scam tx timestamp (datetime or Unix seconds).

    Returns:
        Days since last scam, or 9999 if never / unknown (effectively "clean").
    """
    dt = _to_datetime(last_scam_time)
    if dt is None:
        return 9999
    now = datetime.now(timezone.utc)
    delta = now - dt
    return max(0, delta.days)
