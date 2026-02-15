"""
Domain models for database entities.

Wallet profiles, transaction history, and trust score timeline.
Used by the repository layer; no ORM coupling so backends stay swappable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class WalletProfile:
    """Stored wallet profile (first/last seen, optional snapshot)."""

    wallet: str
    first_seen_at: int
    """Unix timestamp (seconds) of first observed activity."""
    last_seen_at: int
    """Unix timestamp (seconds) of most recent activity."""
    profile_json: str | None = None
    """Optional JSON snapshot of features or metadata; null if not stored."""
    created_at: int | None = None
    updated_at: int | None = None


@dataclass
class TransactionRecord:
    """Single transaction row for history."""

    id: int | None
    wallet: str
    signature: str
    sender: str
    receiver: str
    amount_lamports: int
    timestamp: int | None
    slot: int | None
    created_at: int | None = None

    @property
    def amount_sol(self) -> float:
        return self.amount_lamports / 1_000_000_000.0


@dataclass
class TrustScoreRecord:
    """Single trust score entry in the timeline."""

    id: int | None
    wallet: str
    score: float
    computed_at: int
    """Unix timestamp (seconds) when the score was computed."""
    metadata_json: str | None = None
    """Optional JSON (e.g. anomaly flags, feature snapshot); null if not stored."""
