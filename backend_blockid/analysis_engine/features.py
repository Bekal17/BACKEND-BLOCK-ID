"""
Behavioral feature extraction from wallet transaction history.

Converts a list of parsed transactions (for a given wallet) into a
structured feature vector: tx frequency, avg transaction value,
unique counterparties, velocity. No scoring logic; output is suitable
for downstream scoring, ML, or API exposure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend_blockid.logging import get_logger
from backend_blockid.solana_listener.parser import ParsedTransaction

logger = get_logger(__name__)

# Seconds per day for frequency/velocity normalization
SECONDS_PER_DAY = 86400


@dataclass
class WalletFeatureVector:
    """
    Behavioral feature vector for a wallet over an observed transaction set.

    All numeric features are derived from the supplied transactions only;
    time-based metrics use the span of observed timestamps when available.
    """

    wallet: str
    """Wallet address these features describe."""
    tx_count: int
    """Number of transactions involving this wallet in the observed set."""
    tx_frequency: float | None
    """
    Transactions per day over the observed time span.
    None if fewer than 2 timestamps or no timestamps.
    """
    avg_transaction_value_lamports: float
    """Mean transaction amount (lamports) for txs involving this wallet."""
    avg_transaction_value_sol: float
    """Mean transaction amount in SOL (same as avg, human-readable)."""
    unique_counterparties: int
    """Count of distinct addresses that sent to or received from this wallet."""
    velocity_lamports_per_day: float | None
    """
    Total outgoing+incoming volume (lamports) per day over the time span.
    None if time span cannot be computed (no timestamps or single point).
    """
    velocity_sol_per_day: float | None
    """Same as velocity_lamports_per_day in SOL; None when velocity is None."""
    total_volume_lamports: int
    """Sum of transaction amounts (lamports) where wallet is sender or receiver."""
    total_volume_sol: float
    """Total volume in SOL."""
    time_span_days: float | None = None
    """Observed time span in days (max_ts - min_ts); None if not computable."""
    time_span_seconds: float | None = None
    """Observed time span in seconds; None if not computable."""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable feature vector; stable key order for downstream."""
        out: dict[str, Any] = {
            "wallet": self.wallet,
            "tx_count": self.tx_count,
            "tx_frequency": self.tx_frequency,
            "avg_transaction_value_lamports": self.avg_transaction_value_lamports,
            "avg_transaction_value_sol": self.avg_transaction_value_sol,
            "unique_counterparties": self.unique_counterparties,
            "velocity_lamports_per_day": self.velocity_lamports_per_day,
            "velocity_sol_per_day": self.velocity_sol_per_day,
            "total_volume_lamports": self.total_volume_lamports,
            "total_volume_sol": self.total_volume_sol,
            "time_span_days": self.time_span_days,
            "time_span_seconds": self.time_span_seconds,
        }
        return out


def _filter_and_orient(
    transactions: list[ParsedTransaction],
    wallet: str,
) -> tuple[list[ParsedTransaction], set[str], int, list[int]]:
    """
    Keep only txs involving wallet; collect counterparties and volume.
    Returns (filtered_txs, counterparties, total_volume_lamports, timestamps).
    """
    counterparties: set[str] = set()
    total_lamports = 0
    timestamps: list[int] = []
    filtered: list[ParsedTransaction] = []

    for tx in transactions:
        if tx.sender != wallet and tx.receiver != wallet:
            continue
        filtered.append(tx)
        counterparties.add(tx.sender)
        counterparties.add(tx.receiver)
        total_lamports += tx.amount
        if tx.timestamp is not None:
            timestamps.append(tx.timestamp)

    counterparties.discard(wallet)
    return filtered, counterparties, total_lamports, timestamps


def extract_features(
    transactions: list[ParsedTransaction],
    wallet: str,
    *,
    min_time_span_seconds: float = 1.0,
) -> WalletFeatureVector:
    """
    Convert wallet transaction history into a behavioral feature vector.

    Only transactions where the wallet is sender or receiver are included.
    Tx frequency and velocity are normalized over the observed time span
    (max timestamp - min timestamp). If there are fewer than two timestamps,
    frequency and velocity are set to None.

    Args:
        transactions: Parsed transactions (e.g. from parser.parse_batch).
        wallet: Base58 wallet address to compute features for.
        min_time_span_seconds: Minimum time span (seconds) for rate metrics
            to avoid division by zero or inflated rates; span is clamped below.

    Returns:
        WalletFeatureVector with tx_frequency, avg_transaction_value,
        unique_counterparties, velocity, and supporting fields.
    """
    filtered, counterparties, total_lamports, timestamps = _filter_and_orient(
        transactions, wallet
    )
    n = len(filtered)
    total_sol = total_lamports / 1_000_000_000.0

    if n == 0:
        return WalletFeatureVector(
            wallet=wallet,
            tx_count=0,
            tx_frequency=None,
            avg_transaction_value_lamports=0.0,
            avg_transaction_value_sol=0.0,
            unique_counterparties=0,
            velocity_lamports_per_day=None,
            velocity_sol_per_day=None,
            total_volume_lamports=0,
            total_volume_sol=0.0,
            time_span_days=None,
            time_span_seconds=None,
        )

    avg_lamports = total_lamports / n
    avg_sol = total_sol / n

    time_span_seconds: float | None = None
    time_span_days: float | None = None
    tx_frequency: float | None = None
    velocity_lamports_per_day: float | None = None
    velocity_sol_per_day: float | None = None

    if len(timestamps) >= 2:
        ts_min = min(timestamps)
        ts_max = max(timestamps)
        time_span_seconds = max(ts_max - ts_min, 0) or None
        if time_span_seconds is not None and time_span_seconds < min_time_span_seconds:
            time_span_seconds = min_time_span_seconds
        if time_span_seconds is not None and time_span_seconds > 0:
            time_span_days = time_span_seconds / SECONDS_PER_DAY
            tx_frequency = n / time_span_days
            velocity_lamports_per_day = total_lamports / time_span_days
            velocity_sol_per_day = total_sol / time_span_days

    return WalletFeatureVector(
        wallet=wallet,
        tx_count=n,
        tx_frequency=tx_frequency,
        avg_transaction_value_lamports=round(avg_lamports, 2),
        avg_transaction_value_sol=round(avg_sol, 9),
        unique_counterparties=len(counterparties),
        velocity_lamports_per_day=velocity_lamports_per_day,
        velocity_sol_per_day=velocity_sol_per_day,
        total_volume_lamports=total_lamports,
        total_volume_sol=round(total_sol, 9),
        time_span_days=time_span_days,
        time_span_seconds=time_span_seconds,
    )
