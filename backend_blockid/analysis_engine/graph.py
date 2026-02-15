"""
Wallet relationship graph: store and update edges from transactions.

Edges represent sender -> receiver flows. Stats: tx_count, total_volume, last_seen_timestamp.
Updated every time transactions are analyzed. Used by risk propagation for neighbor lookup.
"""

from __future__ import annotations

from typing import Any

from backend_blockid.logging import get_logger

logger = get_logger(__name__)


def _tx_sender(tx: Any) -> str | None:
    sender = getattr(tx, "sender", None) or (tx.get("sender") if isinstance(tx, dict) else None)
    return (sender or "").strip() or None


def _tx_receiver(tx: Any) -> str | None:
    receiver = getattr(tx, "receiver", None) or (tx.get("receiver") if isinstance(tx, dict) else None)
    return (receiver or "").strip() or None


def _tx_amount(tx: Any) -> int:
    if hasattr(tx, "amount_lamports"):
        return int(tx.amount_lamports)
    if hasattr(tx, "amount"):
        return int(tx.amount)
    if isinstance(tx, dict):
        return int(tx.get("amount_lamports", tx.get("amount", 0)) or 0)
    return 0


def _tx_timestamp(tx: Any) -> int:
    ts = getattr(tx, "timestamp", None) or (tx.get("timestamp") if isinstance(tx, dict) else None)
    return int(ts) if ts is not None else 0


def update_wallet_graph(db: Any, transactions: list[Any]) -> int:
    """
    Update wallet_graph_edges from a list of transactions.

    For each transaction: increment edge (sender_wallet, receiver_wallet) with
    tx_count += 1, total_volume += amount_lamports, last_seen_timestamp = max(last_seen, timestamp).
    Skips rows where sender or receiver is missing. Idempotent only if each tx is passed once;
    if the same tx is passed multiple times, the edge is incremented multiple times (caller should
    pass a deduplicated list if needed).

    Args:
        db: Database instance with upsert_wallet_graph_edge(sender, receiver, amount_lamports, timestamp).
        transactions: List of transaction-like objects with sender, receiver, amount/amount_lamports, timestamp.

    Returns:
        Number of edges updated.
    """
    updated = 0
    for tx in transactions:
        sender = _tx_sender(tx)
        receiver = _tx_receiver(tx)
        if not sender or not receiver or sender == receiver:
            continue
        amount = _tx_amount(tx)
        ts = _tx_timestamp(tx)
        db.upsert_wallet_graph_edge(sender, receiver, amount, ts)
        updated += 1
    if updated:
        logger.debug(
            "wallet_graph_updated",
            tx_count=len(transactions),
            edges_updated=updated,
        )
    return updated
