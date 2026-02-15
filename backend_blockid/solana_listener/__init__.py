"""
Solana blockchain listener package.

Subscribes to Solana transactions (e.g., via WebSocket or polling),
normalizes raw transaction data, and forwards events to the analysis
engine and/or message queue for trust score computation.
"""

from backend_blockid.solana_listener.parser import (
    ParsedTransaction,
    TransactionFrequency,
    parse,
    parse_batch,
)

__all__ = [
    "ParsedTransaction",
    "TransactionFrequency",
    "parse",
    "parse_batch",
]
