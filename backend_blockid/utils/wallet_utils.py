"""Wallet validation utilities."""

from solders.pubkey import Pubkey


def is_valid_wallet(w: str) -> bool:
    """Return True if w is a valid Solana wallet (Pubkey) address."""
    try:
        Pubkey.from_string(w.strip())
        return True
    except Exception:
        return False
