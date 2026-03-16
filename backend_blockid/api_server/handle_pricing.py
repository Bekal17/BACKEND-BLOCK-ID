"""
BlockID Handle Registry — pricing by length.
"""
from __future__ import annotations

import re

HANDLE_PRICING_USD = {
    1: 500.0,   # @v
    2: 200.0,   # @vi
    3: 100.0,   # @vit
    4: 50.0,    # @vita
    5: 30.0,    # @vital
    6: 20.0,    # @vitali
    7: 10.0,    # @vitalik
}
DEFAULT_PRICE_USD = 5.0  # 8+ characters


def get_handle_price(handle: str) -> float:
    """Return USD price for handle based on length (without @)."""
    h = (handle or "").strip().lstrip("@")
    length = len(h)
    if length <= 0:
        return DEFAULT_PRICE_USD
    return HANDLE_PRICING_USD.get(length, DEFAULT_PRICE_USD)


def validate_handle_format(handle: str) -> tuple[bool, str]:
    """
    Validate handle format.
    Rules:
    - Strip @ prefix if present
    - Min 1 character, max 30 characters
    - Only alphanumeric + underscore
    - Cannot start with number
    - Cannot be all numbers
    Returns (is_valid, error_message)
    """
    if not handle or not isinstance(handle, str):
        return False, "Handle is required"
    h = handle.strip().lstrip("@")
    if len(h) < 1:
        return False, "Handle must be at least 1 character"
    if len(h) > 30:
        return False, "Handle must be at most 30 characters"
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", h):
        return False, "Handle may only contain letters, numbers, and underscore; cannot start with a number"
    if h.isdigit():
        return False, "Handle cannot be all numbers"
    return True, ""
