"""
API key generation, hashing, and verification for BlockID.
"""
from __future__ import annotations

import hashlib
import secrets
import string


def _alphanumeric_chars() -> str:
    return string.ascii_letters + string.digits


def _key_prefix_for_env(environment: str) -> str:
    env = (environment or "live").strip().lower()
    if env == "test":
        return "blk_test_"
    return "blk_live_"


def generate_api_key(environment: str = "live") -> tuple[str, str, str]:
    """
    Generate a new API key.
    Key format: blk_live_ + 32 random alphanumeric chars (or blk_test_ for test).
    Returns (raw_key, key_hash, key_prefix).
    """
    prefix = _key_prefix_for_env(environment)
    alphabet = _alphanumeric_chars()
    random_part = "".join(secrets.choice(alphabet) for _ in range(32))
    raw_key = prefix + random_part
    key_hash = hash_api_key(raw_key)
    key_prefix = prefix + random_part[:8] + "..."
    return raw_key, key_hash, key_prefix


def hash_api_key(raw_key: str) -> str:
    """Return SHA-256 hash of the raw API key as hex string."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    """Return True if raw_key hashes to stored_hash."""
    if not raw_key or not stored_hash:
        return False
    return secrets.compare_digest(hash_api_key(raw_key), stored_hash)
