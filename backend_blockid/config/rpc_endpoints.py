"""
RPC endpoint configuration for BlockID Multi-RPC Failover.

Endpoints are resolved from env. Use TEST_MODE=1 or SOLANA_NETWORK=devnet for devnet only.

Env:
  RPC_TIMEOUT: Request timeout seconds (default 5)
  RPC_MAX_RETRIES: Max retries per call (default 3)
  RPC_CACHE_SECONDS: Cache best RPC for N seconds (default 300)
  RPC_ENDPOINTS: Comma-separated override (e.g. url1,url2,url3)
"""

from __future__ import annotations

import os

from backend_blockid.config.env import (
    get_solana_network,
    load_blockid_env,
)

load_blockid_env()

# Default endpoints per network (Helius URL uses HELIUS_API_KEY)
HELIUS_MAINNET = "https://mainnet.helius-rpc.com/?api-key={key}"
HELIUS_DEVNET = "https://devnet.helius-rpc.com/?api-key={key}"
MAINNET_SOLANA = "https://api.mainnet-beta.solana.com"
DEVNET_SOLANA = "https://api.devnet.solana.com"
RPC_POOL_MAINNET = "https://solana-mainnet.rpcpool.com"
RPC_POOL_DEVNET = "https://devnet.rpcpool.com"

# Fallback lists when RPC_ENDPOINTS not set
_MAINNET_DEFAULTS = [
    HELIUS_MAINNET,
    MAINNET_SOLANA,
    RPC_POOL_MAINNET,
]
_DEVNET_DEFAULTS = [
    HELIUS_DEVNET,
    DEVNET_SOLANA,
    RPC_POOL_DEVNET,
]


def _use_test_mode() -> bool:
    raw = (os.getenv("BLOCKID_TEST_MODE") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def get_rpc_endpoints() -> list[str]:
    """
    Return RPC endpoint URLs for failover.
    TEST_MODE or devnet → devnet endpoints only.
    """
    load_blockid_env()

    override = (os.getenv("RPC_ENDPOINTS") or "").strip()
    if override:
        return [u.strip() for u in override.split(",") if u.strip()]

    network = get_solana_network()
    use_devnet = _use_test_mode() or network == "devnet"
    templates = _DEVNET_DEFAULTS if use_devnet else _MAINNET_DEFAULTS

    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    out: list[str] = []
    for t in templates:
        if "{key}" in t:
            if key:
                out.append(t.format(key=key))
        else:
            out.append(t)
    return out if out else ([DEVNET_SOLANA] if use_devnet else [MAINNET_SOLANA])
