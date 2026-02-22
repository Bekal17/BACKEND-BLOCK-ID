"""
Environment variable loading and validation for BlockID.

- SOLANA_NETWORK: devnet | mainnet (default: devnet)
- SOLANA_RPC_URL: RPC endpoint (read from .env)
- HELIUS_API_KEY: Helius API key (fallback for RPC URL when network=mainnet)
- ORACLE_PROGRAM_ID: Deployed trust oracle program ID
- Loads .env from project root when available.
"""

from __future__ import annotations

import os
from pathlib import Path

# Project root: config is backend_blockid/config/, root is 2 levels up
_CONFIG_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _CONFIG_DIR.parent
_ROOT = _BACKEND_DIR.parent
_ENV_PATH = _ROOT / ".env"

# Anchor.toml devnet program ID (programs.devnet.blockid_oracle)
DEFAULT_DEVNET_PROGRAM_ID = "55iMY3uHQadPv4PXwqF1uYWdyie3wqKCwJHs97eWPE6B"
# Anchor.toml mainnet program ID
DEFAULT_MAINNET_PROGRAM_ID = "9etRwVdKdVkvRsMbYVroGPzcdDxZnRmDH1D8Ho6waXGA"

DEVNET_RPC_URL = "https://api.devnet.solana.com"
MAINNET_RPC_URL = "https://api.mainnet-beta.solana.com"
HELIUS_MAINNET_URL_TEMPLATE = "https://mainnet.helius-rpc.com/?api-key={key}"
HELIUS_DEVNET_URL_TEMPLATE = "https://devnet.helius-rpc.com/?api-key={key}"


def load_blockid_env() -> None:
    """Load .env from project root. Safe to call multiple times."""
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV_PATH)
    except Exception:
        pass


def get_solana_network() -> str:
    """
    Return SOLANA_NETWORK from env: devnet | mainnet.
    Default: devnet.
    """
    load_blockid_env()
    raw = (os.getenv("SOLANA_NETWORK") or os.getenv("SOLANA_CLUSTER") or "devnet").strip().lower()
    if raw in ("devnet", "mainnet", "mainnet-beta"):
        return "devnet" if raw in ("devnet",) else "mainnet"
    # Legacy: SOLANA_DEVNET=1
    if (os.getenv("SOLANA_DEVNET") or "").strip().lower() in ("1", "true", "yes"):
        return "devnet"
    return "devnet"


def get_solana_rpc_url() -> str:
    """
    Resolve Solana RPC URL from env.
    Order: SOLANA_RPC_URL > HELIUS_API_KEY (network-specific) > devnet/mainnet default.
    """
    load_blockid_env()
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        network = get_solana_network()
        if network == "devnet":
            return HELIUS_DEVNET_URL_TEMPLATE.format(key=key)
        return HELIUS_MAINNET_URL_TEMPLATE.format(key=key)
    network = get_solana_network()
    return DEVNET_RPC_URL if network == "devnet" else MAINNET_RPC_URL


def get_oracle_program_id() -> str:
    """
    Return ORACLE_PROGRAM_ID from env, or default for current network.
    PDA derivation must use this program ID.
    """
    load_blockid_env()
    pid = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if pid:
        return pid
    network = get_solana_network()
    return DEFAULT_DEVNET_PROGRAM_ID if network == "devnet" else DEFAULT_MAINNET_PROGRAM_ID


def is_devnet() -> bool:
    """Return True if SOLANA_NETWORK is devnet."""
    return get_solana_network() == "devnet"


def use_devnet_dummy_data() -> bool:
    """
    Return True when devnet dummy dataset should be used (no RPC).
    Set BLOCKID_USE_DUMMY_DATA=1 when Helius/RPC unavailable.
    """
    load_blockid_env()
    raw = (os.getenv("BLOCKID_USE_DUMMY_DATA") or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    return False


def get_devnet_dummy_dir() -> Path:
    """Return path to devnet dummy dataset directory."""
    return _BACKEND_DIR / "data" / "devnet_dummy"


def print_blockid_startup(script_name: str) -> None:
    """Print network and program ID at script start."""
    load_blockid_env()
    network = get_solana_network()
    program_id = get_oracle_program_id()
    rpc = get_solana_rpc_url()
    # Mask API key in URL if present
    if "api-key=" in rpc:
        rpc = rpc.split("api-key=")[0] + "api-key=***"
    print(f"[blockid] {script_name} | network={network} | program_id={program_id} | rpc={rpc[:50]}..." if len(rpc) > 50 else f"[blockid] {script_name} | network={network} | program_id={program_id} | rpc={rpc}")
