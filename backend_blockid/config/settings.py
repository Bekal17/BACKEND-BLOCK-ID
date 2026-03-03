"""
Application settings and environment configuration for BlockID.

Loads from .env via python-dotenv. Provides BlockIDSettings for typed access.
Safety checks on startup for production deployment.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _CONFIG_DIR.parent
_ROOT = _BACKEND_DIR.parent
_ENV_PATH = _ROOT / ".env"
_PROD_ENV_EXAMPLE = _CONFIG_DIR / "production.env.example"


def _load_dotenv() -> None:
    """Load .env from project root."""
    try:
        from dotenv import load_dotenv
        load_dotenv(_ENV_PATH)
        if _PROD_ENV_EXAMPLE.exists():
            load_dotenv(_PROD_ENV_EXAMPLE, override=False)
    except ImportError:
        pass


def _env(key: str, default: str = "") -> str:
    """Get env var, strip whitespace."""
    _load_dotenv()
    return (os.getenv(key) or default).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env(key, "0").lower()
    return raw in ("1", "true", "yes", "on")


def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


def _env_float(key: str, default: float = 0.0) -> float:
    try:
        return float(_env(key, str(default)))
    except ValueError:
        return default


def _resolve_rpc_url() -> str:
    """Resolve RPC URL from env (delegates to env.py when needed)."""
    try:
        from backend_blockid.config.env import get_solana_rpc_url
        return get_solana_rpc_url()
    except Exception:
        return ""


@dataclass
class BlockIDSettings:
    """BlockID configuration loaded from environment."""

    # Solana (rpc_url resolved via env.py when empty)
    rpc_url: str = field(default_factory=lambda: _env("SOLANA_RPC_URL") or _env("HELIUS_RPC_URL") or _resolve_rpc_url())
    solana_network: str = field(default_factory=lambda: _env("SOLANA_NETWORK") or _env("SOLANA_CLUSTER") or "devnet")
    oracle_program_id: str = field(default_factory=lambda: _env("ORACLE_PROGRAM_ID"))
    oracle_private_key: str = field(default_factory=lambda: _env("ORACLE_PRIVATE_KEY"))

    # Helius
    helius_key: str = field(default_factory=lambda: _env("HELIUS_API_KEY"))
    helius_max_wallets: int = field(default_factory=lambda: _env_int("HELIUS_MAX_WALLETS_PER_RUN") or _env_int("BLOCKID_MAX_WALLETS", 200))
    helius_incremental_fetch: bool = field(default_factory=lambda: _env_bool("HELIUS_INCREMENTAL_FETCH", True))

    # Database
    db_url: str = field(default_factory=lambda: _env("DB_URL", "sqlite:///blockid.db"))
    db_path: str = field(default_factory=lambda: _env("DB_PATH", "blockid.db"))

    # Pipeline
    test_mode: bool = field(default_factory=lambda: _env_bool("BLOCKID_TEST_MODE", True))
    pipeline_mode: bool = field(default_factory=lambda: _env_bool("BLOCKID_PIPELINE_MODE", True))
    realtime_mode: bool = field(default_factory=lambda: _env_bool("BLOCKID_REALTIME_MODE", False))
    dry_run: bool = field(default_factory=lambda: _env_bool("BLOCKID_DRY_RUN", True))
    skip_publish: bool = field(default_factory=lambda: _env_bool("BLOCKID_SKIP_PUBLISH", True))

    # Trust logic
    confidence_threshold: float = field(default_factory=lambda: _env_float("CONFIDENCE_THRESHOLD", 0.72))
    review_queue_enabled: bool = field(default_factory=lambda: _env_bool("REVIEW_QUEUE_ENABLED", True))
    publish_enabled: bool = field(default_factory=lambda: _env_bool("AUTO_PUBLISH_ENABLED", False))

    # Score decay
    decay_enabled: bool = field(default_factory=lambda: _env_bool("DECAY_ENABLED", True))
    decay_max_score: int = field(default_factory=lambda: _env_int("DECAY_MAX_SCORE", 80))

    # Logging
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    log_file: str = field(default_factory=lambda: _env("LOG_FILE", "logs/blockid.log"))

    # Alerts
    alert_telegram_token: str = field(default_factory=lambda: _env("ALERT_TELEGRAM_TOKEN"))
    alert_email: str = field(default_factory=lambda: _env("ALERT_EMAIL"))

    # Monitoring
    enable_healthcheck: bool = field(default_factory=lambda: _env_bool("ENABLE_HEALTHCHECK", True))
    enable_metrics: bool = field(default_factory=lambda: _env_bool("ENABLE_METRICS", False))

    @property
    def is_mainnet(self) -> bool:
        n = self.solana_network.lower()
        return n in ("mainnet", "mainnet-beta")

    @property
    def is_devnet(self) -> bool:
        return self.solana_network.lower() == "devnet"


_settings: BlockIDSettings | None = None


def get_settings() -> BlockIDSettings:
    """Return singleton BlockIDSettings instance."""
    global _settings
    if _settings is None:
        _settings = BlockIDSettings()
    return _settings


def validate_production_config(strict: bool = True) -> list[str]:
    """
    Validate configuration for production deployment.
    Returns list of error messages. Empty if valid.
    If strict=True and invalid, calls sys.exit(1).
    """
    s = get_settings()
    errors: list[str] = []

    if s.is_mainnet:
        if s.test_mode:
            errors.append("BLOCKID_TEST_MODE must be 0 for mainnet production")
        if "devnet" in (s.rpc_url or "").lower():
            errors.append("RPC_URL must not be devnet when SOLANA_CLUSTER=mainnet")
        if not s.oracle_program_id or len(s.oracle_program_id) < 32:
            errors.append("ORACLE_PROGRAM_ID must be set and valid for mainnet")
        try:
            from solders.pubkey import Pubkey
            Pubkey.from_string(s.oracle_program_id)
        except Exception:
            errors.append("ORACLE_PROGRAM_ID is not a valid Solana public key")

    if strict and errors:
        print("[blockid] PRODUCTION CONFIG VALIDATION FAILED:")
        for e in errors:
            print(f"  - {e}")
        print("[blockid] Fix .env and retry. See backend_blockid/config/production.env.example")
        sys.exit(1)

    return errors


def ensure_production_safe() -> None:
    """
    On startup: verify production config. If mainnet + invalid config → stop server.
    Call from API server lifespan or pipeline entry points.
    """
    s = get_settings()
    if s.is_mainnet:
        validate_production_config(strict=True)
