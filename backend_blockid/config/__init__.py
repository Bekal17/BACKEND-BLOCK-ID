"""
Configuration management for the Backend BlockID agent.

Loads and validates settings from environment variables and optional
config files. Exposes a single source of truth for all service configuration.
"""

from backend_blockid.config.settings import (
    BlockIDSettings,
    ensure_production_safe,
    get_settings,
    validate_production_config,
)

__all__ = [
    "BlockIDSettings",
    "ensure_production_safe",
    "get_settings",
    "validate_production_config",
]
