"""
Configuration management for the Backend BlockID agent.

Loads and validates settings from environment variables and optional
config files. Exposes a single source of truth for all service configuration.
"""

from backend_blockid.config.settings import get_settings  # noqa: F401

__all__ = ["get_settings"]
