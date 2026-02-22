"""
Structured JSON logging: timestamp, wallet_id, event_type, anomaly_flags.

Production-ready: structlog with ISO timestamps, log level, and consistent
keys for aggregation (e.g. Datadog, CloudWatch). All agent modules should
use get_logger() and pass event_type (and wallet_id / anomaly_flags where relevant).

Uses only Python stdlib logging and structlog; no backend_blockid imports to avoid circular imports.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import structlog

# Default log level from env
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = getattr(logging, LOG_LEVEL, logging.INFO)

# JSON output for production (LOG_FORMAT=json); human-readable for local
LOG_FORMAT = os.getenv("LOG_FORMAT", "json").strip().lower()


def _add_timestamp(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Ensure timestamp is always present (ISO 8601)."""
    if "timestamp" not in event_dict:
        event_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def _normalize_event(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Rename structlog 'event' to event_type for consistency; keep message if present."""
    if "event" in event_dict and "event_type" not in event_dict:
        event_dict["event_type"] = event_dict.pop("event")
    if "message" not in event_dict and "event_type" in event_dict:
        event_dict["message"] = str(event_dict["event_type"])
    return event_dict


def configure_structlog() -> None:
    """Configure structlog once at import: JSON, timestamp, level, event_type."""
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _add_timestamp,
        _normalize_event,
    ]
    if LOG_FORMAT == "json":
        shared_processors.append(structlog.processors.JSONRenderer())
    else:
        shared_processors.append(
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
        )
    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(LOG_LEVEL_VALUE),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


# One-time configuration on first import
if not structlog.is_configured():
    configure_structlog()


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Return a structured logger for the given module name.

    Log with event_type (first arg) and optional wallet_id, anomaly_flags, etc.:
        logger = get_logger(__name__)
        logger.info("wallet_analyzed", wallet_id=addr, anomaly_flags=[...], score=85.0)
    Output (JSON): {"event_type": "wallet_analyzed", "wallet_id": "...", "anomaly_flags": [...], "score": 85.0, "timestamp": "...", "level": "info", "logger": "module.name"}
    """
    return structlog.get_logger(name).bind(logger=name)


def bind_wallet(wallet_id: str) -> structlog.BoundLogger:
    """Return a logger with wallet_id bound to all subsequent log calls."""
    return get_logger("backend_blockid").bind(wallet_id=wallet_id)
