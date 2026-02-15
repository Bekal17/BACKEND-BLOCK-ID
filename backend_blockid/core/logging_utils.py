"""
Structured logging — delegate to backend_blockid.logging.

Use get_logger() for JSON logs with timestamp, wallet_id, event_type, anomaly_flags.
"""

from backend_blockid.logging import get_logger

__all__ = ["get_logger"]
