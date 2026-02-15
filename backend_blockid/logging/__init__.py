"""
Structured logging for Backend BlockID.

JSON logs with timestamp, wallet_id, event_type, anomaly_flags.
Use get_logger() in all agent modules for production-ready, aggregation-friendly output.
"""

from backend_blockid.logging.logger import get_logger

__all__ = ["get_logger"]
