"""
Test that blockid_logging can be imported without circular import and logger works.
"""

from __future__ import annotations


def test_logging_import():
    """Import get_logger from blockid_logging and use the logger."""
    from backend_blockid.blockid_logging import get_logger

    logger = get_logger("test")
    assert logger is not None
    assert hasattr(logger, "info")
    assert hasattr(logger, "debug")
    assert hasattr(logger, "warning")
    assert hasattr(logger, "error")
    # Smoke test: call info (should not raise)
    logger.info("test_message", key="value")
