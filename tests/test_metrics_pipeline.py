"""
Unit test for metrics.record_pipeline_run — ensures no structlog event/event TypeError.
Simulates pipeline run and confirms no exception is raised.
"""

from __future__ import annotations


def test_record_pipeline_run_no_exception():
    """Simulate pipeline run and confirm record_pipeline_run does not raise TypeError."""
    from backend_blockid.api_server.metrics import record_pipeline_run

    # Simulate successful pipeline run (same as run_full_pipeline does)
    record_pipeline_run(success=True, wallets_scanned=10)
    record_pipeline_run(success=False, wallets_scanned=0)
    record_pipeline_run(success=True, wallets_scanned=0)
    # No exception = pass. Output would look like:
    # INFO pipeline_run_recorded success=True wallets_scanned=10
    # INFO pipeline_run_recorded success=False wallets_scanned=0
