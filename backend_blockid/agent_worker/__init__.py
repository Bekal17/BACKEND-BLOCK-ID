"""
Agent worker package — 24/7 background orchestration.

Orchestrates the Solana listener, analysis engine, and optional job queue.
Runs the main event loop, schedules periodic tasks, and coordinates
shutdown and health checks.
"""

from backend_blockid.agent_worker.worker import WorkerConfig, run_worker

__all__ = ["WorkerConfig", "run_worker"]
