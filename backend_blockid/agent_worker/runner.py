"""
Agent runner — main event loop and process lifecycle.

Responsibilities:
- Start and supervise the Solana listener, analysis pipeline, and API server
  (or connect to a message queue for distributed workers).
- Run the main event loop (asyncio or threading) and handle graceful shutdown.
- Emit or expose health status for monitoring.
"""

import os
from pathlib import Path

from backend_blockid.agent_worker.worker import WorkerConfig, run_worker


def run_agent() -> None:
    """
    Start the 24/7 agent: Solana listener + worker loop (parse → features → anomalies → trust score → DB).
    Blocks until shutdown (SIGINT/SIGTERM). Config from env: SOLANA_RPC_URL, WALLETS, DB_PATH.
    """
    rpc_url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
    wallets_raw = os.getenv("WALLETS", "").strip()
    wallets = [w.strip() for w in wallets_raw.split(",") if w.strip()]
    if not wallets:
        raise ValueError("WALLETS env must be set (comma-separated wallet addresses)")
    db_path = os.getenv("DB_PATH", "blockid.db").strip() or "blockid.db"
    config = WorkerConfig(
        rpc_url=rpc_url,
        wallets=wallets,
        db_path=Path(db_path),
        poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "45")),
        heartbeat_interval_sec=float(os.getenv("HEARTBEAT_INTERVAL_SEC", "30")),
    )
    run_worker(config)
