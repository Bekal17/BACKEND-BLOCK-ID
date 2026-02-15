"""
Main entrypoint: agent worker (24/7) in background thread + FastAPI server in main thread.

The agent runs in a daemon thread so the process stays alive for the API; the API
runs in the main thread and remains responsive. On SIGINT/SIGTERM the server
shuts down and the process exits (daemon thread is stopped by the runtime).

Env: WALLETS (optional if wallets registered via POST /track-wallet), SOLANA_RPC_URL, DB_PATH, API_HOST, API_PORT, etc.

API-only (no agent): uvicorn backend_blockid.api_server.app:app --host 0.0.0.0 --port 8000
"""

import os
import sys
import threading
from pathlib import Path

# Configure structured JSON logging before other imports that may log
from backend_blockid.logging import get_logger

logger = get_logger("main")


def main() -> None:
    """Start agent worker in background thread, then run FastAPI server in main thread."""
    rpc_url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
    db_path = os.getenv("DB_PATH", "blockid.db").strip() or "blockid.db"
    api_host = os.getenv("API_HOST", "0.0.0.0").strip()
    api_port = int(os.getenv("API_PORT", "8000").strip() or "8000")

    from backend_blockid.database import get_database
    from backend_blockid.agent_worker.worker import WorkerConfig, run_worker

    db = get_database(Path(db_path))
    tracked = db.get_tracked_wallet_addresses()
    env_wallets_raw = os.getenv("WALLETS", "").strip()
    env_wallets = [w.strip() for w in env_wallets_raw.split(",") if w.strip()]
    wallets = tracked if tracked else env_wallets
    if not wallets:
        logger.error(
            "main_config_error",
            message="No wallets to monitor: register via POST /track-wallet or set WALLETS env (comma-separated)",
        )
        sys.exit(1)

    logger.info(
        "main_wallets_loaded",
        source="tracked_wallets" if tracked else "WALLETS env",
        wallet_count=len(wallets),
    )

    config = WorkerConfig(
        rpc_url=rpc_url,
        wallets=wallets,
        db_path=Path(db_path),
        poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "45")),
        heartbeat_interval_sec=float(os.getenv("HEARTBEAT_INTERVAL_SEC", "30")),
    )

    worker_thread = threading.Thread(target=run_worker, args=(config,), daemon=True)
    worker_thread.start()
    logger.info("main_worker_started", thread="daemon")

    from backend_blockid.api_server.app import app
    import uvicorn

    logger.info("main_server_starting", host=api_host, port=api_port)
    uvicorn.run(app, host=api_host, port=api_port, log_level=os.getenv("LOG_LEVEL", "info").lower())


if __name__ == "__main__":
    main()
