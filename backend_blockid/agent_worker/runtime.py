"""
Persistent autonomous worker loop for the trust agent.

Runs as a separate process (CLI entrypoint). Fetches wallets from DB each cycle,
analyzes each (features → anomaly → trust score → alerts), updates DB. Exception
isolation per wallet; loop never crashes. Safe shutdown on KeyboardInterrupt/SIGTERM.

Usage: python -m backend_blockid.agent_worker.runtime
"""

from __future__ import annotations

import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend_blockid.agent_worker.runner import _analyze_and_save_wallet
from backend_blockid.logging import get_logger

logger = get_logger(__name__)

DEFAULT_SCAN_INTERVAL_SEC = 30.0
DEFAULT_MAX_WALLETS_PER_CYCLE = 2000
DEFAULT_CONCURRENCY = 8
DEFAULT_MAX_TX_HISTORY = 500
MIN_SCAN_INTERVAL_SEC = 1.0
MIN_CONCURRENCY = 1


@dataclass
class WorkerConfig:
    """
    Config for the persistent runtime worker.

    scan_interval_seconds: Sleep duration between cycles.
    max_wallets_per_cycle: Cap on wallets fetched and processed per cycle.
    concurrency: Number of parallel wallet analyses per cycle.
    scheduler_config: Optional scheduler config; None uses defaults (priority queue).
    """

    scan_interval_seconds: float = DEFAULT_SCAN_INTERVAL_SEC
    max_wallets_per_cycle: int = DEFAULT_MAX_WALLETS_PER_CYCLE
    concurrency: int = DEFAULT_CONCURRENCY
    db_path: str | Path = field(default_factory=lambda: Path("blockid.db"))
    max_tx_history_per_wallet: int = DEFAULT_MAX_TX_HISTORY
    anomaly_config: Any = None
    alert_config: Any = None
    scheduler_config: Any = None
    priority_scheduler_config: Any = None

    def __post_init__(self) -> None:
        self.scan_interval_seconds = max(MIN_SCAN_INTERVAL_SEC, float(self.scan_interval_seconds))
        self.max_wallets_per_cycle = max(1, int(self.max_wallets_per_cycle))
        self.concurrency = max(MIN_CONCURRENCY, min(self.concurrency, self.max_wallets_per_cycle))


def _analyze_wallet_safe(
    wallet: str,
    db: Any,
    config: WorkerConfig,
    update_last_analyzed: bool = True,
) -> bool:
    """
    Run analysis for one wallet. Swallow all exceptions and log; never raise.
    Returns True if analysis completed without error, False otherwise.
    When update_last_analyzed is True, updates tracked_wallets.last_analyzed_at on success.
    """
    try:
        _analyze_and_save_wallet(
            wallet,
            db,
            config.anomaly_config,
            config.alert_config,
            config.max_tx_history_per_wallet,
        )
        if update_last_analyzed:
            try:
                db.update_tracked_wallet_last_analyzed(wallet, int(time.time()))
            except Exception:
                pass
        return True
    except Exception as e:
        logger.warning(
            "runtime_wallet_failed",
            wallet_id=wallet[:16] if wallet else "?",
            error=str(e),
            exc_info=True,
        )
        return False


def _run_cycle(
    db: Any,
    config: WorkerConfig,
    cycle_number: int,
    scheduler_config: Any = None,
    priority_scheduler_config: Any = None,
) -> tuple[int, int]:
    """
    One cycle: select wallets via priority scheduler, analyze each with concurrency.
    Returns (processed_count, error_count). Exceptions are isolated per wallet.
    """
    from backend_blockid.agent_worker.priority_scheduler import (
        PrioritySchedulerConfig,
        select_wallets_for_cycle,
    )

    pcfg = priority_scheduler_config or PrioritySchedulerConfig(
        cycle_interval_sec=config.scan_interval_seconds,
        max_wallets_per_cycle=config.max_wallets_per_cycle,
    )
    wallets = select_wallets_for_cycle(
        db,
        cycle_number,
        now_ts=int(time.time()),
        config=pcfg,
    )
    if not wallets:
        return 0, 0

    processed = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
        futures = {
            executor.submit(_analyze_wallet_safe, w, db, config): w
            for w in wallets
        }
        for fut in as_completed(futures):
            if fut.result():
                processed += 1
            else:
                errors += 1
    return processed, errors


def run_loop(config: WorkerConfig) -> None:
    """
    Infinite worker loop: cycle (fetch → analyze → update) then sleep.
    Handles KeyboardInterrupt and SIGTERM for clean shutdown. Never crashes;
    per-cycle and per-wallet exceptions are caught and logged.
    """
    from backend_blockid.database import get_database

    db = get_database(config.db_path)
    shutdown = False

    def request_shutdown(*args: Any, **kwargs: Any) -> None:
        nonlocal shutdown
        shutdown = True

    try:
        signal.signal(signal.SIGTERM, request_shutdown)
    except (AttributeError, ValueError):
        # Windows or unsupported
        pass

    cycle = 0
    logger.info(
        "runtime_worker_started",
        scan_interval_sec=config.scan_interval_seconds,
        max_wallets_per_cycle=config.max_wallets_per_cycle,
        concurrency=config.concurrency,
        db_path=str(config.db_path),
    )

    while not shutdown:
        cycle += 1
        cycle_start = time.monotonic()
        try:
            processed, errors = _run_cycle(
                db,
                config,
                cycle,
                scheduler_config=config.scheduler_config,
                priority_scheduler_config=getattr(config, "priority_scheduler_config", None),
            )
            cycle_elapsed = time.monotonic() - cycle_start
            logger.info(
                "runtime_cycle_done",
                cycle=cycle,
                processed=processed,
                errors=errors,
                duration_sec=round(cycle_elapsed, 2),
            )
        except Exception as e:
            logger.exception("runtime_cycle_failed", cycle=cycle, error=str(e))

        # Sleep until next cycle; wake periodically to check shutdown
        deadline = cycle_start + config.scan_interval_seconds
        while not shutdown and time.monotonic() < deadline:
            sleep_for = min(1.0, max(0.0, deadline - time.monotonic()))
            if sleep_for > 0:
                time.sleep(sleep_for)

    logger.info("runtime_worker_stopped", cycle=cycle)


def _load_config_from_env() -> WorkerConfig:
    """Build WorkerConfig from environment with defaults."""
    from backend_blockid.agent_worker.priority_scheduler import PrioritySchedulerConfig
    from backend_blockid.alerts.engine import AlertConfig
    from backend_blockid.analysis_engine.anomaly import AnomalyConfig
    from backend_blockid.scheduler import SchedulerConfig

    db_path = os.getenv("DB_PATH", "blockid.db").strip() or "blockid.db"
    scan_interval = float(os.getenv("SCAN_INTERVAL_SECONDS", str(DEFAULT_SCAN_INTERVAL_SEC)))
    max_wallets = int(os.getenv("MAX_WALLETS_PER_CYCLE", str(DEFAULT_MAX_WALLETS_PER_CYCLE)))
    return WorkerConfig(
        scan_interval_seconds=scan_interval,
        max_wallets_per_cycle=max_wallets,
        concurrency=int(os.getenv("RUNTIME_CONCURRENCY", str(DEFAULT_CONCURRENCY))),
        db_path=Path(db_path),
        max_tx_history_per_wallet=int(os.getenv("MAX_TX_HISTORY_PER_WALLET", str(DEFAULT_MAX_TX_HISTORY))),
        anomaly_config=AnomalyConfig(),
        alert_config=AlertConfig(),
        scheduler_config=SchedulerConfig(),
        priority_scheduler_config=PrioritySchedulerConfig(
            cycle_interval_sec=scan_interval,
            max_wallets_per_cycle=max_wallets,
        ),
    )


def main() -> int:
    """CLI entrypoint: load config from env and run the worker loop."""
    try:
        config = _load_config_from_env()
        run_loop(config)
        return 0
    except KeyboardInterrupt:
        logger.info("runtime_shutdown_signal")
        return 0
    except Exception as e:
        logger.exception("runtime_fatal", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
