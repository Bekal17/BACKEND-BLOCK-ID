"""
Agent runner — main event loop and process lifecycle.

- run_agent(): listener-based worker (for main.py standalone).
- run_periodic_worker(): DB-driven periodic loop (fetch tracked wallets, analyze, update scores).
  Started by FastAPI lifespan; runs in background thread, never blocks API.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend_blockid.agent_worker.worker import WorkerConfig, run_worker
from backend_blockid.logging import get_logger

logger = get_logger(__name__)

DEFAULT_PERIODIC_INTERVAL_SEC = 30.0
DEFAULT_MAX_WALLETS_PER_TICK = 2000
SHUTDOWN_JOIN_TIMEOUT_SEC = 15.0


@dataclass
class PeriodicRunnerConfig:
    """Config for the periodic background runner (DB → analyze → trust score)."""

    db_path: str | Path = Path("blockid.db")
    interval_sec: float = DEFAULT_PERIODIC_INTERVAL_SEC
    max_wallets_per_tick: int = DEFAULT_MAX_WALLETS_PER_TICK
    max_tx_history_per_wallet: int = 500
    anomaly_config: Any = None


def _analyze_and_save_wallet(
    wallet: str,
    db: Any,
    anomaly_config: Any,
    max_history: int,
) -> None:
    """
    Load wallet history from DB, compute features, anomalies, trust score;
    write trust score and anomalies to DB. Swallow exceptions and log.
    """
    from backend_blockid.database.models import WalletProfile
    from backend_blockid.solana_listener.parser import ParsedTransaction
    from backend_blockid.analysis_engine.features import extract_features
    from backend_blockid.analysis_engine.anomaly import detect_anomalies
    from backend_blockid.analysis_engine.scorer import compute_trust_score

    history = db.get_transaction_history(wallet, limit=max_history)
    if not history:
        logger.debug("periodic_wallet_skip_no_history", wallet_id=wallet)
        return
    txs = [
        ParsedTransaction(
            sender=r.sender,
            receiver=r.receiver,
            amount=r.amount_lamports,
            timestamp=r.timestamp,
            signature=r.signature,
            slot=r.slot,
        )
        for r in history
    ]
    features = extract_features(txs, wallet)
    anomaly_result = detect_anomalies(features, config=anomaly_config)
    score = compute_trust_score(features, anomaly_result)
    now = int(time.time())
    db.insert_trust_score(
        wallet,
        score=round(score, 2),
        computed_at=now,
        metadata={
            "anomaly_flags": [f.to_dict() for f in anomaly_result.flags],
            "is_anomalous": anomaly_result.is_anomalous,
            "tx_count": features.tx_count,
        },
    )
    ts_min = min((r.timestamp for r in history if r.timestamp is not None), default=now)
    ts_max = max((r.timestamp for r in history if r.timestamp is not None), default=now)
    db.upsert_wallet_profile(
        WalletProfile(wallet=wallet, first_seen_at=ts_min, last_seen_at=ts_max, profile_json=None)
    )
    if anomaly_result.flags:
        logger.info(
            "periodic_wallet_anomalies",
            wallet_id=wallet,
            trust_score=round(score, 2),
            anomaly_flags=[f.to_dict() for f in anomaly_result.flags],
            flag_count=len(anomaly_result.flags),
        )


def run_periodic_worker(
    config: PeriodicRunnerConfig,
    stop_event: threading.Event,
) -> None:
    """
    Run the periodic analysis loop: every interval_sec, fetch tracked wallets
    from DB, analyze each, update trust score and save anomalies. Runs until
    stop_event is set. Crashes in a single tick or wallet are caught and logged;
    the loop continues. Intended to run in a background thread (e.g. from FastAPI lifespan).
    """
    from backend_blockid.database import get_database

    db = get_database(config.db_path)
    interval = max(1.0, config.interval_sec)
    logger.info(
        "periodic_runner_started",
        interval_sec=interval,
        max_wallets_per_tick=config.max_wallets_per_tick,
        db_path=str(config.db_path),
    )
    tick_count = 0
    while not stop_event.is_set():
        tick_start = time.monotonic()
        tick_count += 1
        try:
            wallets = db.get_tracked_wallets(limit=config.max_wallets_per_tick)
            processed = 0
            errors = 0
            if not wallets:
                logger.debug("periodic_tick_no_wallets", tick=tick_count)
            else:
                for wallet in wallets:
                    if stop_event.is_set():
                        break
                    try:
                        _analyze_and_save_wallet(
                            wallet,
                            db,
                            config.anomaly_config,
                            config.max_tx_history_per_wallet,
                        )
                        processed += 1
                    except Exception as e:
                        errors += 1
                        logger.warning(
                            "periodic_wallet_failed",
                            wallet_id=wallet[:8] if wallet else "?",
                            error=str(e),
                        )
                logger.info(
                    "periodic_tick_done",
                    tick=tick_count,
                    wallets=len(wallets),
                    processed=processed,
                    errors=errors,
                )
        except Exception as e:
            logger.exception("periodic_tick_failed", tick=tick_count, error=str(e))
        # Sleep until next tick; wake periodically to check stop_event
        deadline = tick_start + interval
        while not stop_event.is_set() and time.monotonic() < deadline:
            stop_event.wait(timeout=min(1.0, max(0, deadline - time.monotonic())))
    logger.info("periodic_runner_stopped", tick_count=tick_count)


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
