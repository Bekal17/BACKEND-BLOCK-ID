"""
Core 24/7 agent loop: listener → parse → features → anomalies → trust score → database.

Runs the Solana listener in a background thread and processes new transactions
in the main thread: fetch full tx, parse, store, compute features, detect anomalies,
update trust score, store results. Heartbeat logs and safe error handling so
the agent keeps running.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from backend_blockid.alerts.engine import AlertConfig, evaluate_and_store_alerts
from backend_blockid.analysis_engine.anomaly import AnomalyConfig, detect_anomalies
from backend_blockid.analysis_engine.features import extract_features
from backend_blockid.analysis_engine.scorer import compute_trust_score
from backend_blockid.database import get_database
from backend_blockid.database.models import WalletProfile
from backend_blockid.blockid_logging import get_logger
from backend_blockid.solana_listener.listener import SolanaListener
from backend_blockid.solana_listener.models import SignatureInfo
from backend_blockid.solana_listener.parser import ParsedTransaction, parse

logger = get_logger(__name__)

# Default heartbeat interval (seconds)
DEFAULT_HEARTBEAT_INTERVAL_SEC = 30.0
# RPC request timeout for getTransaction
RPC_TIMEOUT_SEC = 15.0


def fetch_transaction(rpc_url: str, signature: str) -> dict[str, Any] | None:
    """
    Fetch a single transaction by signature via getTransaction (JSON encoding).
    Returns the raw result object (with transaction, meta, blockTime, slot) or None.
    """
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "json", "maxSupportedTransactionVersion": 0},
        ],
    }
    try:
        with httpx.Client(timeout=RPC_TIMEOUT_SEC) as client:
            resp = client.post(rpc_url.rstrip("/"), json=body)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.debug("worker_fetch_tx_failed", signature=signature[:16], error=str(e))
        return None
    err = data.get("error")
    if err:
        logger.debug("worker_fetch_tx_rpc_error", error=str(err))
        return None
    result = data.get("result")
    if result is None:
        return None
    return result


@dataclass
class WorkerConfig:
    """Configuration for the 24/7 agent worker."""

    rpc_url: str
    wallets: list[str]
    db_path: str | Path = Path("blockid.db")
    poll_interval_sec: float = 45.0
    heartbeat_interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL_SEC
    anomaly_config: AnomalyConfig | None = None
    alert_config: AlertConfig | None = None
    max_tx_history_for_features: int = 500


@dataclass
class WorkerState:
    """Mutable state for heartbeat and monitoring."""

    last_wallet_processed: str | None = None
    last_processed_at: float | None = None
    last_error: str | None = None
    processed_count: int = 0
    error_count: int = 0


def process_wallet_batch(
    wallet: str,
    signatures: list[SignatureInfo],
    rpc_url: str,
    db: Any,
    anomaly_config: AnomalyConfig | None,
    alert_config: AlertConfig | None,
    max_history: int,
    state: WorkerState,
) -> None:
    """
    For one wallet and list of new signatures: fetch full txs, parse, store,
    compute features, detect anomalies, update trust score, store profile.
    Exceptions are caught by the caller; state is updated for heartbeat.
    """
    if not signatures:
        return
    rpc_url = rpc_url.rstrip("/")
    parsed_list = []
    for sig_info in signatures:
        sig = sig_info.signature
        raw = fetch_transaction(rpc_url, sig)
        if raw is None:
            continue
        parsed = parse(raw)
        if parsed is not None:
            parsed_list.append(parsed)
    if not parsed_list:
        return
    inserted = db.insert_parsed_transactions(wallet, parsed_list)
    logger.info(
        "worker_transactions_inserted",
        wallet_id=wallet,
        inserted=inserted,
        total=len(parsed_list),
    )
    history = db.get_transaction_history(wallet, limit=max_history)
    if not history:
        return
    try:
        from backend_blockid.analysis_engine.graph import update_wallet_graph
        update_wallet_graph(db, history)
    except Exception as e:
        logger.warning("worker_graph_update_failed", wallet_id=wallet[:16] if wallet else "?", error=str(e))
    txs_for_features = [
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
    features = extract_features(txs_for_features, wallet)
    anomaly_result = detect_anomalies(features, config=anomaly_config)
    base_score = compute_trust_score(features, anomaly_result)
    try:
        from backend_blockid.analysis_engine.risk_propagation import propagate_risk
        score = propagate_risk(db, wallet, base_score)
    except Exception as e:
        logger.warning("worker_risk_propagation_failed", wallet_id=wallet[:16] if wallet else "?", error=str(e))
        score = base_score
    try:
        from backend_blockid.analysis_engine.identity_cluster import apply_cluster_penalty
        score = apply_cluster_penalty(db, wallet, score)
    except Exception as e:
        logger.warning("worker_cluster_penalty_failed", wallet_id=wallet[:16] if wallet else "?", error=str(e))
    try:
        from backend_blockid.analysis_engine.entity_reputation import apply_entity_modifier
        score = apply_entity_modifier(db, wallet, score)
    except Exception as e:
        logger.warning("worker_entity_modifier_failed", wallet_id=wallet[:16] if wallet else "?", error=str(e))
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
    profile = WalletProfile(
        wallet=wallet,
        first_seen_at=ts_min,
        last_seen_at=ts_max,
        profile_json=None,
    )
    db.upsert_wallet_profile(profile)
    final_score = round(score, 2)
    stored_alerts = evaluate_and_store_alerts(
        wallet, final_score, anomaly_result, db, config=alert_config
    )
    state.last_wallet_processed = wallet
    state.last_processed_at = time.time()
    state.processed_count += 1
    anomaly_flags = [f.to_dict() for f in anomaly_result.flags]
    logger.info(
        "worker_wallet_analyzed",
        wallet_id=wallet,
        trust_score=final_score,
        anomaly_flags=anomaly_flags,
        is_anomalous=anomaly_result.is_anomalous,
        tx_count=features.tx_count,
        alerts_stored=stored_alerts,
    )


def run_worker(config: WorkerConfig) -> None:
    """
    Run the 24/7 agent: start Solana listener in a daemon thread, process
    (wallet, signatures) batches in the main thread with heartbeat and safe errors.
    Blocks until shutdown (SIGINT/SIGTERM handled by the listener thread).
    """
    db = get_database(config.db_path)
    state = WorkerState()
    work_queue: queue.Queue[tuple[str, list[SignatureInfo]]] = queue.Queue()

    def on_transaction(wallet: str, sigs: list[SignatureInfo]) -> None:
        if sigs:
            work_queue.put((wallet, sigs))

    listener = SolanaListener(
        config.rpc_url,
        config.wallets,
        poll_interval_sec=config.poll_interval_sec,
        on_transaction=on_transaction,
    )
    listener_thread = threading.Thread(target=listener.start, daemon=True)
    listener_thread.start()
    logger.info(
        "worker_started",
        wallet_count=len(config.wallets),
        poll_interval_sec=config.poll_interval_sec,
        heartbeat_interval_sec=config.heartbeat_interval_sec,
        db_path=str(config.db_path),
    )
    heartbeat_interval = max(1.0, config.heartbeat_interval_sec)
    last_heartbeat = time.monotonic()
    anomaly_cfg = config.anomaly_config

    try:
        while True:
            try:
                try:
                    wallet, sigs = work_queue.get(timeout=heartbeat_interval)
                except queue.Empty:
                    wallet, sigs = None, None
                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_interval:
                    logger.info(
                        "worker_heartbeat",
                        queue_size=work_queue.qsize(),
                        last_wallet=state.last_wallet_processed,
                        last_processed_at=state.last_processed_at,
                        processed_count=state.processed_count,
                        error_count=state.error_count,
                        last_error=state.last_error,
                    )
                    last_heartbeat = now
                if wallet is not None and sigs:
                    process_wallet_batch(
                        wallet,
                        sigs,
                        config.rpc_url,
                        db,
                        anomaly_cfg,
                        config.alert_config,
                        config.max_tx_history_for_features,
                        state,
                    )
            except KeyboardInterrupt:
                logger.info("worker_shutdown_signal")
                break
            except Exception as e:
                state.error_count += 1
                state.last_error = str(e)
                logger.exception("worker_batch_failed", error=str(e))
                continue
    finally:
        logger.info("worker_stopped")
