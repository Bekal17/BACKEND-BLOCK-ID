"""
Background worker: sync trust scores from Solana to local DB every 5 minutes.

Fetches on-chain TrustScoreAccount for tracked wallets via getMultipleAccounts,
decodes and inserts into trust_scores table. Keeps API reads fast (DB-only).
"""

from __future__ import annotations

import base64
import os
import struct
import time
from pathlib import Path
from typing import Any

from backend_blockid.database import Database, get_database
from backend_blockid.logging import get_logger
from backend_blockid.oracle.solana_publisher import (
    TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN,
    TRUST_SCORE_ACCOUNT_WALLET_LEN,
    _load_keypair,
    get_trust_score_pda,
    parse_trust_score_account_data,
)

logger = get_logger(__name__)

SYNC_INTERVAL_SEC = 300  # 5 minutes
BATCH_SIZE = 100
MIN_ACCOUNT_LEN = TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN + TRUST_SCORE_ACCOUNT_WALLET_LEN + 1 + 1 + 8  # 50
UPDATED_AT_OFFSET = TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN + TRUST_SCORE_ACCOUNT_WALLET_LEN + 1 + 1  # 42


def _raw_bytes_from_account_data(data: object) -> bytes | None:
    """Normalize RPC account.data to bytes."""
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception:
            return None
    if isinstance(data, (list, tuple)) and data:
        first = data[0]
        if isinstance(first, str):
            try:
                return base64.b64decode(first)
            except Exception:
                return None
        if isinstance(first, int):
            return bytes(data)
        return None
    if hasattr(data, "data"):
        return _raw_bytes_from_account_data(getattr(data, "data", None))
    return None


def run_sync_once(db: Database) -> int:
    """
    Sync one run: get tracked wallets, fetch on-chain via getMultipleAccounts,
    decode and insert into trust_scores. Returns number of wallets updated.
    """
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key or not program_id_str:
        logger.warning("trust_score_sync_skip", reason="missing ORACLE_PRIVATE_KEY or ORACLE_PROGRAM_ID")
        return 0

    try:
        keypair = _load_keypair(oracle_key)
        oracle_pubkey = keypair.pubkey()
        program_id = Pubkey.from_string(program_id_str)
    except Exception as e:
        logger.warning("trust_score_sync_skip", reason="config_error", error=str(e))
        return 0

    from backend_blockid.api_server.db_wallet_tracking import load_active_wallets
    wallets = load_active_wallets()[:2000]
    if not wallets:
        return 0

    client = Client(rpc_url)
    updated = 0
    for i in range(0, len(wallets), BATCH_SIZE):
        chunk = wallets[i : i + BATCH_SIZE]
        pdas: list = []
        valid: list[str] = []
        for w in chunk:
            w = (w or "").strip()
            if not w:
                continue
            try:
                wallet_pubkey = Pubkey.from_string(w)
            except Exception:
                continue
            pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
            pdas.append(pda)
            valid.append(w)

        if not pdas:
            continue
        try:
            resp = client.get_multiple_accounts(pdas, encoding="base64")
        except Exception as e:
            logger.warning("trust_score_sync_rpc_error", error=str(e))
            continue

        value_list = getattr(resp, "value", None) or (
            getattr(resp, "result", None) and getattr(resp.result, "value", None)
        )
        if not value_list:
            continue

        for j, wallet_str in enumerate(valid):
            if j >= len(value_list):
                break
            acc = value_list[j]
            if acc is None:
                continue
            data = getattr(acc, "data", None)
            raw = _raw_bytes_from_account_data(data)
            if raw is None or len(raw) < MIN_ACCOUNT_LEN:
                continue
            parsed = parse_trust_score_account_data(raw)
            if parsed is None:
                continue
            score, risk = parsed
            updated_at_ts = struct.unpack("<q", raw[UPDATED_AT_OFFSET : UPDATED_AT_OFFSET + 8])[0]
            try:
                db.insert_trust_score(
                    wallet_str,
                    float(score),
                    computed_at=updated_at_ts,
                    metadata={"risk": risk},
                )
                updated += 1
            except Exception as e:
                logger.debug("trust_score_sync_insert_skip", wallet=wallet_str[:16], error=str(e))

    return updated


def run_trust_score_sync_loop(stop_event: Any, db_path: Path, interval_sec: float = SYNC_INTERVAL_SEC) -> None:
    """Loop: every interval_sec run sync once, until stop_event is set."""
    logger.info("trust_score_sync_worker_started", interval_sec=interval_sec)
    while not stop_event.wait(timeout=interval_sec):
        try:
            db = get_database(db_path)
            n = run_sync_once(db)
            if n > 0:
                logger.info("trust_score_sync_done", updated=n)
        except Exception as e:
            logger.exception("trust_score_sync_error", error=str(e))
    logger.info("trust_score_sync_worker_stopped")
