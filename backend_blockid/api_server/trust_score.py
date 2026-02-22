from __future__ import annotations

"""
FastAPI router: GET /trust-score/{wallet}, POST /trust-score/list.

Reads from local database first (fast). DB is populated by publish_one_wallet and
by the background Solana sync worker. Falls back to RPC only when explicitly needed.
"""

import base64
import functools
import json
import os
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend_blockid.database import Database, get_database
from backend_blockid.ai_engine.reason_weight_engine import aggregate_score
from backend_blockid.database.repositories import get_wallet_reasons
from backend_blockid.database.repositories import get_wallet_reasons
from backend_blockid.ml.reason_codes import REASON_WEIGHTS
from backend_blockid.blockid_logging import get_logger
from backend_blockid.oracle.solana_publisher import (
    TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN,
    TRUST_SCORE_ACCOUNT_TRUST_SCORE_OFFSET,
    TRUST_SCORE_ACCOUNT_WALLET_LEN,
    _load_keypair,
    get_trust_score_pda,
    parse_trust_score_account_data,
)

load_dotenv()

logger = get_logger(__name__)

router = APIRouter(prefix="/trust-score", tags=["trust-score"])

DEBUG_LATENCY = (os.getenv("BLOCKID_DEBUG_LATENCY") or "").strip() == "1"


def _log_latency(
    wallet: str,
    db_ms: float,
    rpc_ms: float,
    decode_ms: float,
    total_ms: float,
    num_wallets: int | None = None,
) -> None:
    """Structured latency log when BLOCKID_DEBUG_LATENCY=1."""
    if not DEBUG_LATENCY:
        return
    payload: dict[str, Any] = {
        "event": "trust_score_latency",
        "wallet": wallet,
        "db_ms": round(db_ms, 2),
        "rpc_ms": round(rpc_ms, 2),
        "decode_ms": round(decode_ms, 2),
        "total_ms": round(total_ms, 2),
    }
    if num_wallets is not None:
        payload["num_wallets"] = num_wallets
    logger.info("trust_score_latency", **payload)


def get_db() -> Database:
    """Dependency: database for trust score cache (DB_PATH env)."""
    path = Path((os.getenv("DB_PATH") or "blockid.db").strip() or "blockid.db")
    return get_database(path)

# Full Anchor layout: 8 disc + 32 wallet + 1 score + 1 risk + 8 updated_at
MIN_ACCOUNT_LEN = TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN + TRUST_SCORE_ACCOUNT_WALLET_LEN + 1 + 1 + 8  # 50
UPDATED_AT_OFFSET = TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN + TRUST_SCORE_ACCOUNT_WALLET_LEN + 1 + 1  # 42


@functools.lru_cache(maxsize=2048)
def _get_trust_score_pda_cached(program_id_str: str, oracle_pubkey_str: str, wallet_str: str) -> Any:
    """Derive trust score PDA; cached by (program_id, oracle, wallet) strings to avoid repeated find_program_address."""
    from solders.pubkey import Pubkey
    program_id = Pubkey.from_string(program_id_str)
    oracle_pubkey = Pubkey.from_string(oracle_pubkey_str)
    wallet_pubkey = Pubkey.from_string(wallet_str)
    return get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)


def _raw_bytes_from_account_data(data: object) -> bytes | None:
    """Normalize get_account_info() account.data to bytes. Handles bytes, base64, list, UiAccountData, list of ints."""
    if data is None:
        return None
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception:
            return None
    if isinstance(data, (list, tuple)):
        if not data:
            return None
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
        inner = getattr(data, "data", None)
        if isinstance(inner, bytes):
            return inner
        if isinstance(inner, str):
            try:
                return base64.b64decode(inner)
            except Exception:
                return None
        if isinstance(inner, (list, tuple)) and len(inner) > 0:
            if isinstance(inner[0], str):
                try:
                    return base64.b64decode(inner[0])
                except Exception:
                    return None
            if isinstance(inner[0], int):
                return bytes(inner)
        return _raw_bytes_from_account_data(inner)
    try:
        if hasattr(data, "__iter__") and not isinstance(data, (str, bytes)):
            arr = list(data)
            if arr and isinstance(arr[0], int):
                return bytes(arr)
    except Exception:
        pass
    return None


def _updated_at_iso(ts: int) -> str:
    """Unix seconds to ISO 8601 UTC string."""
    if ts == 0:
        return "1970-01-01T00:00:00Z"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return str(ts)


def _oracle_and_pda_for_wallet(wallet: str) -> tuple[Any, Any]:
    """Load oracle pubkey and derive PDA for a wallet. Raises on config error."""
    from solders.pubkey import Pubkey
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key or not program_id_str:
        raise HTTPException(
            status_code=503,
            detail="Server misconfiguration: ORACLE_PRIVATE_KEY and ORACLE_PROGRAM_ID required",
        )
    keypair = _load_keypair(oracle_key)
    oracle_pubkey = keypair.pubkey()
    program_id = Pubkey.from_string(program_id_str)
    wallet_pubkey = Pubkey.from_string(wallet)
    pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    return oracle_pubkey, pda


def _record_to_response(wallet: str, record: Any, oracle_pubkey: Any, pda: Any) -> dict[str, Any]:
    """Build API response from DB record + oracle/pda. Includes reason_codes from metadata when present."""
    risk = 0
    if record.metadata_json:
        try:
            meta = json.loads(record.metadata_json)
            risk = int(meta.get("risk", 0))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    reason_codes = get_wallet_reasons(wallet)
    if not reason_codes:
        from backend_blockid.ai_engine.positive_reasons import default_positive_reason
        from backend_blockid.database.repositories import insert_wallet_reason

        positive = default_positive_reason()
        try:
            insert_wallet_reason(
                wallet,
                positive["code"],
                positive["weight"],
                confidence=positive["confidence"],
                tx_hash=None,
                tx_link=None,
            )
            logger.info("positive_reason_inserted", wallet=wallet)
        except Exception:
            logger.exception("positive_reason_insert_failed", wallet=wallet)

        reason_codes = [positive]
    base_score = int(round(record.score))
    final_score = aggregate_score(base_score, reason_codes)
    out = {
        "wallet": wallet,
        "score": final_score,
        "risk": risk,
        "reason_codes": reason_codes,
        "updated_at": _updated_at_iso(record.computed_at),
        "oracle_pubkey": str(oracle_pubkey),
        "pda": str(pda),
    }
    return out


@router.get("/{wallet}")
async def get_trust_score(wallet: str, db: Database = Depends(get_db)) -> dict[str, Any]:
    """
    Return trust score for a wallet. Reads from local database first (fast).
    Returns 404 if wallet has no cached score (sync worker or publish will populate).
    """
    t_total = time.perf_counter()
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("trust_score_invalid_wallet", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=400, detail="Invalid wallet pubkey") from e

    t_db = time.perf_counter()
    timeline = db.get_trust_score_timeline(wallet, limit=1)
    db_ms = (time.perf_counter() - t_db) * 1000
    if not timeline:
        raise HTTPException(status_code=404, detail="Trust score not found for this wallet")

    latest = timeline[0]
    t_decode = time.perf_counter()
    try:
        oracle_pubkey, pda = _oracle_and_pda_for_wallet(wallet)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("trust_score_config_error", error=str(e))
        raise HTTPException(status_code=503, detail="Invalid oracle or program config") from e
    out = _record_to_response(wallet, latest, oracle_pubkey, pda)
    decode_ms = (time.perf_counter() - t_decode) * 1000
    total_ms = (time.perf_counter() - t_total) * 1000
    _log_latency(wallet=wallet[:32], db_ms=db_ms, rpc_ms=0.0, decode_ms=decode_ms, total_ms=total_ms)
    return out


class TrustScoreListRequest(BaseModel):
    """POST /trust-score/list body: list of wallet pubkeys."""

    wallets: list[str] = Field(..., min_length=1, max_length=100, description="Wallet pubkeys to fetch trust scores for")


def _not_scored(wallet: str) -> dict[str, Any]:
    """Standard response for missing or invalid trust score account."""
    return {"wallet": wallet, "status": "not_scored"}


@router.post("/list")
async def list_trust_scores(body: TrustScoreListRequest, db: Database = Depends(get_db)) -> list[dict[str, Any]]:
    """
    Batch fetch trust scores from local database. One batch query; missing wallets
    return status="not_scored". No RPC in hot path for fast response (< 50ms for 100 wallets).
    """
    t_total = time.perf_counter()
    from solders.pubkey import Pubkey

    wallet_list = [(w or "").strip() for w in body.wallets]
    t_db = time.perf_counter()
    batch = db.get_latest_trust_scores_batch(wallet_list)
    db_ms = (time.perf_counter() - t_db) * 1000

    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key or not program_id_str:
        logger.error("trust_score_list_missing_config", need="ORACLE_PRIVATE_KEY, ORACLE_PROGRAM_ID")
        raise HTTPException(
            status_code=503,
            detail="Server misconfiguration: ORACLE_PRIVATE_KEY and ORACLE_PROGRAM_ID required",
        )
    try:
        keypair = _load_keypair(oracle_key)
        oracle_pubkey = keypair.pubkey()
        oracle_pubkey_str = str(oracle_pubkey)
    except Exception as e:
        logger.error("trust_score_list_config_error", error=str(e))
        raise HTTPException(status_code=503, detail="Invalid oracle or program config") from e

    t_decode = time.perf_counter()
    results: list[dict[str, Any]] = []
    for wallet_str in wallet_list:
        if not wallet_str:
            results.append(_not_scored(""))
            continue
        try:
            Pubkey.from_string(wallet_str)
        except Exception:
            results.append(_not_scored(wallet_str))
            continue
        record = batch.get(wallet_str)
        if record is None:
            results.append(_not_scored(wallet_str))
            continue
        pda = _get_trust_score_pda_cached(program_id_str, oracle_pubkey_str, wallet_str)
        results.append(_record_to_response(wallet_str, record, oracle_pubkey, pda))
    decode_ms = (time.perf_counter() - t_decode) * 1000
    total_ms = (time.perf_counter() - t_total) * 1000
    _log_latency(
        wallet="batch",
        db_ms=db_ms,
        rpc_ms=0.0,
        decode_ms=decode_ms,
        total_ms=total_ms,
        num_wallets=len(wallet_list),
    )
    return results
