"""
Realtime wallet pipeline — same logic as run_full_pipeline, scoped to a single wallet.

Used when a wallet is not found in trust_scores. Runs the full BlockID analysis pipeline
for that wallet. Does NOT run cluster-wide operations: graph_clustering, propagation_engine,
batch_publish.

Steps (same modules as run_full_pipeline):
  1. scan_wallet (incremental_wallet_meta_scanner)
  2. flow_features (flow_features_for_wallet)
  3. drainer_detection (drainer_features_for_wallet)
  4. auto_evidence_collector (scan_wallet_transactions._scan_wallet + insert)
  5. reason_aggregator (main_async)
  6. reason_weight_engine — skip for single wallet (would wipe wallet_reasons; we apply weights inline)
  7. predict_wallet_score_for_wallet — ML scoring for requested wallet only
  8. update_wallet_score_async (dynamic_risk_v2)

Target runtime: < 3 seconds.
"""

from __future__ import annotations

import asyncio
import csv
import os
import time
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.db_wallet_tracking_light import insert_reason_evidence_async
from backend_blockid.database.repositories import insert_wallet_reason
from backend_blockid.ml.reason_codes import get_reason_weights
from backend_blockid.ai_engine.dynamic_risk_v2 import update_wallet_score_async
from backend_blockid.tools.helius_client import helius_request

logger = get_logger(__name__)

_BACKEND_DIR = Path(__file__).resolve().parent.parent
_DATA_DIR = _BACKEND_DIR / "data"

REALTIME_TX_LIMIT = 20
API_KEY = (os.getenv("HELIUS_API_KEY") or "").strip()
HELIUS_BASE = (os.getenv("HELIUS_BASE") or "https://api.helius.xyz").rstrip("/")
REQUEST_TIMEOUT = 12


def _build_url(wallet: str, before_sig: str | None = None) -> str:
    url = f"{HELIUS_BASE}/v0/addresses/{wallet}/transactions?api-key={API_KEY}&limit={REALTIME_TX_LIMIT}"
    if before_sig:
        url += f"&before-signature={before_sig}"
    return url


def _parse_tx_to_record(tx: dict[str, Any], queried_wallet: str) -> dict[str, Any] | None:
    """Extract transfer into a record for DB insert."""
    sig = (
        tx.get("signature")
        or tx.get("transactionSignature")
        or tx.get("txHash")
        or tx.get("hash")
        or ""
    )
    if not sig:
        return None
    ts = tx.get("timestamp") or tx.get("blockTime") or 0
    program_id = ""
    for ix in tx.get("instructions") or []:
        pid = ix.get("programId") or ix.get("programIdIndex") or ""
        if pid:
            program_id = str(pid)
            break

    for t in tx.get("nativeTransfers") or []:
        frm = (t.get("fromUserAccount") or "").strip()
        to = (t.get("toUserAccount") or "").strip()
        if not frm or not to:
            continue
        try:
            amt = float(t.get("amount") or 0) / 1e9
        except (TypeError, ValueError):
            amt = 0.0
        return {
            "signature": sig,
            "wallet": queried_wallet,
            "from_wallet": frm,
            "to_wallet": to,
            "amount": amt,
            "amount_lamports": int((amt or 0) * 1e9),
            "timestamp": int(ts) if ts else 0,
            "program_id": program_id or "11111111111111111111111111111111",
        }

    for t in tx.get("tokenTransfers") or []:
        frm = (t.get("fromUserAccount") or t.get("fromTokenAccount") or "").strip()
        to = (t.get("toUserAccount") or t.get("toTokenAccount") or "").strip()
        if not frm or not to:
            continue
        try:
            raw = t.get("tokenAmount") or t.get("amount") or 0
            if isinstance(raw, dict):
                amt = float(raw.get("amount", 0) or 0)
                dec = int(raw.get("decimals", 6) or 6)
                amt = amt / (10**dec)
            else:
                amt = float(raw)
        except (TypeError, ValueError):
            amt = 0.0
        return {
            "signature": sig,
            "wallet": queried_wallet,
            "from_wallet": frm,
            "to_wallet": to,
            "amount": amt,
            "amount_lamports": int((amt or 0) * 1e9),
            "timestamp": int(ts) if ts else 0,
            "program_id": program_id or "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        }
    return None


def _fetch_transactions(wallet: str) -> list[dict[str, Any]]:
    """Fetch transactions from Helius Enhanced API."""
    import requests

    url = _build_url(wallet)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        helius_request("addresses/transactions", wallet, request_count=1)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("realtime_pipeline_fetch_failed", wallet=wallet[:16], error=str(e))
        return []


def _append_wallet_to_csv(path: Path, wallet: str, row: dict[str, Any]) -> None:
    """Append or update a single wallet row in CSV. Creates file if missing."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(row.keys())
    rows: list[dict] = []
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or fieldnames
            for r in reader:
                if (r.get("wallet") or "").strip() != wallet:
                    rows.append(r)
    rows.append(row)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


async def _ensure_wallet_in_trust_scores(wallet: str) -> None:
    """Ensure wallet exists in trust_scores (for auto_evidence load_active_wallets)."""
    try:
        conn = await get_conn()
        try:
            exists = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
            if not exists:
                now = int(time.time())
                await conn.execute(
                    """
                    INSERT INTO trust_scores (wallet, score, risk_level, computed_at, updated_at)
                    VALUES ($1, 50, 'MEDIUM', $2, CURRENT_TIMESTAMP)
                    """,
                    wallet,
                    now,
                )
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.debug("ensure_wallet_trust_scores_skip", wallet=wallet[:16], error=str(e))


async def _insert_transactions(conn, wallet: str, records: list[dict[str, Any]]) -> int:
    inserted = 0
    for r in records:
        try:
            await conn.execute(
                """
                INSERT INTO transactions
                (wallet, signature, sender, receiver, amount_lamports, timestamp, slot, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (wallet, signature) DO NOTHING
                """,
                r["wallet"],
                r["signature"],
                r["from_wallet"],
                r["to_wallet"],
                r["amount_lamports"],
                r["timestamp"],
                None,
                int(time.time()),
            )
            inserted += 1
        except Exception:
            pass
    return inserted


async def _apply_reason_weights(wallet: str, evidence: list[dict]) -> None:
    """Apply reason weights and insert into wallet_reasons (same logic as reason_weight_engine)."""
    weights = get_reason_weights()
    seen_codes: set[str] = set()

    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM wallet_reasons WHERE wallet = $1", wallet)
    finally:
        await release_conn(conn)

    for row in evidence:
        code = (row.get("reason_code") or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        weight = int(weights.get(code, 0))
        tx_hash = row.get("tx_signature")
        await insert_wallet_reason(
            wallet=wallet,
            reason_code=code,
            weight=weight,
            confidence=1.0,
            tx_hash=tx_hash,
        )

    if not seen_codes:
        await insert_wallet_reason(
            wallet=wallet,
            reason_code="NO_RISK_DETECTED",
            weight=0,
            confidence=1.0,
        )


async def run_realtime_wallet_pipeline(wallet: str) -> int:
    """
    Run full BlockID pipeline for a single wallet. Same logic as run_full_pipeline.
    Returns the number of trust_scores rows inserted (1 if new, 0 if updated).
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return 0

    conn = await get_conn()
    try:
        existed_before = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
    finally:
        await release_conn(conn)

    await _ensure_wallet_in_trust_scores(wallet)

    # Step 1: scan_wallet
    logger.info("realtime_pipeline_step", step="scan_wallet", wallet=wallet[:16])
    try:
        from backend_blockid.oracle.incremental_wallet_meta_scanner import scan_wallet
        await scan_wallet(wallet)
    except Exception as e:
        logger.debug("realtime_scan_wallet_skip", wallet=wallet[:16], error=str(e))

    # Step 1.5: build_wallet_profile
    logger.info("realtime_pipeline_step", step="build_wallet_profile", wallet=wallet[:16])
    try:
        from backend_blockid.oracle.wallet_profile_builder import build_wallet_profile
        await build_wallet_profile(wallet)
    except Exception as e:
        logger.debug(
            "realtime_wallet_profile_skip",
            wallet=wallet[:16],
            error=str(e),
        )

    # Fetch transactions and insert (pipeline prerequisite)
    records: list[dict[str, Any]] = []
    if API_KEY:
        raw = _fetch_transactions(wallet)
        for tx in raw:
            r = _parse_tx_to_record(tx, wallet)
            if r:
                records.append(r)

    conn = await get_conn()
    existed = False
    try:
        if records:
            await _insert_transactions(conn, wallet, records)

        # Step 2: flow_features
        logger.info("realtime_pipeline_step", step="flow_features", wallet=wallet[:16])
        try:
            from backend_blockid.config.env import get_solana_rpc_url
            from backend_blockid.oracle.flow_features import flow_features_for_wallet
            url = get_solana_rpc_url()
            if url:
                loop = asyncio.get_event_loop()
                row = await loop.run_in_executor(
                    None, lambda: flow_features_for_wallet(url, wallet, REALTIME_TX_LIMIT),
                )
                _append_wallet_to_csv(_DATA_DIR / "flow_features.csv", wallet, row)
        except Exception as e:
            logger.debug("realtime_flow_skip", wallet=wallet[:16], error=str(e))

        # Step 3: drainer_detection
        logger.info("realtime_pipeline_step", step="drainer_detection", wallet=wallet[:16])
        try:
            from backend_blockid.config.env import get_solana_rpc_url
            from backend_blockid.oracle.drainer_detection import drainer_features_for_wallet
            url = get_solana_rpc_url()
            if url:
                loop = asyncio.get_event_loop()
                row = await loop.run_in_executor(
                    None, lambda: drainer_features_for_wallet(url, wallet, REALTIME_TX_LIMIT),
                )
                _append_wallet_to_csv(_DATA_DIR / "drainer_features.csv", wallet, row)
        except Exception as e:
            logger.debug("realtime_drainer_skip", wallet=wallet[:16], error=str(e))

        # Step 4: auto_evidence_collector
        logger.info("realtime_pipeline_step", step="auto_evidence_collector", wallet=wallet[:16])
        from backend_blockid.oracle.scan_wallet_transactions import load_scam_wallets, _scan_wallet
        scam_set = load_scam_wallets()
        evidence = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _scan_wallet(wallet, scam_set),
        )
        seen_ev: set[tuple[str, str, str | None, str | None]] = set()
        for row in evidence:
            key = (row["wallet"], row["reason_code"], row.get("tx_signature"), row.get("counterparty"))
            if key in seen_ev:
                continue
            seen_ev.add(key)
            try:
                await insert_reason_evidence_async(
                    wallet=row["wallet"],
                    reason_code=row["reason_code"],
                    tx_signature=row.get("tx_signature"),
                    counterparty=row.get("counterparty"),
                    amount=row.get("amount"),
                    token=row.get("token"),
                    timestamp=row.get("timestamp"),
                )
            except Exception:
                pass

        # Apply reason weights (reason_weight_engine logic for single wallet)
        await _apply_reason_weights(wallet, evidence)

        # Step 5: reason_aggregator
        logger.info("realtime_pipeline_step", step="reason_aggregator", wallet=wallet[:16])
        try:
            from backend_blockid.oracle.reason_aggregator import main_async as reason_aggregator_main
            await reason_aggregator_main()
        except Exception as e:
            logger.debug("realtime_reason_aggregator_skip", wallet=wallet[:16], error=str(e))

        # Step 6: reason_weight_engine — skip (we applied weights above; full module would wipe DB)

        # Ensure wallet in cluster_features for predict
        cluster_path = _DATA_DIR / "cluster_features.csv"
        default_row = {
            "wallet": wallet,
            "cluster_size": 1,
            "scam_neighbor_count": 0,
            "distance_to_scam": 999,
            "percent_to_same_cluster": 0,
            "is_scam_cluster_member": 0,
            "wallet_age_days": 0,
            "last_scam_days": 9999,
            "graph_distance": 999,
        }
        _append_wallet_to_csv(cluster_path, wallet, default_row)

        # Step 7: predict_wallet_score
        logger.info("realtime_pipeline_step", step="predict_wallet_score", wallet=wallet[:16])
        try:
            from backend_blockid.ml.predict_wallet_score import predict_wallet_score_for_wallet
            await predict_wallet_score_for_wallet(wallet)
        except Exception as e:
            logger.debug("realtime_predict_skip", wallet=wallet[:16], error=str(e))
    finally:
        await release_conn(conn)

    # Step 8: update_wallet_score_async
    logger.info("realtime_pipeline_step", step="update_wallet_score_async", wallet=wallet[:16])
    await update_wallet_score_async(wallet)

    inserted_count = 1 if not existed_before else 0
    logger.info(
        "realtime_pipeline_done",
        wallet=wallet[:16],
        trust_inserted=inserted_count,
    )
    return inserted_count


async def run_realtime_wallet_pipeline_streaming(wallet: str):
    """
    Run full BlockID pipeline for a single wallet, yielding progress as (step_id, message, **extra).
    For use with SSE / Realtime Investigator Mode.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return

    conn = await get_conn()
    try:
        existed_before = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
    finally:
        await release_conn(conn)

    await _ensure_wallet_in_trust_scores(wallet)

    # Step 1: fetch_tx
    yield ("fetch_tx", "Fetching wallet transactions", {"wallet": wallet[:16]})
    try:
        from backend_blockid.oracle.incremental_wallet_meta_scanner import scan_wallet
        await scan_wallet(wallet)
    except Exception as e:
        logger.debug("realtime_scan_wallet_skip", wallet=wallet[:16], error=str(e))

    # Step 1.5: build_wallet_profile
    logger.info("realtime_pipeline_step", step="build_wallet_profile", wallet=wallet[:16])
    try:
        from backend_blockid.oracle.wallet_profile_builder import build_wallet_profile
        await build_wallet_profile(wallet)
    except Exception as e:
        logger.debug(
            "realtime_wallet_profile_skip",
            wallet=wallet[:16],
            error=str(e),
        )

    records: list[dict[str, Any]] = []
    if API_KEY:
        raw = _fetch_transactions(wallet)
        for tx in raw:
            r = _parse_tx_to_record(tx, wallet)
            if r:
                records.append(r)

    conn = await get_conn()
    try:
        if records:
            await _insert_transactions(conn, wallet, records)

        # Step 2: build_network
        yield ("build_network", "Building wallet network", {"wallet": wallet[:16]})
        try:
            from backend_blockid.config.env import get_solana_rpc_url
            from backend_blockid.oracle.flow_features import flow_features_for_wallet
            url = get_solana_rpc_url()
            if url:
                loop = asyncio.get_event_loop()
                row = await loop.run_in_executor(
                    None, lambda: flow_features_for_wallet(url, wallet, REALTIME_TX_LIMIT),
                )
                _append_wallet_to_csv(_DATA_DIR / "flow_features.csv", wallet, row)
        except Exception as e:
            logger.debug("realtime_flow_skip", wallet=wallet[:16], error=str(e))

        # Step 3: detect_drainer
        yield ("detect_drainer", "Detecting drainer patterns", {"wallet": wallet[:16]})
        try:
            from backend_blockid.config.env import get_solana_rpc_url
            from backend_blockid.oracle.drainer_detection import drainer_features_for_wallet
            url = get_solana_rpc_url()
            if url:
                loop = asyncio.get_event_loop()
                row = await loop.run_in_executor(
                    None, lambda: drainer_features_for_wallet(url, wallet, REALTIME_TX_LIMIT),
                )
                _append_wallet_to_csv(_DATA_DIR / "drainer_features.csv", wallet, row)
        except Exception as e:
            logger.debug("realtime_drainer_skip", wallet=wallet[:16], error=str(e))

        from backend_blockid.oracle.scan_wallet_transactions import load_scam_wallets, _scan_wallet
        scam_set = load_scam_wallets()
        evidence = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _scan_wallet(wallet, scam_set),
        )
        seen_ev: set[tuple[str, str, str | None, str | None]] = set()
        for row in evidence:
            key = (row["wallet"], row["reason_code"], row.get("tx_signature"), row.get("counterparty"))
            if key in seen_ev:
                continue
            seen_ev.add(key)
            try:
                await insert_reason_evidence_async(
                    wallet=row["wallet"],
                    reason_code=row["reason_code"],
                    tx_signature=row.get("tx_signature"),
                    counterparty=row.get("counterparty"),
                    amount=row.get("amount"),
                    token=row.get("token"),
                    timestamp=row.get("timestamp"),
                )
            except Exception:
                pass
        await _apply_reason_weights(wallet, evidence)

        # Step 4: compute_score
        yield ("compute_score", "Computing trust score", {"wallet": wallet[:16]})
        try:
            from backend_blockid.oracle.reason_aggregator import main_async as reason_aggregator_main
            await reason_aggregator_main()
        except Exception as e:
            logger.debug("realtime_reason_aggregator_skip", wallet=wallet[:16], error=str(e))

        cluster_path = _DATA_DIR / "cluster_features.csv"
        default_row = {
            "wallet": wallet,
            "cluster_size": 1,
            "scam_neighbor_count": 0,
            "distance_to_scam": 999,
            "percent_to_same_cluster": 0,
            "is_scam_cluster_member": 0,
            "wallet_age_days": 0,
            "last_scam_days": 9999,
            "graph_distance": 999,
        }
        _append_wallet_to_csv(cluster_path, wallet, default_row)

        try:
            from backend_blockid.ml.predict_wallet_score import predict_wallet_score_for_wallet
            await predict_wallet_score_for_wallet(wallet)
        except Exception as e:
            logger.debug("realtime_predict_skip", wallet=wallet[:16], error=str(e))
    finally:
        await release_conn(conn)

    await update_wallet_score_async(wallet)
    yield ("done", "Analysis complete", {"wallet": wallet[:16], "trust_inserted": 1 if not existed_before else 0})
