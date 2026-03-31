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
  6. reason_weight_engine — skip for single wallet (applied inline)
  7. predict_wallet_score_for_wallet — ML scoring
  8. daemon_enrichment — removed (DAEMON_API_KEY 404); replaced by Cyclops
  9. update_wallet_score_async (dynamic_risk_v2)

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
from backend_blockid.database.score_history import log_score_change
from backend_blockid.ml.reason_codes import get_reason_weights
from backend_blockid.ai_engine.dynamic_risk_v2 import update_wallet_score_async
from backend_blockid.api_server.identity_eligibility import check_eligibility
from backend_blockid.integrations.cyclops_client import analyze_wallet as cyclops_analyze
from backend_blockid.integrations.daemon_ai_client import explain_wallet_risk
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
        result = data if isinstance(data, list) else []
        logger.debug("fetch_transactions_done", wallet=wallet[:16], count=len(result))
        if result:
            first = result[0]
            logger.debug(
                "fetch_transactions_sample",
                wallet=wallet[:16],
                has_nativeTransfers=bool(first.get("nativeTransfers")),
                has_tokenTransfers=bool(first.get("tokenTransfers")),
                keys=list(first.keys())[:8],
            )
        return result
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
            exists = await conn.fetchval(
                "SELECT 1 FROM trust_scores WHERE wallet = $1", wallet
            )
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
                (wallet, signature, sender, receiver, amount_lamports,
                timestamp, slot, created_at, program_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
                r.get("program_id"),
            )
            inserted += 1
        except Exception as e:
            logger.warning(
                "insert_transaction_failed",
                wallet=wallet[:16],
                error=str(e),
                signature=r.get("signature", "")[:16],
            )
    logger.debug(
        "insert_transactions_done",
        wallet=wallet[:16],
        inserted=inserted,
        total=len(records),
    )
    return inserted


async def _apply_reason_weights(wallet: str, evidence: list[dict]) -> None:
    """Apply reason weights and insert into wallet_reasons."""
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


# ---------------------------------------------------------------------------
# Daemon enrichment removed (DAEMON_API_KEY 404)
# Replaced by Cyclops: backend_blockid.integrations.cyclops_client
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_realtime_wallet_pipeline(wallet: str) -> int:
    """
    Run full BlockID pipeline for a single wallet.
    Returns 1 if wallet is new, 0 if updated.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return 0

    conn = await get_conn()
    try:
        existed_before = await conn.fetchval(
            "SELECT 1 FROM trust_scores WHERE wallet = $1", wallet
        )
    finally:
        await release_conn(conn)

    await _ensure_wallet_in_trust_scores(wallet)

    # Step 1: scan_wallet
    logger.debug("realtime_pipeline_step", step="scan_wallet", wallet=wallet[:16])
    try:
        from backend_blockid.oracle.incremental_wallet_meta_scanner import scan_wallet
        await scan_wallet(wallet)
    except Exception as e:
        logger.debug("realtime_scan_wallet_skip", wallet=wallet[:16], error=str(e))

    # Step 1.5: build_wallet_profile
    logger.debug("realtime_pipeline_step", step="build_wallet_profile", wallet=wallet[:16])
    try:
        from backend_blockid.oracle.wallet_profile_builder import build_wallet_profile
        await build_wallet_profile(wallet)
    except Exception as e:
        logger.debug("realtime_wallet_profile_skip", wallet=wallet[:16], error=str(e))

    # Fetch + insert transactions
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

        # Step 2: flow_features
        logger.debug("realtime_pipeline_step", step="flow_features", wallet=wallet[:16])
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
        logger.debug("realtime_pipeline_step", step="drainer_detection", wallet=wallet[:16])
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
        logger.debug("realtime_pipeline_step", step="auto_evidence_collector", wallet=wallet[:16])
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

        # Step 4b: _apply_reason_weights — DELETE + insert negative reasons (must run before Step 5b)
        await _apply_reason_weights(wallet, evidence)

        # Step 5: reason_aggregator
        logger.debug("realtime_pipeline_step", step="reason_aggregator", wallet=wallet[:16])
        try:
            from backend_blockid.oracle.reason_aggregator import main_async as reason_aggregator_main
            await reason_aggregator_main()
        except Exception as e:
            logger.debug("realtime_reason_aggregator_skip", wallet=wallet[:16], error=str(e))

        # Step 5b: detect_positive_reasons — insert positive reasons AFTER 4b so DELETE does not wipe them
        logger.debug("realtime_pipeline_step", step="detect_positive_reasons", wallet=wallet[:16])
        try:
            from backend_blockid.ai_engine.positive_reasons import detect_positive_reasons
            positive = await detect_positive_reasons(wallet)
            for r in positive:
                await insert_wallet_reason(
                    wallet=wallet,
                    reason_code=r["code"],
                    weight=int(r.get("weight", 0)),
                    confidence=float(r.get("confidence", 1.0)),
                    tx_hash=r.get("tx_hash"),
                )
        except Exception as e:
            logger.warning(
                "realtime_positive_reasons_skip",
                wallet=wallet[:16],
                error=str(e),
            )

        # Step 6: reason_weight_engine — skip (applied inline above)

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
        logger.debug("realtime_pipeline_step", step="predict_wallet_score", wallet=wallet[:16])
        try:
            from backend_blockid.ml.predict_wallet_score import predict_wallet_score_for_wallet
            await predict_wallet_score_for_wallet(wallet)
        except Exception as e:
            logger.debug("realtime_predict_skip", wallet=wallet[:16], error=str(e))

    finally:
        await release_conn(conn)

    # Step 8: Cyclops risk enrichment
    try:
        cyclops_data = await cyclops_analyze(wallet, max_depth=2)
        if cyclops_data:
            logger.info(
                "cyclops_enrichment_done",
                wallet=wallet[:16],
                risk_score=cyclops_data.get("risk_score"),
                risk_level=cyclops_data.get("risk_level"),
                is_sanctioned=cyclops_data.get("is_sanctioned"),
                nodes=cyclops_data.get("nodes_count"),
            )
            # Apply sanctions penalty
            if cyclops_data.get("is_sanctioned"):
                logger.warning("cyclops_sanctioned_wallet", wallet=wallet[:16])
                # Will be used in score adjustment below
            # Apply high risk penalty if Cyclops risk is HIGH or CRITICAL
            cyclops_risk_level = cyclops_data.get("risk_level", "")
            if cyclops_risk_level in ("HIGH", "CRITICAL"):
                logger.warning(
                    "cyclops_high_risk",
                    wallet=wallet[:16],
                    risk_level=cyclops_risk_level,
                    risk_score=cyclops_data.get("risk_score"),
                )
        else:
            cyclops_data = None
            logger.info("cyclops_enrichment_skipped", wallet=wallet[:16], reason="no result")
    except Exception as _e:
        cyclops_data = None
        logger.warning("cyclops_enrichment_error", wallet=wallet[:16], error=str(_e))

    # Persist Cyclops data to wallet_meta (best-effort)
    try:
        meta_conn = await get_conn()
        try:
            await meta_conn.execute(
                """
                INSERT INTO wallet_meta (wallet, cyclops_risk_score, cyclops_risk_level,
                    cyclops_is_sanctioned, cyclops_updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (wallet) DO UPDATE SET
                    cyclops_risk_score = EXCLUDED.cyclops_risk_score,
                    cyclops_risk_level = EXCLUDED.cyclops_risk_level,
                    cyclops_is_sanctioned = EXCLUDED.cyclops_is_sanctioned,
                    cyclops_updated_at = NOW()
                """,
                wallet,
                cyclops_data.get("risk_score") if cyclops_data else None,
                cyclops_data.get("risk_level") if cyclops_data else None,
                cyclops_data.get("is_sanctioned", False) if cyclops_data else False,
            )
        finally:
            await release_conn(meta_conn)
    except Exception:
        pass  # Column might not exist yet, non-fatal

    # Step 8b: Daemon-AI explanation
    try:
        if cyclops_data or True:  # always try to explain
            # Get current reasons from DB
            reason_conn = await get_conn()
            try:
                reason_rows = await reason_conn.fetch(
                    "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 LIMIT 10",
                    wallet,
                )
                reasons = [r["reason_code"] for r in reason_rows]
                score_row = await reason_conn.fetchrow(
                    "SELECT score, risk_level FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
                    wallet,
                )
                current_score = float(score_row["score"]) if score_row else 50.0
                current_tier = str(score_row["risk_level"]) if score_row else "MEDIUM"
            finally:
                await release_conn(reason_conn)

            ai_explanation = await explain_wallet_risk(
                wallet=wallet,
                trust_score=current_score,
                risk_tier=current_tier,
                reasons=reasons,
                cyclops_risk_level=cyclops_data.get("risk_level") if cyclops_data else None,
                cyclops_risk_score=cyclops_data.get("risk_score") if cyclops_data else None,
                is_sanctioned=cyclops_data.get("is_sanctioned", False) if cyclops_data else False,
            )

            if ai_explanation:
                # Store in wallet_meta
                explain_conn = await get_conn()
                try:
                    await explain_conn.execute(
                        """
                        INSERT INTO wallet_meta (wallet, ai_explanation, ai_explanation_updated_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (wallet) DO UPDATE SET
                            ai_explanation = EXCLUDED.ai_explanation,
                            ai_explanation_updated_at = NOW()
                        """,
                        wallet,
                        ai_explanation,
                    )
                    logger.info("daemon_ai_explanation_stored", wallet=wallet[:16])
                except Exception:
                    pass
                finally:
                    await release_conn(explain_conn)
    except Exception as _e:
        logger.warning("daemon_ai_pipeline_error", wallet=wallet[:16], error=str(_e))

    # Step 9: [NEW] Behavioral linking scan (suggestions only; user must confirm)
    logger.debug("realtime_pipeline_step", step="behavioral_linking", wallet=wallet[:16])
    try:
        from backend_blockid.ml.behavioral_linking import run_linking_scan, save_suggestions
        link_conn = await get_conn()
        try:
            suggestions = await run_linking_scan(wallet, link_conn)
            if suggestions:
                handle_row = await link_conn.fetchrow(
                    "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
                    wallet,
                )
                handle = handle_row["handle"] if handle_row else None
                saved = await save_suggestions(wallet, suggestions, handle, link_conn)
                logger.debug(
                    "behavioral_linking_done",
                    wallet=wallet[:16],
                    suggestions_found=len(suggestions),
                    suggestions_saved=saved,
                )
        finally:
            await release_conn(link_conn)
    except Exception as e:
        logger.debug("behavioral_linking_skip", wallet=wallet[:16], error=str(e))

    # Step 9b: Apply linking boost to trust score
    try:
        from backend_blockid.ml.behavioral_linking import calculate_linking_boost

        boost_conn = await get_conn()
        try:
            boost, linking_reasons = await calculate_linking_boost(wallet, boost_conn)
            if boost != 0 or linking_reasons:
                # Insert linking reason codes
                for code in linking_reasons:
                    weight = get_reason_weights().get(code, 0)
                    await insert_wallet_reason(
                        wallet=wallet,
                        reason_code=code,
                        weight=weight,
                        confidence=1.0,
                    )

                if linking_reasons:
                    for code in linking_reasons:
                        await boost_conn.execute(
                            """
                            INSERT INTO wallet_reasons (wallet, reason_code, created_at)
                            VALUES ($1, $2, NOW())
                            ON CONFLICT (wallet, reason_code) DO NOTHING
                            """,
                            wallet,
                            code,
                        )

                if boost != 0:
                    # Step 1: fetch score_before for linking history
                    score_row = await boost_conn.fetchrow(
                        "SELECT score FROM trust_scores WHERE wallet = $1",
                        wallet,
                    )
                    score_before_linking = (
                        float(score_row["score"]) if score_row and score_row["score"] is not None else None
                    )
                    base_score = score_before_linking if score_before_linking is not None else 0.0
                    new_linking_score = min(97.0, max(0.0, base_score + float(boost)))

                    # Step 2: log LINKING change BEFORE UPDATE (non-fatal)
                    try:
                        logger.debug(
                            "score_history_linking_hook",
                            wallet=wallet[:16],
                            boost=boost,
                            linking_reasons=linking_reasons,
                            score_before=score_before_linking,
                        )
                        await log_score_change(
                            wallet=wallet,
                            score_before=score_before_linking,
                            score_after=float(new_linking_score),
                            change_category="LINKING",
                            triggered_by="linking_engine",
                            reason_codes=linking_reasons or None,
                            confidence=None,
                            metadata={
                                "boost": float(boost),
                                "signals_count": len(linking_reasons or []),
                            },
                        )
                    except Exception:
                        pass

                    # Step 3: apply linking boost to trust_scores
                    await boost_conn.execute(
                        """
                        UPDATE trust_scores
                        SET score = LEAST(97, GREATEST(0, score + $1)),
                            final_score = LEAST(97, GREATEST(0, COALESCE(final_score, score) + $1)),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE wallet = $2
                        """,
                        boost,
                        wallet,
                    )

                logger.debug(
                    "linking_boost_applied",
                    wallet=wallet[:16],
                    boost=boost,
                    linking_reasons=linking_reasons,
                )
        finally:
            await release_conn(boost_conn)
    except Exception as e:
        logger.debug("linking_boost_skip", wallet=wallet[:16], error=str(e))

    # Step 10: update_wallet_score_async (uses Daemon-enriched data if persisted)
    logger.debug("realtime_pipeline_step", step="update_wallet_score_async", wallet=wallet[:16])
    await update_wallet_score_async(wallet)

    # AUTO-MINT: Trigger Identity NFT mint if eligible
    # Fires when score >= 30 and wallet doesn't have NFT yet
    try:
        auto_mint_conn = await get_conn()
        try:
            # Check if already minted
            existing_nft = await auto_mint_conn.fetchrow(
                "SELECT mint_status FROM identity_nft WHERE wallet = $1",
                wallet,
            )
            already_minted = (
                existing_nft
                and (existing_nft.get("mint_status") or "").upper() == "MINTED"
            )

            if already_minted:
                logger.info(
                    "auto_mint_identity_nft_skip",
                    wallet=wallet[:16],
                    reason="already_minted",
                )
            else:
                elig = await check_eligibility(wallet, auto_mint_conn)
                if not elig["eligible"]:
                    logger.info(
                        "auto_mint_identity_nft_skip",
                        wallet=wallet[:16],
                        skip_reason="not_eligible",
                        eligibility_reason=elig.get("reason", ""),
                        trust_score=elig.get("trust_score"),
                        score_tier=elig.get("score_tier"),
                    )
                else:
                    logger.info(
                        "auto_mint_identity_nft_triggered",
                        wallet=wallet[:16],
                        trust_score=elig["trust_score"],
                        score_tier=elig.get("score_tier"),
                    )
                    # Import here to avoid circular imports
                    from backend_blockid.api_server.identity_api import (
                        MintRequest,
                        mint_identity_nft,
                    )
                    mint_req = MintRequest(wallet=wallet)
                    mint_result = await mint_identity_nft(mint_req)
                    logger.info(
                        "auto_mint_identity_nft_done",
                        wallet=wallet[:16],
                        success=mint_result.get("success", False),
                    )
        finally:
            await release_conn(auto_mint_conn)
    except Exception as e:
        # Non-fatal — auto-mint failure should never crash pipeline
        logger.warning(
            "auto_mint_identity_nft_skip",
            wallet=wallet[:16],
            reason="exception",
            error=str(e),
        )

    inserted_count = 1 if not existed_before else 0
    logger.debug(
        "realtime_pipeline_done",
        wallet=wallet[:16],
        trust_inserted=inserted_count,
    )
    return inserted_count


# ---------------------------------------------------------------------------
# Streaming pipeline (SSE / Realtime Investigator Mode)
# ---------------------------------------------------------------------------

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
        existed_before = await conn.fetchval(
            "SELECT 1 FROM trust_scores WHERE wallet = $1", wallet
        )
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
    try:
        from backend_blockid.oracle.wallet_profile_builder import build_wallet_profile
        await build_wallet_profile(wallet)
    except Exception as e:
        logger.debug("realtime_wallet_profile_skip", wallet=wallet[:16], error=str(e))

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

        # Step 5b: detect_positive_reasons — insert positive reasons AFTER 4b so DELETE does not wipe them
        try:
            from backend_blockid.ai_engine.positive_reasons import detect_positive_reasons
            positive = await detect_positive_reasons(wallet)
            for r in positive:
                await insert_wallet_reason(
                    wallet=wallet,
                    reason_code=r["code"],
                    weight=int(r.get("weight", 0)),
                    confidence=float(r.get("confidence", 1.0)),
                    tx_hash=r.get("tx_hash"),
                )
        except Exception as e:
            logger.warning(
                "realtime_positive_reasons_skip",
                wallet=wallet[:16],
                error=str(e),
            )

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

    # Step 8: Cyclops risk enrichment
    try:
        cyclops_data = await cyclops_analyze(wallet, max_depth=2)
        if cyclops_data:
            logger.info(
                "cyclops_enrichment_done",
                wallet=wallet[:16],
                risk_score=cyclops_data.get("risk_score"),
                risk_level=cyclops_data.get("risk_level"),
                is_sanctioned=cyclops_data.get("is_sanctioned"),
                nodes=cyclops_data.get("nodes_count"),
            )
            # Apply sanctions penalty
            if cyclops_data.get("is_sanctioned"):
                logger.warning("cyclops_sanctioned_wallet", wallet=wallet[:16])
                # Will be used in score adjustment below
            # Apply high risk penalty if Cyclops risk is HIGH or CRITICAL
            cyclops_risk_level = cyclops_data.get("risk_level", "")
            if cyclops_risk_level in ("HIGH", "CRITICAL"):
                logger.warning(
                    "cyclops_high_risk",
                    wallet=wallet[:16],
                    risk_level=cyclops_risk_level,
                    risk_score=cyclops_data.get("risk_score"),
                )
        else:
            cyclops_data = None
            logger.info("cyclops_enrichment_skipped", wallet=wallet[:16], reason="no result")
    except Exception as _e:
        cyclops_data = None
        logger.warning("cyclops_enrichment_error", wallet=wallet[:16], error=str(_e))

    # Persist Cyclops data to wallet_meta (best-effort)
    try:
        meta_conn = await get_conn()
        try:
            await meta_conn.execute(
                """
                INSERT INTO wallet_meta (wallet, cyclops_risk_score, cyclops_risk_level,
                    cyclops_is_sanctioned, cyclops_updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (wallet) DO UPDATE SET
                    cyclops_risk_score = EXCLUDED.cyclops_risk_score,
                    cyclops_risk_level = EXCLUDED.cyclops_risk_level,
                    cyclops_is_sanctioned = EXCLUDED.cyclops_is_sanctioned,
                    cyclops_updated_at = NOW()
                """,
                wallet,
                cyclops_data.get("risk_score") if cyclops_data else None,
                cyclops_data.get("risk_level") if cyclops_data else None,
                cyclops_data.get("is_sanctioned", False) if cyclops_data else False,
            )
        finally:
            await release_conn(meta_conn)
    except Exception:
        pass  # Column might not exist yet, non-fatal

    # Step 8b: Daemon-AI explanation
    try:
        if cyclops_data or True:  # always try to explain
            # Get current reasons from DB
            reason_conn = await get_conn()
            try:
                reason_rows = await reason_conn.fetch(
                    "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 LIMIT 10",
                    wallet,
                )
                reasons = [r["reason_code"] for r in reason_rows]
                score_row = await reason_conn.fetchrow(
                    "SELECT score, risk_level FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
                    wallet,
                )
                current_score = float(score_row["score"]) if score_row else 50.0
                current_tier = str(score_row["risk_level"]) if score_row else "MEDIUM"
            finally:
                await release_conn(reason_conn)

            ai_explanation = await explain_wallet_risk(
                wallet=wallet,
                trust_score=current_score,
                risk_tier=current_tier,
                reasons=reasons,
                cyclops_risk_level=cyclops_data.get("risk_level") if cyclops_data else None,
                cyclops_risk_score=cyclops_data.get("risk_score") if cyclops_data else None,
                is_sanctioned=cyclops_data.get("is_sanctioned", False) if cyclops_data else False,
            )

            if ai_explanation:
                # Store in wallet_meta
                explain_conn = await get_conn()
                try:
                    await explain_conn.execute(
                        """
                        INSERT INTO wallet_meta (wallet, ai_explanation, ai_explanation_updated_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (wallet) DO UPDATE SET
                            ai_explanation = EXCLUDED.ai_explanation,
                            ai_explanation_updated_at = NOW()
                        """,
                        wallet,
                        ai_explanation,
                    )
                    logger.info("daemon_ai_explanation_stored", wallet=wallet[:16])
                except Exception:
                    pass
                finally:
                    await release_conn(explain_conn)
    except Exception as _e:
        logger.warning("daemon_ai_pipeline_error", wallet=wallet[:16], error=str(_e))

    # Step 5.5 (streaming): behavioral_linking
    yield ("behavioral_linking", "Scanning for linked wallets", {"wallet": wallet[:16]})
    try:
        from backend_blockid.ml.behavioral_linking import run_linking_scan, save_suggestions
        link_conn = await get_conn()
        try:
            suggestions = await run_linking_scan(wallet, link_conn)
            if suggestions:
                handle_row = await link_conn.fetchrow(
                    "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
                    wallet,
                )
                handle = handle_row["handle"] if handle_row else None
                await save_suggestions(wallet, suggestions, handle, link_conn)
        finally:
            await release_conn(link_conn)
    except Exception as e:
        logger.debug("behavioral_linking_skip", wallet=wallet[:16], error=str(e))

    # Step 6 (streaming): finalize score
    await update_wallet_score_async(wallet)

    yield (
        "done",
        "Analysis complete",
        {
            "wallet": wallet[:16],
            "trust_inserted": 1 if not existed_before else 0,
            "cyclops_risk_score": cyclops_data.get("risk_score") if cyclops_data else None,
            "cyclops_is_sanctioned": cyclops_data.get("is_sanctioned") if cyclops_data else None,
            "cyclops_risk_level": cyclops_data.get("risk_level") if cyclops_data else None,
        },
    )
