"""
FastAPI server — read-only API over database.

Exposes GET /wallet/{address} returning latest trust score and anomaly flags.
Reads from database only; does not compute scores. Config via env (DB_PATH).
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Depends, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from solders.pubkey import Pubkey

from backend_blockid.api_server.db_wallet_tracking import (
    add_wallet as tracking_add_wallet,
    get_wallet_info as tracking_get_wallet_info,
    init_db as wallet_tracking_init_db,
    list_wallets as tracking_list_wallets,
)
from backend_blockid.api_server.badge_api import router as badge_router
from backend_blockid.api_server.graph_api import router as graph_router, investigation_router as graph_investigation_router
from backend_blockid.api_server.investigation_api import router as investigation_router
from backend_blockid.api_server.realtime_api import router as realtime_router
from backend_blockid.api_server.explain_wallet import router as explain_router
from backend_blockid.api_server.report_api import router as report_router
from backend_blockid.api_server.review_queue_api import router as review_queue_router
from backend_blockid.api_server.helius_api import router as helius_router
from backend_blockid.api_server.monitoring_api import router as monitoring_router
from backend_blockid.api_server.transaction_api import router as transaction_router
from backend_blockid.api_server.billing_api import router as billing_router
from backend_blockid.api_server.api_key_api import router as api_key_router
from backend_blockid.api_server.b2b_api import router as b2b_router
from backend_blockid.api_server.usage_api import router as usage_router
from backend_blockid.api_server.explorer_api import router as explorer_router
from backend_blockid.api.explorer_identity import router as explorer_identity_router
from backend_blockid.api.wallet_overview import router as wallet_overview_router
from backend_blockid.api.wallet_dashboard import router as wallet_dashboard_router
from backend_blockid.api.realtime_investigator import router as investigator_router
from backend_blockid.api_server.billing_middleware import BillingMiddleware
from backend_blockid.api_server.api_key_middleware import ApiKeyMiddleware, start_hourly_flush
from backend_blockid.api_server.metrics import generate_metrics, http_request_duration_seconds
from backend_blockid.api_server.trust_score import router as trust_router
from backend_blockid.database.pg_connection import init_db
from backend_blockid.blockid_logging import get_logger
from backend_blockid.oracle.realtime_wallet_pipeline import run_realtime_wallet_pipeline

logger = get_logger(__name__)

print("API USING DATABASE: PostgreSQL (asyncpg)")

# Periodic runner: interval and shutdown join timeout (seconds)
PERIODIC_INTERVAL_SEC = float(os.getenv("PERIODIC_INTERVAL_SEC", "30").strip() or "30")
PERIODIC_SHUTDOWN_JOIN_SEC = 15.0


# -----------------------------------------------------------------------------
# Config and dependency
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Response models
# -----------------------------------------------------------------------------

class WalletResponse(BaseModel):
    """GET /wallet/{address} response: latest trust score, flags, and reason codes from DB."""

    address: str = Field(..., description="Wallet address (base58)")
    trust_score: float = Field(..., ge=0, le=100, description="Latest trust score (0–100)")
    computed_at: int = Field(..., description="Unix timestamp when score was computed")
    flags: list[dict[str, Any]] = Field(default_factory=list, description="Anomaly flags from latest computation")
    reason_codes: list[str] = Field(default_factory=list, description="Trust score reason codes (e.g. NEW_WALLET, LOW_ACTIVITY)")


class TrackWalletRequest(BaseModel):
    """POST /track-wallet body: register a wallet for monitoring."""

    wallet: str = Field(..., min_length=8, max_length=64, description="Solana wallet address (base58)")


class TrackWalletResponse(BaseModel):
    """POST /track-wallet response."""

    wallet: str = Field(..., description="Wallet address registered")
    registered: bool = Field(..., description="True if newly added, False if already tracked")


# Step 2 Wallet Tracking (db_wallet_tracking)
class TrackWalletStep2Request(BaseModel):
    """POST /track_wallet body: wallet + optional label. Validated as Solana PublicKey."""

    wallet: str = Field(..., min_length=8, max_length=64, description="Solana wallet (base58)")
    label: str | None = Field(None, max_length=256, description="Optional label")


class TrackWalletStep2Response(BaseModel):
    """POST /track_wallet response."""

    wallet: str = Field(..., description="Wallet address")
    label: str = Field("", description="Label stored")
    registered: bool = Field(..., description="True if newly added, False if already tracked")


class ImportWalletsCsvResponse(BaseModel):
    """POST /import_wallets_csv response."""

    imported: int = Field(..., description="Number of wallets newly added")
    duplicates: int = Field(..., description="Number skipped (already in DB)")
    invalid: list[str] = Field(default_factory=list, description="Invalid wallet values rejected")


# -----------------------------------------------------------------------------
# Lifespan: start background periodic runner (never blocks API)
# -----------------------------------------------------------------------------


TRUST_SCORE_SYNC_INTERVAL_SEC = float(os.getenv("TRUST_SCORE_SYNC_INTERVAL_SEC", "300").strip() or "300")  # 5 min


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize PostgreSQL pool; start background workers when available."""
    await init_db()
    asyncio.create_task(start_hourly_flush(app))

    from backend_blockid.config import ensure_production_safe

    ensure_production_safe()

    try:
        wallet_tracking_init_db()
    except Exception as e:
        logger.warning("wallet_tracking_init_skip", error=str(e))

    yield


# -----------------------------------------------------------------------------
# App and routes
# -----------------------------------------------------------------------------

app = FastAPI(
    title="Backend BlockID API",
    description="Read-only API for wallet trust scores and flags (data from database).",
    version="0.1.0",
    lifespan=lifespan,
)


@app.options("/{rest_of_path:path}")
async def preflight_handler(rest_of_path: str):
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )


app.add_middleware(BillingMiddleware)
app.add_middleware(ApiKeyMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Track API request latency for Prometheus."""
    start = time.perf_counter()
    response = await call_next(request)
    dur = time.perf_counter() - start
    try:
        method = getattr(request, "method", "GET") or "GET"
        path = getattr(request, "url", None) and getattr(request.url, "path", "") or request.scope.get("path", "")
        # Normalize path for cardinality (e.g. /wallet/xxx -> /wallet/{address})
        if path.startswith("/wallet/") and len(path) > 8:
            path = "/wallet/{address}"
        elif path.startswith("/api/wallet/"):
            path = "/api/wallet/{address}"
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(dur)
    except Exception:
        pass
    return response


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint for Grafana scraping."""
    return Response(generate_metrics(), media_type="text/plain; charset=utf-8")


app.include_router(trust_router, prefix="/api", tags=["Trust Score"])
app.include_router(explain_router)
app.include_router(badge_router)
app.include_router(investigation_router)
app.include_router(graph_router)
app.include_router(graph_investigation_router)
app.include_router(realtime_router)
app.include_router(report_router)
app.include_router(review_queue_router)
app.include_router(helius_router)
app.include_router(monitoring_router)
app.include_router(transaction_router)
app.include_router(billing_router, prefix="/api/billing")
app.include_router(api_key_router, prefix="/api")
app.include_router(b2b_router)
app.include_router(usage_router, prefix="/api")
app.include_router(explorer_identity_router)
app.include_router(explorer_router)
app.include_router(wallet_overview_router)
app.include_router(wallet_dashboard_router)
app.include_router(investigator_router)



@app.post("/wallet/recalculate/{wallet}")
async def recalculate_wallet(wallet: str) -> dict[str, Any]:
    """
    Trigger realtime wallet pipeline to recalculate trust score for a single wallet.
    Powers the 'Recalculate Score' button in the BlockID dashboard.
    """
    wallet = wallet.strip()
    if len(wallet) < 32 or len(wallet) > 44:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "wallet": wallet,
                "message": "invalid wallet address",
            },
        )
    try:
        print(f"[API] Recalculating wallet score: {wallet}")
        logger.info("recalculate_wallet_start", wallet=wallet[:16])
        print("[RealtimePipeline] Starting wallet analysis")
        trust_inserted = await run_realtime_wallet_pipeline(wallet)
        print("[RealtimePipeline] Updating wallet score")
        print("[RealtimePipeline] Completed")
        logger.info("recalculate_wallet_done", wallet=wallet[:16], trust_inserted=trust_inserted)
        return {
            "status": "ok",
            "wallet": wallet,
            "message": "Wallet analysis completed",
        }
    except Exception as e:
        print(f"[API] ERROR recalculating wallet {wallet}: {e}")
        logger.exception("recalculate_wallet_error", wallet=wallet[:16], error=str(e))
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "wallet": wallet,
                "message": str(e),
            },
        )


@app.post("/track-wallet", response_model=TrackWalletResponse)
def track_wallet(body: TrackWalletRequest):
    """
    Register a wallet for monitoring. Inserts into tracked_wallets (db_wallet_tracking).
    Returns registered=True when newly added, registered=False when already tracked.
    """
    wallet = body.wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    logger.info("track_wallet_called", wallet=wallet[:16] + "...")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")
    registered = tracking_add_wallet(wallet)
    resp = TrackWalletResponse(wallet=wallet, registered=registered)
    return JSONResponse(
        status_code=201 if registered else 200,
        content=resp.model_dump(),
    )


@app.get("/wallet/{pubkey}/cluster")
async def get_wallet_cluster(pubkey: str):
    """
    Returns cluster graph data for a wallet.
    Read-only endpoint for Explorer 2.0.
    """
    pubkey = pubkey.strip()
    if not pubkey:
        raise HTTPException(status_code=400, detail="pubkey must be non-empty")
    try:
        Pubkey.from_string(pubkey)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    from backend_blockid.database.repositories import get_wallet_cluster_data

    result = await get_wallet_cluster_data(pubkey)
    return result


@app.get("/wallet/{address}", response_model=WalletResponse)
async def get_wallet(address: str) -> WalletResponse:
    """
    Return the latest trust score and anomaly flags for a wallet.

    Reads from PostgreSQL. Returns 404 if the wallet has no trust score record.
    """
    address = address.strip()
    if not address:
        raise HTTPException(status_code=400, detail="address must be non-empty")

    from backend_blockid.database.repositories import get_trust_score_latest

    latest = await get_trust_score_latest(address)
    if not latest:
        raise HTTPException(
            status_code=404,
            detail=f"No trust score found for wallet {address[:8]}...",
        )
    flags: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    meta_json = latest.get("metadata_json")
    if meta_json:
        try:
            meta = json.loads(meta_json)
            flags = meta.get("anomaly_flags") or []
            rc = meta.get("reason_codes")
            if isinstance(rc, list):
                reason_codes = [str(c) for c in rc]
        except Exception:
            pass
    if not reason_codes:
        try:
            info = tracking_get_wallet_info(address)
            if info and info.get("reason_codes"):
                rc_raw = info["reason_codes"]
                if isinstance(rc_raw, str):
                    parsed = json.loads(rc_raw)
                    if isinstance(parsed, list):
                        reason_codes = [str(c) for c in parsed]
                elif isinstance(rc_raw, list):
                    reason_codes = [str(c) for c in rc_raw]
        except Exception:
            pass

    return WalletResponse(
        address=address,
        trust_score=round(float(latest.get("score", 0)), 2),
        computed_at=int(latest.get("computed_at", 0)),
        flags=flags,
        reason_codes=reason_codes,
    )


async def _health_checks() -> dict:
    """Run health checks for database, Helius, pipeline, API."""
    import os
    db_ok = False
    helius_ok = False
    last_pipeline_success: bool | None = None
    try:
        from backend_blockid.database.pg_connection import get_conn, release_conn
        conn = await get_conn()
        await conn.fetchval("SELECT 1")
        await release_conn(conn)
        db_ok = True
    except Exception:
        pass
    try:
        key = (os.getenv("HELIUS_API_KEY") or "").strip()
        helius_ok = bool(key)
        if helius_ok:
            try:
                from backend_blockid.tools.helius_cost_monitor import get_today_stats, DAILY_LIMIT
                _, cost = get_today_stats()
                helius_ok = cost <= DAILY_LIMIT
            except Exception:
                helius_ok = True  # assume ok if we can't check
    except Exception:
        pass
    try:
        from backend_blockid.api_server.monitoring_api import _get_last_pipeline_run
        last = _get_last_pipeline_run()
        last_pipeline_success = last["success"] if last else None
    except Exception:
        pass
    return {
        "database_ok": db_ok,
        "helius_ok": helius_ok,
        "last_pipeline_success": last_pipeline_success,
        "api_status": "ok",
        "status": "ok" if db_ok else "degraded",
    }


@app.get("/health")
async def health() -> JSONResponse:
    """Liveness/readiness probe: database, Helius, last pipeline, API status."""
    data = await _health_checks()
    status = 200 if data.get("database_ok", False) else 503
    return JSONResponse(status_code=status, content=data)


# -----------------------------------------------------------------------------
# Debug: wallet status (tracked_wallets + on-chain PDA)
# -----------------------------------------------------------------------------


class WalletStatusResponse(BaseModel):
    """GET /debug/wallet_status/{wallet} response."""

    in_database: bool = Field(..., description="Wallet is in tracked_wallets (db_wallet_tracking)")
    onchain_pda_exists: bool = Field(..., description="Trust score PDA account exists on Solana")
    last_score: int | None = Field(None, description="Last score from tracked_wallets, or null")


@app.get("/debug/wallet_status/{wallet}", response_model=WalletStatusResponse)
def debug_wallet_status(wallet: str) -> WalletStatusResponse:
    """
    Debug: check if wallet is in tracked_wallets and if its trust score PDA exists on-chain.
    Uses db_wallet_tracking and Solana RPC (ORACLE_PROGRAM_ID, ORACLE_PRIVATE_KEY, SOLANA_RPC_URL).
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    info = tracking_get_wallet_info(wallet)
    in_database = info is not None
    last_score = info.get("last_score") if info else None

    onchain_pda_exists = False
    from backend_blockid.config.env import get_oracle_program_id, get_solana_rpc_url, load_blockid_env

    load_blockid_env()
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = get_oracle_program_id()
    rpc_url = get_solana_rpc_url()
    if oracle_key and program_id_str:
        try:
            from backend_blockid.oracle.solana_publisher import _load_keypair, get_trust_score_pda
            from solana.rpc.api import Client
            keypair = _load_keypair(oracle_key)
            oracle_pubkey = keypair.pubkey()
            program_id = Pubkey.from_string(program_id_str)
            wallet_pubkey = Pubkey.from_string(wallet)
            pda = get_trust_score_pda(program_id, wallet_pubkey)
            client = Client(rpc_url)
            resp = client.get_account_info(pda, encoding="base64")
            acc = getattr(resp, "value", None) or (
                getattr(resp.result, "value", None) if hasattr(resp, "result") else None
            )
            onchain_pda_exists = acc is not None and getattr(acc, "data", None) is not None
        except Exception as e:
            logger.debug("debug_wallet_status_rpc_error", wallet=wallet[:16] + "...", error=str(e))

    return WalletStatusResponse(
        in_database=in_database,
        onchain_pda_exists=onchain_pda_exists,
        last_score=last_score,
    )


# -----------------------------------------------------------------------------
# Analytics: wallet report (run_wallet_analysis without publishing)
# -----------------------------------------------------------------------------


@app.get("/wallet_report/{wallet}")
def get_wallet_report(wallet: str) -> dict[str, Any]:
    """
    Run full analytics pipeline for a wallet (scan -> risk -> trust). Returns
    metrics, risk, score, risk_label. Does not publish to the oracle.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    try:
        from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
        return run_wallet_analysis(wallet)
    except Exception as e:
        logger.exception("wallet_report_failed", wallet=wallet[:16] + "...", error=str(e))
        raise HTTPException(status_code=500, detail=f"Analytics failed: {e!s}") from e


# -----------------------------------------------------------------------------
# Step 2 Wallet Tracking (track_wallet, tracked_wallets, import_wallets_csv)
# -----------------------------------------------------------------------------


@app.post("/track_wallet", response_model=TrackWalletStep2Response)
def track_wallet_step2(body: TrackWalletStep2Request) -> JSONResponse:
    """
    Add a wallet to Step 2 tracking. Validates wallet with Solana PublicKey.
    Returns 201 when newly added, 200 when already tracked.
    """
    try:
        registered = tracking_add_wallet(body.wallet, body.label or "")
        label = (body.label or "").strip()
        return JSONResponse(
            status_code=201 if registered else 200,
            content=TrackWalletStep2Response(
                wallet=body.wallet.strip(),
                label=label,
                registered=registered,
            ).model_dump(),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("track_wallet_step2_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to add wallet") from e


@app.get("/tracked_wallets")
def get_tracked_wallets() -> list[dict[str, Any]]:
    """
    Return all wallets in Step 2 tracking (id, wallet, label, last_score, last_risk, last_checked, is_active).
    """
    try:
        return tracking_list_wallets()
    except Exception as e:
        logger.exception("tracked_wallets_list_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to list wallets") from e


@app.post("/import_wallets_csv", response_model=ImportWalletsCsvResponse)
async def import_wallets_csv(file: UploadFile = File(..., description="CSV with columns: wallet, label")) -> JSONResponse:
    """
    Import wallets from CSV. Expected columns: wallet, label (label optional).
    Invalid wallets are rejected (Solana PublicKey validation); duplicates are skipped.
    """
    try:
        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if "wallet" not in (reader.fieldnames or []):
            raise HTTPException(status_code=400, detail="CSV must have a 'wallet' column")
        imported = 0
        duplicates = 0
        invalid: list[str] = []
        for row in reader:
            wallet = (row.get("wallet") or "").strip()
            label = (row.get("label") or "").strip() or None
            if not wallet:
                continue
            try:
                added = tracking_add_wallet(wallet, label or "")
                if added:
                    imported += 1
                else:
                    duplicates += 1
            except ValueError:
                invalid.append(wallet)
        return JSONResponse(
            status_code=200,
            content=ImportWalletsCsvResponse(
                imported=imported,
                duplicates=duplicates,
                invalid=invalid,
            ).model_dump(),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("import_wallets_csv_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to import CSV") from e


@app.exception_handler(HTTPException)
def http_exception_handler(request: Any, exc: HTTPException) -> JSONResponse:
    """Consistent JSON error response for HTTPException."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
