"""
FastAPI server — read-only API over database.

Exposes GET /wallet/{address} returning latest trust score and anomaly flags.
Reads from database only; does not compute scores. Config via env (DB_PATH).
"""

from __future__ import annotations

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
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from solders.pubkey import Pubkey

from backend_blockid.api_server.db_wallet_tracking import (
    add_wallet as tracking_add_wallet,
    get_wallet_info as tracking_get_wallet_info,
    init_db as wallet_tracking_init_db,
    list_wallets as tracking_list_wallets,
)
from backend_blockid.api_server.trust_score import router as trust_router
from backend_blockid.database import get_database, Database
from backend_blockid.logging import get_logger

logger = get_logger(__name__)

# Periodic runner: interval and shutdown join timeout (seconds)
PERIODIC_INTERVAL_SEC = float(os.getenv("PERIODIC_INTERVAL_SEC", "30").strip() or "30")
PERIODIC_SHUTDOWN_JOIN_SEC = 15.0


# -----------------------------------------------------------------------------
# Config and dependency
# -----------------------------------------------------------------------------

def get_db_path() -> Path:
    return Path(os.getenv("DB_PATH", "blockid.db").strip() or "blockid.db")


def get_db() -> Database:
    """Dependency: single Database instance per request (or app-scoped in production)."""
    return get_database(get_db_path())


# -----------------------------------------------------------------------------
# Response models
# -----------------------------------------------------------------------------

class WalletResponse(BaseModel):
    """GET /wallet/{address} response: latest trust score and flags from DB."""

    address: str = Field(..., description="Wallet address (base58)")
    trust_score: float = Field(..., ge=0, le=100, description="Latest trust score (0–100)")
    computed_at: int = Field(..., description="Unix timestamp when score was computed")
    flags: list[dict[str, Any]] = Field(default_factory=list, description="Anomaly flags from latest computation")


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
    """Start periodic runner and trust-score sync worker in background threads; signal stop on shutdown."""
    from backend_blockid.agent_worker.runner import (
        PeriodicRunnerConfig,
        run_periodic_worker,
        SHUTDOWN_JOIN_TIMEOUT_SEC,
    )
    from backend_blockid.api_server.trust_score_sync import run_trust_score_sync_loop

    stop_event = threading.Event()
    config = PeriodicRunnerConfig(
        db_path=get_db_path(),
        interval_sec=PERIODIC_INTERVAL_SEC,
        max_wallets_per_tick=int(os.getenv("PERIODIC_MAX_WALLETS", "2000").strip() or "2000"),
        max_tx_history_per_wallet=500,
    )
    thread = threading.Thread(
        target=run_periodic_worker,
        args=(config, stop_event),
        name="periodic-runner",
        daemon=True,
    )
    thread.start()
    logger.info("api_periodic_runner_started", interval_sec=config.interval_sec)

    sync_stop = threading.Event()
    sync_thread = threading.Thread(
        target=run_trust_score_sync_loop,
        args=(sync_stop, get_db_path(), TRUST_SCORE_SYNC_INTERVAL_SEC),
        name="trust-score-sync",
        daemon=True,
    )
    sync_thread.start()
    logger.info("trust_score_sync_started", interval_sec=TRUST_SCORE_SYNC_INTERVAL_SEC)

    try:
        wallet_tracking_init_db()
    except Exception as e:
        logger.warning("wallet_tracking_init_skip", error=str(e))

    yield

    stop_event.set()
    sync_stop.set()
    thread.join(timeout=SHUTDOWN_JOIN_TIMEOUT_SEC)
    sync_thread.join(timeout=15.0)
    if thread.is_alive():
        logger.warning(
            "api_periodic_runner_shutdown_timeout",
            timeout_sec=SHUTDOWN_JOIN_TIMEOUT_SEC,
        )
    else:
        logger.info("api_periodic_runner_stopped")
    if sync_thread.is_alive():
        logger.warning("trust_score_sync_shutdown_timeout", timeout_sec=15.0)
    else:
        logger.info("trust_score_sync_stopped")


# -----------------------------------------------------------------------------
# App and routes
# -----------------------------------------------------------------------------

app = FastAPI(
    title="Backend BlockID API",
    description="Read-only API for wallet trust scores and flags (data from database).",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(trust_router, prefix="/api", tags=["Trust Score"])



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


@app.get("/wallet/{address}", response_model=WalletResponse)
def get_wallet(address: str, db: Database = Depends(get_db)) -> WalletResponse:
    """
    Return the latest trust score and anomaly flags for a wallet.

    Reads from database only. Returns 404 if the wallet has no trust score record.
    """
    address = address.strip()
    if not address:
        raise HTTPException(status_code=400, detail="address must be non-empty")

    timeline = db.get_trust_score_timeline(address, limit=1)
    if not timeline:
        raise HTTPException(
            status_code=404,
            detail=f"No trust score found for wallet {address[:8]}...",
        )
    latest = timeline[0]
    flags: list[dict[str, Any]] = []
    if latest.metadata_json:
        try:
            meta = json.loads(latest.metadata_json)
            flags = meta.get("anomaly_flags") or []
        except Exception:
            pass

    return WalletResponse(
        address=address,
        trust_score=round(latest.score, 2),
        computed_at=latest.computed_at,
        flags=flags,
    )


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe: API is up."""
    return {"status": "ok"}


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
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    if oracle_key and program_id_str:
        try:
            from backend_blockid.oracle.solana_publisher import _load_keypair, get_trust_score_pda
            from solana.rpc.api import Client
            keypair = _load_keypair(oracle_key)
            oracle_pubkey = keypair.pubkey()
            program_id = Pubkey.from_string(program_id_str)
            wallet_pubkey = Pubkey.from_string(wallet)
            pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
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
