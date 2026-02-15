"""
FastAPI server — read-only API over database.

Exposes GET /wallet/{address} returning latest trust score and anomaly flags.
Reads from database only; does not compute scores. Config via env (DB_PATH).
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

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
    created_at: int = Field(..., description="Unix timestamp when registered")
    registered: bool = Field(..., description="True if newly added, False if already tracked")


# -----------------------------------------------------------------------------
# Lifespan: start background periodic runner (never blocks API)
# -----------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start periodic runner in background thread on startup; signal stop on shutdown."""
    from backend_blockid.agent_worker.runner import (
        PeriodicRunnerConfig,
        run_periodic_worker,
        SHUTDOWN_JOIN_TIMEOUT_SEC,
    )

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
    yield
    stop_event.set()
    thread.join(timeout=SHUTDOWN_JOIN_TIMEOUT_SEC)
    if thread.is_alive():
        logger.warning(
            "api_periodic_runner_shutdown_timeout",
            timeout_sec=SHUTDOWN_JOIN_TIMEOUT_SEC,
        )
    else:
        logger.info("api_periodic_runner_stopped")


# -----------------------------------------------------------------------------
# App and routes
# -----------------------------------------------------------------------------

app = FastAPI(
    title="Backend BlockID API",
    description="Read-only API for wallet trust scores and flags (data from database).",
    version="0.1.0",
    lifespan=lifespan,
)


@app.post("/track-wallet", response_model=TrackWalletResponse)
def track_wallet(
    body: TrackWalletRequest,
    db: Database = Depends(get_db),
):
    """
    Register a wallet for monitoring. The agent automatically monitors all
    registered wallets. Returns 201 when newly added, 200 when already tracked.
    """
    wallet = body.wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    now = int(time.time())
    registered = db.add_tracked_wallet(wallet)
    created_at = now if registered else (db.get_tracked_wallet_created_at(wallet) or now)
    logger.info(
        "track_wallet",
        wallet_id=wallet,
        registered=registered,
        created_at=created_at,
    )
    resp = TrackWalletResponse(
        wallet=wallet,
        created_at=created_at,
        registered=registered,
    )
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


@app.exception_handler(HTTPException)
def http_exception_handler(request: Any, exc: HTTPException) -> JSONResponse:
    """Consistent JSON error response for HTTPException."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
