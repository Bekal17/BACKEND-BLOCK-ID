"""
BlockID Step 2 Wallet Tracking â€” SQLAlchemy-backed wallet list and score history.

Uses DATABASE_URL for PostgreSQL when set; otherwise falls back to SQLite
(WALLET_TRACKING_DB_PATH or wallet_tracking.db). Same public API for FastAPI and batch_publish.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import Boolean, Column, Integer, String, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

Base = declarative_base()

# -----------------------------------------------------------------------------
# SQLAlchemy models
# -----------------------------------------------------------------------------


class TrackedWallet(Base):
    """
    Tracked wallet for Step 2: one row per wallet with optional label, last score, and reason codes.
    """

    __tablename__ = "tracked_wallets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet = Column(String(64), unique=True, nullable=False, index=True)
    label = Column(String(256), nullable=True)
    last_score = Column(Integer, nullable=True)
    last_risk = Column(String(32), nullable=True)
    last_checked = Column(Integer, nullable=True)  # Unix timestamp
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    reason_codes = Column(String(1024), nullable=True)  # JSON array of reason code strings

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "wallet": self.wallet,
            "label": self.label or "",
            "last_score": self.last_score,
            "last_risk": self.last_risk or "",
            "last_checked": self.last_checked,
            "is_active": self.is_active,
            "reason_codes": self.reason_codes,
        }


class ScoreHistory(Base):
    """
    History of scores per wallet (append-only). One row per publish.
    """

    __tablename__ = "score_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet = Column(String(64), nullable=False, index=True)
    score = Column(Integer, nullable=False)
    risk = Column(String(32), nullable=True)
    timestamp = Column(Integer, nullable=False, index=True)  # Unix


class WalletReasonEvidence(Base):
    """
    Evidence linking a wallet to a reason code: tx-level records supporting
    why a wallet was flagged (e.g. DRAINER_INTERACTION, RAPID_TOKEN_DUMP).
    Append-only audit trail for reason attribution.
    """

    __tablename__ = "wallet_reason_evidence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet = Column(String(64), nullable=False, index=True)
    reason_code = Column(String(64), nullable=False, index=True)
    tx_signature = Column(String(128), nullable=True, index=True)
    counterparty = Column(String(64), nullable=True, index=True)
    amount = Column(String(64), nullable=True)  # Lamports or human-readable; string avoids precision loss
    token = Column(String(64), nullable=True, index=True)  # Mint address or symbol
    timestamp = Column(Integer, nullable=True, index=True)  # Unix seconds

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "wallet": self.wallet,
            "reason_code": self.reason_code,
            "tx_signature": self.tx_signature,
            "counterparty": self.counterparty,
            "amount": self.amount,
            "token": self.token,
            "timestamp": self.timestamp,
        }


# -----------------------------------------------------------------------------
# Engine and session (DATABASE_URL â†’ Postgres, else SQLite)
# -----------------------------------------------------------------------------

DEFAULT_SQLITE_PATH = "wallet_tracking.db"


def _get_database_url() -> str:
    """Return BLOCKID_DB_URL, DATABASE_URL for Postgres if set; else SQLite from DATABASE_PATH, WALLET_TRACKING_DB_PATH, or default."""
    url = (os.getenv("BLOCKID_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    if url:
        return url
    path = (
        (os.getenv("DATABASE_PATH") or "").strip()
        or (os.getenv("WALLET_TRACKING_DB_PATH") or "").strip()
        or "blockid.db"
    )
    return f"sqlite:///{path}"


# Expose DB_URL for init_db and logging
DB_URL = _get_database_url()
print("DB_URL USED:", DB_URL)



_engine = None
_SessionLocal: sessionmaker | None = None


def _get_engine():
    """Create or return cached engine. Thread-safe for typical FastAPI/batch usage."""
    global _engine
    if _engine is None:
        url = _get_database_url()
        connect_args = {}
        if url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        _engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
        logger.info("wallet_tracking_engine", url=url.split("?")[0].split("//")[-1])
    return _engine


def _get_session_factory() -> sessionmaker:
    """Return session factory bound to engine."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_get_engine())
    return _SessionLocal


@contextmanager
def _session_scope() -> Iterator[Session]:
    """Context manager for a single session. Commits on success, rolls back on error."""
    factory = _get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _validate_wallet(wallet: str) -> None:
    """Validate Solana wallet using solana-py PublicKey. Raises ValueError if invalid."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise ValueError("wallet must be non-empty")
    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(wallet)
    except Exception as e:
        raise ValueError(f"Invalid Solana wallet: {e}") from e


def _migrate_reason_codes(engine: Any) -> None:
    """Add reason_codes column to tracked_wallets if missing (migration)."""
    from sqlalchemy import text
    url = _get_database_url()
    if "sqlite" in url:
        with engine.connect() as conn:
            try:
                result = conn.execute(text("PRAGMA table_info(tracked_wallets)"))
                rows = result.fetchall()
                if rows and not any(r[1] == "reason_codes" for r in rows):
                    conn.execute(text("ALTER TABLE tracked_wallets ADD COLUMN reason_codes TEXT"))
                    conn.commit()
                    logger.info("wallet_tracking_migration", added="reason_codes")
            except Exception as e:
                logger.debug("wallet_tracking_migration_skip", error=str(e))
    else:
        with engine.connect() as conn:
            try:
                conn.execute(text(
                    "ALTER TABLE tracked_wallets ADD COLUMN IF NOT EXISTS reason_codes VARCHAR(1024)"
                ))
                conn.commit()
            except Exception as e:
                if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                    logger.debug("wallet_tracking_migration_skip", error=str(e))


def init_db() -> None:
    """
    Create wallet tracking tables if they do not exist.
    Uses SQLAlchemy Base.metadata.create_all. Safe to call on every startup.
    Runs migration to add reason_codes column if missing.
    """
    try:
        engine = _get_engine()
        Base.metadata.create_all(bind=engine)
        _migrate_reason_codes(engine)
        logger.info("wallet_tracking_init_db", url=_get_database_url().split("?")[0].split("//")[-1])
    except Exception as e:
        logger.exception("wallet_tracking_init_db_failed", error=str(e))
        raise


def add_wallet(wallet: str, label: str | None = None) -> bool:
    """
    Insert wallet into tracked_wallets. Validates with Solana PublicKey before insert.
    Returns True if inserted, False if duplicate (ignored). Commits on success.
    """
    _validate_wallet(wallet)
    wallet = wallet.strip()
    label = (label or "").strip() or None
    try:
        with _session_scope() as session:
            w = TrackedWallet(wallet=wallet, label=label, is_active=True)
            session.add(w)
            session.flush()
        logger.info("wallet_added_to_db", wallet=wallet[:16] + "...")
        return True
    except IntegrityError:
        logger.info("wallet_already_exists", wallet=wallet[:16] + "...")
        return False
    except Exception as e:
        logger.exception("wallet_tracking_add_failed", wallet=wallet[:16], error=str(e))
        raise


def get_wallet_info(wallet: str) -> dict[str, Any] | None:
    """
    Return one wallet's row as dict (id, wallet, label, last_score, last_risk, last_checked, is_active)
    or None if not in tracked_wallets.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return None
    try:
        with _session_scope() as session:
            row = session.query(TrackedWallet).filter(TrackedWallet.wallet == wallet).first()
            return row.to_dict() if row else None
    except Exception as e:
        logger.exception("wallet_tracking_get_wallet_failed", wallet=wallet[:16], error=str(e))
        raise


def list_wallets() -> list[dict[str, Any]]:
    """
    Return all tracked wallets as list of dicts with keys:
    id, wallet, label, last_score, last_risk, last_checked, is_active.
    """
    try:
        with _session_scope() as session:
            rows = session.query(TrackedWallet).order_by(TrackedWallet.id).all()
            return [r.to_dict() for r in rows]
    except Exception as e:
        logger.exception("wallet_tracking_list_failed", error=str(e))
        raise


def update_wallet_score(
    wallet: str,
    score: int,
    risk: str | None = None,
    reason_codes: list[str] | None = None,
) -> None:
    """
    Update last_score, last_risk, last_checked, reason_codes for a wallet and append a row to score_history.
    reason_codes is stored as JSON list string; pass list of strings (e.g. ["NEW_WALLET", "LOW_ACTIVITY"]).
    """
    wallet = (wallet or "").strip()
    risk = (risk or "").strip() or None
    now = int(time.time())
    reason_codes_json: str | None = None
    if reason_codes is not None:
        try:
            import json
            reason_codes_json = json.dumps(reason_codes) if reason_codes else None
        except (TypeError, ValueError):
            reason_codes_json = None
    try:
        with _session_scope() as session:
            updates: dict[str, Any] = {
                "last_score": score,
                "last_risk": risk,
                "last_checked": now,
            }
            if reason_codes is not None:
                updates["reason_codes"] = reason_codes_json
            session.query(TrackedWallet).filter(TrackedWallet.wallet == wallet).update(updates)
            session.add(ScoreHistory(wallet=wallet, score=score, risk=risk, timestamp=now))
        logger.debug("wallet_tracking_score_updated", wallet=wallet[:16], score=score)
    except Exception as e:
        logger.exception("wallet_tracking_update_score_failed", wallet=wallet[:16], error=str(e))
        raise


def load_active_wallets() -> list[str]:
    """
    Return list of wallet addresses where is_active = true.
    Used by batch publish to decide which wallets to publish.
    """
    try:
        with _session_scope() as session:
            rows = session.query(TrackedWallet.wallet).filter(TrackedWallet.is_active.is_(True)).order_by(TrackedWallet.id).all()
            return [r[0] for r in rows]
    except Exception as e:
        logger.exception("wallet_tracking_load_active_failed", error=str(e))
        raise


def load_active_wallets_with_scores() -> list[tuple[str, int | None]]:
    """
    Return list of (wallet, last_score) for active wallets. last_score is None if never set.
    Used by batch publish to pass a score per wallet (or caller uses default).
    """
    try:
        with _session_scope() as session:
            rows = (
                session.query(TrackedWallet.wallet, TrackedWallet.last_score)
                .filter(TrackedWallet.is_active.is_(True))
                .order_by(TrackedWallet.id)
                .all()
            )
            return [(r[0], r[1]) for r in rows]
    except Exception as e:
        logger.exception("wallet_tracking_load_active_scores_failed", error=str(e))
        raise


def insert_reason_evidence(
    wallet: str,
    reason_code: str,
    *,
    tx_signature: str | None = None,
    counterparty: str | None = None,
    amount: str | None = None,
    token: str | None = None,
    timestamp: int | None = None,
) -> int:
    """
    Insert one wallet_reason_evidence row. Returns the new row id.
    """
    wallet = (wallet or "").strip()
    reason_code = (reason_code or "").strip()
    if not wallet or not reason_code:
        raise ValueError("wallet and reason_code are required")
    import time
    ts = timestamp if timestamp is not None else int(time.time())
    try:
        with _session_scope() as session:
            row = WalletReasonEvidence(
                wallet=wallet,
                reason_code=reason_code,
                tx_signature=(tx_signature or "").strip() or None,
                counterparty=(counterparty or "").strip() or None,
                amount=(str(amount).strip() or None) if amount is not None else None,
                token=(token or "").strip() or None,
                timestamp=ts,
            )
            session.add(row)
            session.flush()
            return row.id
    except Exception as e:
        logger.exception("insert_reason_evidence_failed", wallet=wallet[:16], reason_code=reason_code, error=str(e))
        raise


def list_reason_evidence(
    wallet: str | None = None,
    reason_code: str | None = None,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return evidence rows as dicts, optionally filtered by wallet and/or reason_code."""
    try:
        with _session_scope() as session:
            q = session.query(WalletReasonEvidence)
            if wallet:
                q = q.filter(WalletReasonEvidence.wallet == wallet.strip())
            if reason_code:
                q = q.filter(WalletReasonEvidence.reason_code == reason_code.strip())
            rows = q.order_by(WalletReasonEvidence.id.desc()).limit(limit).all()
            return [r.to_dict() for r in rows]
    except Exception as e:
        logger.exception("list_reason_evidence_failed", error=str(e))
        raise


def reset_engine_for_test() -> None:
    """
    Clear cached engine and session factory. For tests only; use with a new WALLET_TRACKING_DB_PATH.
    """
    global _engine, _SessionLocal
    _engine = None
    _SessionLocal = None


def __getattr__(name: str) -> Any:
    """Lazy engine: expose 'engine' without creating at import time until first access."""
    if name == "engine":
        return _get_engine()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
