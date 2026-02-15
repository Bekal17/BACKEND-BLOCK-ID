"""
Solana trust oracle publisher: read trust scores from DB and publish via update_trust_score.

Batches updates at a configurable interval. Retries failed transactions. Logs tx signatures.

Config via env:
- SOLANA_RPC_URL: RPC endpoint (default https://api.mainnet-beta.solana.com).
- ORACLE_PRIVATE_KEY: Base58 or JSON array of 64 bytes (oracle keypair).
- ORACLE_PROGRAM_ID: Deployed trust oracle program ID.
- PUBLISH_INTERVAL_SECONDS: Seconds between batch runs (default 60).
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.logging import get_logger

logger = get_logger(__name__)

# Anchor: instruction discriminator = first 8 bytes of sha256("global:instruction_name")
UPDATE_TRUST_SCORE_DISCRIMINATOR = hashlib.sha256(b"global:update_trust_score").digest()[:8]
SYS_PROGRAM_ID_STR = "11111111111111111111111111111111"
DEFAULT_PUBLISH_INTERVAL_SEC = 60.0
DEFAULT_MAX_UPDATES_PER_BATCH = 20
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SEC = 2.0


def _score_to_risk_level(score: float) -> int:
    """RiskLevel u8: Low=0, Medium=1, High=2, Critical=3."""
    if score < 30:
        return 3
    if score < 50:
        return 2
    if score < 70:
        return 1
    return 0


def _load_keypair(private_key: str) -> Any:
    """Load Keypair from ORACLE_PRIVATE_KEY: base58 string or JSON array of 64 bytes."""
    raw = private_key.strip()
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            if len(arr) >= 64:
                from solders.keypair import Keypair
                return Keypair.from_bytes(bytes(arr[:64]))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    try:
        import base58
        from solders.keypair import Keypair
        secret = base58.b58decode(raw)
        return Keypair.from_bytes(secret)
    except Exception as e:
        logger.warning("oracle_keypair_load_failed", error=str(e))
        raise ValueError("Invalid ORACLE_PRIVATE_KEY") from e


def _build_update_trust_score_instruction(
    program_id: Any,
    oracle_pubkey: Any,
    wallet_pubkey: Any,
    trust_score: int,
    risk_level: int,
    sys_program_id: Any,
) -> tuple[Any, Any]:
    """
    Build update_trust_score instruction and PDA for trust_score_account.
    Returns (Instruction, trust_score_account_pubkey).
    """
    from solders.instruction import Instruction, AccountMeta
    from solders.pubkey import Pubkey

    seeds = [b"trust_score", bytes(oracle_pubkey), bytes(wallet_pubkey)]
    trust_score_account, _ = Pubkey.find_program_address(seeds, program_id)

    # Data: 8-byte discriminator + u8 trust_score + u8 risk_level
    data = bytearray(UPDATE_TRUST_SCORE_DISCRIMINATOR)
    data.append(trust_score & 0xFF)
    data.append(risk_level & 0xFF)

    accounts = [
        AccountMeta(pubkey=trust_score_account, is_signer=False, is_writable=True),
        AccountMeta(pubkey=oracle_pubkey, is_signer=True, is_writable=False),
        AccountMeta(pubkey=wallet_pubkey, is_signer=False, is_writable=False),
        AccountMeta(pubkey=sys_program_id, is_signer=False, is_writable=False),
    ]
    ix = Instruction(program_id=program_id, data=bytes(data), accounts=accounts)
    return ix, trust_score_account


@dataclass
class PublisherConfig:
    """Config for the Solana trust oracle publisher (env or explicit)."""

    solana_rpc_url: str = field(default_factory=lambda: os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip())
    oracle_private_key: str = field(default_factory=lambda: (os.getenv("ORACLE_PRIVATE_KEY") or "").strip())
    oracle_program_id: str = field(default_factory=lambda: (os.getenv("ORACLE_PROGRAM_ID") or "TRUSTxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").strip())
    publish_interval_seconds: float = field(default_factory=lambda: float(os.getenv("PUBLISH_INTERVAL_SECONDS", str(int(DEFAULT_PUBLISH_INTERVAL_SEC)))))
    max_updates_per_batch: int = DEFAULT_MAX_UPDATES_PER_BATCH
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC

    def __post_init__(self) -> None:
        if self.publish_interval_seconds < 1.0:
            self.publish_interval_seconds = DEFAULT_PUBLISH_INTERVAL_SEC
        if not self.oracle_private_key:
            raise ValueError("ORACLE_PRIVATE_KEY must be set")


class TrustOraclePublisher:
    """
    Read trust scores from DB and publish to Solana via update_trust_score.
    Batches at publish_interval_seconds; retries failed txs; logs signatures.
    """

    def __init__(self, db: Any, config: PublisherConfig | None = None) -> None:
        self._db = db
        self._config = config or PublisherConfig()
        self._keypair = _load_keypair(self._config.oracle_private_key)
        from solders.pubkey import Pubkey
        self._program_id = Pubkey.from_string(self._config.oracle_program_id)
        self._sys_program_id = Pubkey.from_string(SYS_PROGRAM_ID_STR)
        self._client: Any = None

    def _client_ensure(self) -> Any:
        if self._client is None:
            from solana.rpc.api import Client
            self._client = Client(self._config.solana_rpc_url)
        return self._client

    def _fetch_pending_updates(self, limit: int) -> list[tuple[str, float]]:
        """Return list of (wallet, trust_score) from DB (tracked wallets with latest score)."""
        wallets = self._db.get_tracked_wallets(limit=limit * 2)
        if not wallets:
            return []
        latest = self._db.get_latest_trust_scores_for_wallets(wallets)
        out: list[tuple[str, float]] = []
        for w in wallets:
            rec = latest.get(w)
            if rec is None:
                continue
            score = float(rec.score)
            if score < 0 or score > 100:
                continue
            out.append((w.strip(), score))
            if len(out) >= limit:
                break
        return out

    def _send_batch(self, updates: list[tuple[str, float]]) -> list[str]:
        """Build one tx per update (or pack multiple instructions if tx size allows). Returns list of tx signatures."""
        from solders.pubkey import Pubkey
        from solana.transaction import Transaction

        client = self._client_ensure()
        oracle_pubkey = self._keypair.pubkey()
        signatures: list[str] = []
        # Solana tx size limit ~1232 bytes; one update_trust_score is small; we pack up to max_updates_per_batch
        max_per_tx = min(self._config.max_updates_per_batch, 20)
        for i in range(0, len(updates), max_per_tx):
            chunk = updates[i : i + max_per_tx]
            instructions = []
            for wallet_str, score in chunk:
                try:
                    wallet_pubkey = Pubkey.from_string(wallet_str)
                except Exception:
                    logger.warning("oracle_invalid_wallet", wallet_id=wallet_str[:16])
                    continue
                trust_score_u8 = max(0, min(100, int(round(score))))
                risk_level_u8 = _score_to_risk_level(score)
                ix, _ = _build_update_trust_score_instruction(
                    self._program_id,
                    oracle_pubkey,
                    wallet_pubkey,
                    trust_score_u8,
                    risk_level_u8,
                    self._sys_program_id,
                )
                instructions.append(ix)
            if not instructions:
                continue
            for attempt in range(self._config.retry_attempts):
                try:
                    resp = client.get_latest_blockhash()
                    recent_blockhash = getattr(resp, "value", None) or (resp.result.value if hasattr(resp, "result") and hasattr(resp.result, "value") else None)
                    if not recent_blockhash:
                        raise RuntimeError("No blockhash")
                    tx = Transaction(recent_blockhash=recent_blockhash, fee_payer=oracle_pubkey)
                    for ix in instructions:
                        tx.add(ix)
                    result = client.send_transaction(tx, self._keypair)
                    sig_val = getattr(result, "value", None) or (getattr(result, "result", None) and getattr(result.result, "value", None))
                    if sig_val:
                        sig = str(sig_val)
                        signatures.append(sig)
                        logger.info(
                            "oracle_tx_sent",
                            signature=sig,
                            instruction_count=len(instructions),
                        )
                    else:
                        err = getattr(result, "error", None) or getattr(result, "value", result)
                        raise RuntimeError(str(err))
                    break
                except Exception as e:
                    logger.warning(
                        "oracle_tx_failed",
                        attempt=attempt + 1,
                        error=str(e),
                    )
                    if attempt < self._config.retry_attempts - 1:
                        time.sleep(self._config.retry_backoff_sec * (attempt + 1))
                    else:
                        logger.error("oracle_tx_retries_exhausted", error=str(e))
        return signatures

    def run_once(self) -> int:
        """Fetch pending updates from DB, send batch, return number of tx signatures logged."""
        updates = self._fetch_pending_updates(self._config.max_updates_per_batch)
        if not updates:
            logger.debug("oracle_no_updates")
            return 0
        sigs = self._send_batch(updates)
        return len(sigs)

    def run_loop(self, stop_event: Any | None = None) -> None:
        """Run publish loop every publish_interval_seconds until stop_event is set."""
        import threading
        stop = stop_event or threading.Event()
        logger.info(
            "oracle_publisher_started",
            interval_sec=self._config.publish_interval_seconds,
            rpc_url=self._config.solana_rpc_url[:32] + "..." if len(self._config.solana_rpc_url) > 32 else self._config.solana_rpc_url,
        )
        while not stop.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.exception("oracle_publisher_tick_failed", error=str(e))
            deadline = time.monotonic() + self._config.publish_interval_seconds
            while not stop.is_set() and time.monotonic() < deadline:
                stop.wait(timeout=min(1.0, max(0, deadline - time.monotonic())))
        logger.info("oracle_publisher_stopped")


def run_publisher_loop(
    db: Any,
    config: PublisherConfig | None = None,
    stop_event: Any | None = None,
) -> None:
    """Convenience: create publisher and run loop. Config from env if not provided."""
    cfg = config or PublisherConfig()
    pub = TrustOraclePublisher(db, config=cfg)
    pub.run_loop(stop_event=stop_event)
