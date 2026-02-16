"""
Solana trust oracle publisher: read trust scores from DB and publish via update_trust_score.

- Uses ORACLE_PROGRAM_ID from env. Builds update_trust_score instruction from Anchor IDL (embedded TRUST_ORACLE_IDL).
- Sends trust_score update per wallet; logs tx signature; retries failed tx with backoff.
- Devnet: set SOLANA_DEVNET=1 or SOLANA_CLUSTER=devnet (or SOLANA_RPC_URL to devnet RPC).
- Safety: score delta threshold, max tx/min, dry_run, confirmation verification, full signature audit.
Config: SOLANA_RPC_URL, ORACLE_PRIVATE_KEY, ORACLE_PROGRAM_ID, PUBLISH_INTERVAL_SECONDS, etc.
"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.logging import get_logger

logger = get_logger(__name__)

DEFAULT_IDL_FILENAME = "blockid_oracle.json"
IDL_SUBPATH = os.path.join("target", "idl", DEFAULT_IDL_FILENAME)

# In-memory cache for loaded IDL and discriminator (avoids repeated file reads).
_idl_cache: dict[str, Any] | None = None
_discriminator_cache: bytes | None = None

SYS_PROGRAM_ID_STR = "11111111111111111111111111111111"
DEVNET_RPC_URL = "https://api.devnet.solana.com"
MAINNET_RPC_URL = "https://api.mainnet-beta.solana.com"
DEFAULT_PUBLISH_INTERVAL_SEC = 60.0

# Fallback IDL for instruction lookup when file is not used (no discriminator; loaded IDL takes precedence).
TRUST_ORACLE_IDL = {
    "version": "0.1.0",
    "name": "blockid_oracle",
    "instructions": [
        {
            "name": "update_trust_score",
            "accounts": [
                {"name": "trust_score_account", "writable": True, "signer": False},
                {"name": "oracle", "writable": False, "signer": True},
                {"name": "wallet", "writable": False, "signer": False},
                {"name": "system_program", "writable": False, "signer": False},
            ],
            "args": [
                {"name": "wallet", "type": "pubkey"},
                {"name": "score", "type": "u8"},
                {"name": "risk", "type": "u8"},
            ],
        },
    ],
}


def _resolve_idl_path() -> str | None:
    """Resolve path to target/idl/blockid_oracle.json. Tries ANCHOR_IDL_PATH, cwd, then repo root from __file__."""
    env_path = (os.getenv("ANCHOR_IDL_PATH") or "").strip()
    if env_path and os.path.isfile(env_path):
        return env_path
    cwd_path = os.path.join(os.getcwd(), IDL_SUBPATH)
    if os.path.isfile(cwd_path):
        return cwd_path
    try:
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        candidate = os.path.join(root, "target", "idl", DEFAULT_IDL_FILENAME)
        if os.path.isfile(candidate):
            return candidate
    except Exception:
        pass
    return None


def _load_idl() -> dict[str, Any]:
    """Load Anchor IDL from target/idl/blockid_oracle.json. Cached after first load."""
    global _idl_cache
    if _idl_cache is not None:
        return _idl_cache
    path = _resolve_idl_path()
    if not path:
        raise FileNotFoundError(
            "Anchor IDL not found. Set ANCHOR_IDL_PATH to target/idl/blockid_oracle.json or run from repo root after 'anchor build'."
        )
    with open(path, encoding="utf-8") as f:
        _idl_cache = json.load(f)
    logger.debug("oracle_idl_loaded", path=path)
    return _idl_cache


def _get_update_trust_score_discriminator() -> bytes:
    """Return 8-byte instruction discriminator for update_trust_score from Anchor IDL. Cached."""
    global _discriminator_cache
    if _discriminator_cache is not None:
        return _discriminator_cache
    idl = _load_idl()
    instructions = idl.get("instructions") or idl.get("instruction") or []
    for ix in instructions:
        if ix.get("name") == "update_trust_score":
            disc = ix.get("discriminator")
            if disc is not None and len(disc) == 8:
                _discriminator_cache = bytes(disc)
                return _discriminator_cache
            break
    raise ValueError(
        "IDL instruction 'update_trust_score' missing or has invalid discriminator (expected 8-byte array). "
        "Regenerate IDL with 'anchor build'."
    )
DEFAULT_MAX_UPDATES_PER_BATCH = 20
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SEC = 2.0
DEFAULT_SCORE_DELTA_THRESHOLD = 3.0
DEFAULT_MAX_TX_PER_MINUTE = 10
DEFAULT_CONFIRM_TIMEOUT_SEC = 30.0
DEFAULT_CONFIRM_POLL_INTERVAL_SEC = 1.0
DRY_RUN_SIGNATURE_PLACEHOLDER = "dry_run"


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
    """
    Load Keypair from ORACLE_PRIVATE_KEY. Supports:
    - File path to Solana keypair JSON (e.g. id.json with 64-byte array)
    - Inline JSON array of 64 bytes
    - Base58 private key string
    """
    from solders.keypair import Keypair

    raw = private_key.strip()
    if not raw:
        raise ValueError("ORACLE_PRIVATE_KEY is empty. Set a file path to keypair JSON or a base58 secret key.")

    # File path: load JSON and create keypair from array
    if os.path.isfile(raw):
        try:
            with open(raw, encoding="utf-8") as f:
                arr = json.load(f)
            if not isinstance(arr, list) or len(arr) < 64:
                raise ValueError(
                    f"ORACLE_PRIVATE_KEY file must contain a JSON array of at least 64 bytes; got {type(arr).__name__} of length {len(arr) if isinstance(arr, list) else 0}."
                )
            return Keypair.from_bytes(bytes(arr[:64]))
        except FileNotFoundError:
            raise ValueError(f"ORACLE_PRIVATE_KEY file not found: {raw}") from None
        except json.JSONDecodeError as e:
            raise ValueError(f"ORACLE_PRIVATE_KEY file is not valid JSON: {e}") from e
        except (TypeError, ValueError) as e:
            logger.warning("oracle_keypair_load_failed", error=str(e))
            raise ValueError(f"Invalid keypair data in ORACLE_PRIVATE_KEY file: {e}") from e

    # Inline JSON array
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            if len(arr) >= 64:
                return Keypair.from_bytes(bytes(arr[:64]))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # Base58 private key string
    try:
        import base58
        secret = base58.b58decode(raw)
        return Keypair.from_bytes(secret)
    except Exception as e:
        logger.warning("oracle_keypair_load_failed", error=str(e))
        raise ValueError(
            "ORACLE_PRIVATE_KEY is not a valid file path, JSON array, or base58 secret key. "
            "Use a path to id.json or a base58-encoded key."
        ) from e


def build_update_trust_score_instruction(
    program_id: Any,
    oracle_pubkey: Any,
    wallet_pubkey: Any,
    trust_score: int,
    risk_level: int,
    sys_program_id: Any,
    *,
    idl: dict[str, Any] | None = None,
) -> tuple[Any, Any]:
    """
    Build update_trust_score instruction: update_trust_score(wallet, score, risk).
    Validates score 0–100 and risk 0–3. Returns (Instruction, trust_score_account_pubkey).
    """
    idl = idl or TRUST_ORACLE_IDL
    ix_def = next((i for i in idl.get("instructions", []) if i.get("name") == "update_trust_score"), None)
    if not ix_def:
        raise ValueError("IDL missing update_trust_score instruction")

    score = int(trust_score)
    risk = int(risk_level)
    if not 0 <= score <= 100:
        raise ValueError(f"score must be 0–100, got {score}")
    if not 0 <= risk <= 3:
        raise ValueError(f"risk must be 0–3, got {risk}")

    return _build_update_trust_score_instruction(
        program_id, oracle_pubkey, wallet_pubkey, score & 0xFF, risk & 0xFF, sys_program_id
    )


def _build_update_trust_score_instruction(
    program_id: Any,
    oracle_pubkey: Any,
    wallet_pubkey: Any,
    trust_score: int,
    risk_level: int,
    sys_program_id: Any,
) -> tuple[Any, Any]:
    """
    Build update_trust_score instruction for Anchor (wallet: Pubkey, score: u8, risk: u8).
    Data: 8-byte discriminator (from IDL) + 32-byte wallet + 1-byte score + 1-byte risk.
    AccountMeta order must match Anchor IDL exactly; Anchor derives signer from AccountMeta.is_signer,
    not from the transaction's signer list, so oracle must have is_signer=True here to avoid 3010 AccountNotSigner.
    """
    from solders.instruction import Instruction, AccountMeta
    from solders.pubkey import Pubkey

    seeds = [b"trust_score", bytes(oracle_pubkey), bytes(wallet_pubkey)]
    trust_score_account, _ = Pubkey.find_program_address(seeds, program_id)

    wallet_bytes = bytes(wallet_pubkey)
    if len(wallet_bytes) != 32:
        raise ValueError(f"Wallet pubkey must be 32 bytes, got {len(wallet_bytes)}")

    discriminator = _get_update_trust_score_discriminator()
    score_byte = trust_score & 0xFF
    risk_byte = risk_level & 0xFF
    data_bytes = (
        discriminator
        + wallet_bytes
        + bytes([score_byte, risk_byte])
    )
    logger.debug(
        "oracle_instruction_data",
        length=len(data_bytes),
        hex=data_bytes.hex(),
    )

    # Order must match Anchor IDL: 1 oracle (signer + writable), 2 wallet (readonly),
    # 3 trust_score_account (writable), 4 system_program (readonly).
    # Anchor checks signer from AccountMeta.is_signer, not from the tx signer list; oracle must be True here.
    accounts = [
        AccountMeta(pubkey=oracle_pubkey, is_signer=True, is_writable=True),
        AccountMeta(pubkey=wallet_pubkey, is_signer=False, is_writable=False),
        AccountMeta(pubkey=trust_score_account, is_signer=False, is_writable=True),
        AccountMeta(pubkey=sys_program_id, is_signer=False, is_writable=False),
    ]
    print("=== DEBUG ACCOUNTS ===")
    for a in accounts:
        print(a.pubkey, "signer:", a.is_signer, "writable:", a.is_writable)
    print("======================")

    ix = Instruction(program_id=program_id, data=data_bytes, accounts=accounts)
    return ix, trust_score_account


def get_trust_score_pda(program_id: Any, oracle_pubkey: Any, wallet_pubkey: Any) -> Any:
    """Derive trust score PDA. Seeds: [b'trust_score', oracle, wallet]."""
    from solders.pubkey import Pubkey
    seeds = [b"trust_score", bytes(oracle_pubkey), bytes(wallet_pubkey)]
    pda, _ = Pubkey.find_program_address(seeds, program_id)
    return pda


# TrustScoreAccount layout: 8 discriminator + 32 wallet + 1 trust_score + 1 risk_level + 8 last_updated + 32 oracle
TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN = 8
TRUST_SCORE_ACCOUNT_WALLET_LEN = 32
TRUST_SCORE_ACCOUNT_TRUST_SCORE_OFFSET = TRUST_SCORE_ACCOUNT_DISCRIMINATOR_LEN + TRUST_SCORE_ACCOUNT_WALLET_LEN  # 40
TRUST_SCORE_ACCOUNT_MIN_LEN = TRUST_SCORE_ACCOUNT_TRUST_SCORE_OFFSET + 2  # at least trust_score + risk_level


def parse_trust_score_account_data(data: bytes) -> tuple[int, int] | None:
    """
    Parse TrustScoreAccount data. Returns (trust_score: 0-100, risk_level: 0-3) or None if invalid.
    """
    if data is None or len(data) < TRUST_SCORE_ACCOUNT_MIN_LEN:
        return None
    trust_score = data[TRUST_SCORE_ACCOUNT_TRUST_SCORE_OFFSET] & 0xFF
    risk_level = data[TRUST_SCORE_ACCOUNT_TRUST_SCORE_OFFSET + 1] & 0xFF
    return (trust_score, risk_level)


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def _default_rpc_url() -> str:
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    if _parse_bool_env("SOLANA_DEVNET", False) or (os.getenv("SOLANA_CLUSTER") or "").strip().lower() == "devnet":
        return DEVNET_RPC_URL
    return MAINNET_RPC_URL


@dataclass
class SolanaPublisherConfig:
    """Config for the Solana trust oracle publisher (env or explicit). Devnet: set SOLANA_DEVNET=1 or SOLANA_CLUSTER=devnet."""

    solana_rpc_url: str = field(default_factory=_default_rpc_url)
    oracle_private_key: str = field(default_factory=lambda: (os.getenv("ORACLE_PRIVATE_KEY") or "").strip())
    oracle_program_id: str = field(default_factory=lambda: (os.getenv("ORACLE_PROGRAM_ID") or "TRUSTxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").strip())
    publish_interval_seconds: float = field(default_factory=lambda: float(os.getenv("PUBLISH_INTERVAL_SECONDS", str(int(DEFAULT_PUBLISH_INTERVAL_SEC)))))
    max_updates_per_batch: int = DEFAULT_MAX_UPDATES_PER_BATCH
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC
    score_delta_threshold: float = field(default_factory=lambda: float(os.getenv("SCORE_DELTA_THRESHOLD", str(DEFAULT_SCORE_DELTA_THRESHOLD))))
    max_tx_per_minute: int = field(default_factory=lambda: int(os.getenv("MAX_TX_PER_MINUTE", str(DEFAULT_MAX_TX_PER_MINUTE))))
    dry_run: bool = field(default_factory=lambda: _parse_bool_env("DRY_RUN", False))
    confirm_timeout_sec: float = field(default_factory=lambda: float(os.getenv("CONFIRM_TIMEOUT_SEC", str(int(DEFAULT_CONFIRM_TIMEOUT_SEC)))))
    confirm_poll_interval_sec: float = DEFAULT_CONFIRM_POLL_INTERVAL_SEC

    def __post_init__(self) -> None:
        if self.publish_interval_seconds < 1.0:
            self.publish_interval_seconds = DEFAULT_PUBLISH_INTERVAL_SEC
        if not self.oracle_private_key and not self.dry_run:
            raise ValueError("ORACLE_PRIVATE_KEY must be set when not in dry_run")
        if self.max_tx_per_minute < 1:
            self.max_tx_per_minute = 1
        if self.score_delta_threshold < 0:
            self.score_delta_threshold = 0.0


class SolanaTrustOraclePublisher:
    """
    Read updated trust scores from DB, publish via update_trust_score. Safety: score delta
    threshold, max tx per minute, dry_run, confirmation verification. Log all tx signatures.
    Must never spam network.
    """

    def __init__(self, db: Any, config: SolanaPublisherConfig | None = None) -> None:
        self._db = db
        self._config = config or SolanaPublisherConfig()
        self._keypair: Any = None
        if not self._config.dry_run:
            self._keypair = _load_keypair(self._config.oracle_private_key)
        from solders.pubkey import Pubkey
        self._program_id = Pubkey.from_string(self._config.oracle_program_id)
        self._sys_program_id = Pubkey.from_string(SYS_PROGRAM_ID_STR)
        self._client: Any = None
        self._last_published: dict[str, float] = {}
        self._tx_timestamps: deque[float] = deque(maxlen=1000)

    def _client_ensure(self) -> Any:
        if self._client is None:
            from solana.rpc.api import Client
            self._client = Client(self._config.solana_rpc_url)
        return self._client

    def _fetch_pending_updates(self, limit: int) -> list[tuple[str, float]]:
        """Read updated trust scores from DB; filter by score-delta threshold."""
        wallets = self._db.get_tracked_wallets(limit=limit * 2)
        if not wallets:
            return []
        latest = self._db.get_latest_trust_scores_for_wallets(wallets)
        out: list[tuple[str, float]] = []
        thresh = self._config.score_delta_threshold
        for w in wallets:
            rec = latest.get(w)
            if rec is None:
                continue
            score = float(rec.score)
            if score < 0 or score > 100:
                continue
            w = w.strip()
            last = self._last_published.get(w)
            if last is not None and abs(score - last) <= thresh:
                continue
            out.append((w, score))
            if len(out) >= limit:
                break
        return out

    def _verify_confirmation(self, signature: str) -> bool:
        """Poll for tx confirmation until timeout. Log result. Return True if confirmed."""
        client = self._client_ensure()
        timeout = self._config.confirm_timeout_sec
        interval = self._config.confirm_poll_interval_sec
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                from solders.signature import Signature
                sig = Signature.from_string(signature)
                resp = client.get_signature_statuses([sig])
                statuses = getattr(resp, "value", None) or (
                    getattr(resp.result, "value", None) if hasattr(resp, "result") else None
                )
                if statuses and len(statuses) > 0:
                    st = statuses[0]
                    if st is None:
                        time.sleep(interval)
                        continue
                    err = getattr(st, "err", None)
                    confirm = getattr(st, "confirmation_status", None) or ""
                    if err is not None:
                        logger.warning(
                            "oracle_tx_confirm_failed",
                            signature=signature,
                            reason="transaction_failed",
                            err=str(err),
                        )
                        return False
                    if confirm in ("confirmed", "finalized"):
                        logger.info("oracle_tx_confirmed", signature=signature, confirmation_status=confirm)
                        return True
                time.sleep(interval)
            except Exception as e:
                logger.warning("oracle_tx_confirm_poll_error", signature=signature, error=str(e))
                time.sleep(interval)
        logger.warning(
            "oracle_tx_confirm_failed",
            signature=signature,
            reason="timeout",
            timeout_sec=timeout,
        )
        return False

    def _send_batch(self, updates: list[tuple[str, float]]) -> int:
        """
        Build and send transactions for batch. Rate limit (max tx/min), dry_run support,
        confirmation verification. Update _last_published only on successful send.
        Log all tx signatures; audit batch at end.
        """
        from solders.pubkey import Pubkey
        from solana.transaction import Transaction

        cfg = self._config
        batch_signatures: list[str] = []
        client = self._client_ensure() if not cfg.dry_run else None
        oracle_pubkey = self._keypair.pubkey() if self._keypair else None
        max_per_tx = min(cfg.max_updates_per_batch, 20)

        for i in range(0, len(updates), max_per_tx):
            window_start = time.monotonic() - 60.0
            while self._tx_timestamps and self._tx_timestamps[0] < window_start:
                self._tx_timestamps.popleft()
            if len(self._tx_timestamps) >= cfg.max_tx_per_minute:
                logger.warning(
                    "oracle_rate_limited_tx_per_minute",
                    max_tx_per_minute=cfg.max_tx_per_minute,
                    current_in_window=len(self._tx_timestamps),
                )
                break

            chunk = updates[i : i + max_per_tx]
            instructions: list[Any] = []
            wallets_in_tx: list[str] = []
            chunk_scores: list[tuple[str, float]] = []
            for wallet_str, score in chunk:
                try:
                    wallet_pubkey = Pubkey.from_string(wallet_str)
                except Exception:
                    logger.warning("oracle_invalid_wallet", wallet_id=wallet_str[:16])
                    continue
                trust_score_u8 = max(0, min(100, int(round(score))))
                risk_level_u8 = _score_to_risk_level(score)
                if oracle_pubkey is not None:
                    ix, _ = build_update_trust_score_instruction(
                        self._program_id,
                        oracle_pubkey,
                        wallet_pubkey,
                        trust_score_u8,
                        risk_level_u8,
                        self._sys_program_id,
                    )
                    instructions.append(ix)
                wallets_in_tx.append(wallet_str)
                chunk_scores.append((wallet_str, score))

            if cfg.dry_run:
                sig = DRY_RUN_SIGNATURE_PLACEHOLDER
                batch_signatures.append(sig)
                logger.info(
                    "oracle_dry_run",
                    signature=sig,
                    wallets_updated=wallets_in_tx,
                    instruction_count=len(instructions) if instructions else len(wallets_in_tx),
                )
                for wallet in wallets_in_tx:
                    logger.info(
                        "oracle_wallet_updated",
                        signature=sig,
                        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                    )
                continue

            if not instructions:
                continue

            sent = False
            for attempt in range(cfg.retry_attempts):
                try:
                    resp = client.get_latest_blockhash()
                    recent_blockhash = getattr(resp, "value", None) or (
                        getattr(resp.result, "value", None) if hasattr(resp, "result") else None
                    )
                    if not recent_blockhash:
                        raise RuntimeError("No blockhash")
                    tx = Transaction(recent_blockhash=recent_blockhash, fee_payer=oracle_pubkey)
                    for ix in instructions:
                        tx.add(ix)
                    result = client.send_transaction(tx, self._keypair)
                    sig_val = getattr(result, "value", None) or (
                        getattr(result.result, "value", None) if hasattr(result, "result") else None
                    )
                    if sig_val:
                        sig = str(sig_val)
                        batch_signatures.append(sig)
                        sent = True
                        logger.info(
                            "oracle_tx_sent",
                            signature=sig,
                            instruction_count=len(instructions),
                            wallets_updated=wallets_in_tx,
                        )
                        for wallet in wallets_in_tx:
                            logger.info(
                                "oracle_wallet_updated",
                                signature=sig,
                                wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
                            )
                        self._verify_confirmation(sig)
                        for w, sc in chunk_scores:
                            self._last_published[w] = sc
                        self._tx_timestamps.append(time.monotonic())
                    else:
                        err = getattr(result, "error", None) or getattr(result, "value", result)
                        raise RuntimeError(str(err))
                    break
                except Exception as e:
                    backoff = cfg.retry_backoff_sec * (2 ** attempt)
                    logger.warning(
                        "oracle_tx_failed",
                        attempt=attempt + 1,
                        error=str(e),
                        backoff_sec=round(backoff, 1),
                    )
                    if attempt < cfg.retry_attempts - 1:
                        time.sleep(backoff)
                    else:
                        logger.error("oracle_tx_retries_exhausted", error=str(e), wallets=wallets_in_tx)
            if not sent:
                break

        if batch_signatures:
            logger.info("oracle_tx_signatures_batch", signatures=batch_signatures, count=len(batch_signatures))
        return len(batch_signatures)

    def run_once(self) -> int:
        """Fetch pending updates from DB, send batch, return number of tx signatures."""
        updates = self._fetch_pending_updates(self._config.max_updates_per_batch)
        if not updates:
            logger.debug("oracle_no_updates")
            return 0
        return self._send_batch(updates)

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


def run_solana_publisher_loop(
    db: Any,
    config: SolanaPublisherConfig | None = None,
    stop_event: Any | None = None,
) -> None:
    """Convenience: create publisher and run loop. Config from env if not provided."""
    cfg = config or SolanaPublisherConfig()
    pub = SolanaTrustOraclePublisher(db, config=cfg)
    pub.run_loop(stop_event=stop_event)
