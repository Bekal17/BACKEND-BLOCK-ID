#!/usr/bin/env python3
"""
Publish trust score for one wallet and read it back.

Uses ORACLE_PROGRAM_ID and builds instruction from Anchor IDL. Retries failed tx;
logs signature; supports devnet (SOLANA_DEVNET=1 or SOLANA_CLUSTER=devnet).
Treats "confirmed" or "finalized" as success (configurable via SOLANA_COMMITMENT).

Usage:
  python publish_one_wallet.py [WALLET_PUBKEY] [SCORE]
  # or set env: WALLET, SCORE (default 75)

Env: SOLANA_RPC_URL, ORACLE_PRIVATE_KEY, ORACLE_PROGRAM_ID, WALLET, SCORE,
     SOLANA_COMMITMENT (default=confirmed), SOLANA_DEVNET, SOLANA_CLUSTER.
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# Run from project root so backend_blockid is importable
if __name__ == "__main__" and not __package__:
    _root = os.path.abspath(os.path.dirname(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)

from backend_blockid.database import get_database
from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.reason_builder import (
    get_reason_codes_for_wallet,
    get_weighted_risk_for_wallet,
    load_reason_cache,
)
from backend_blockid.oracle.solana_publisher import (
    DEVNET_RPC_URL,
    MAINNET_RPC_URL,
    _load_keypair,
    _parse_bool_env,
    _score_to_risk_level,
    build_update_trust_score_instruction,
    get_trust_score_pda,
    parse_trust_score_account_data,
)

logger = get_logger(__name__)

# Anchor PDA seeds for trust_score_account (lib.rs: seeds = [b"trust_score", oracle.key(), wallet.key()]).
# If IDL contains accounts[].pda.seeds we use those; otherwise this matches Anchor exactly.
TRUST_SCORE_PDA_SEED_PREFIX = b"trust_score"


def _load_idl_for_pda() -> dict[str, Any] | None:
    """Load Anchor IDL from env or target/idl/blockid_oracle.json. Returns None if not found."""
    import json
    path = (os.getenv("ANCHOR_IDL_PATH") or "").strip()
    if path and os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    for base in [os.getcwd(), os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))]:
        candidate = os.path.join(base, "target", "idl", "blockid_oracle.json")
        if os.path.isfile(candidate):
            try:
                with open(candidate, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return None


def derive_trust_score_pda(
    program_id: Any,
    oracle_pubkey: Any,
    wallet_pubkey: Any,
) -> Any:
    """
    Derive trust_score_account PDA to match Anchor exactly.
    Reads PDA seeds from IDL when present (accounts[].pda.seeds); otherwise uses
    [b"trust_score", oracle.to_bytes(), wallet.to_bytes()] per Anchor lib.rs.
    """
    from solders.pubkey import Pubkey

    idl = _load_idl_for_pda()
    seeds_list: list[bytes] = []

    if idl:
        instructions = idl.get("instructions") or idl.get("instruction") or []
        for ix in instructions:
            if ix.get("name") != "update_trust_score":
                continue
            accounts = ix.get("accounts") or []
            for acc in accounts:
                if acc.get("name") != "trust_score_account":
                    continue
                pda = acc.get("pda") or acc.get("seeds")
                if not pda:
                    break
                raw_seeds = pda.get("seeds") if isinstance(pda, dict) else pda
                if not raw_seeds:
                    break
                for s in raw_seeds:
                    if not isinstance(s, dict):
                        continue
                    kind = s.get("kind") or s.get("type")
                    if kind == "const":
                        val = s.get("value")
                        if isinstance(val, list):
                            seeds_list.append(bytes(val))
                        elif isinstance(val, str):
                            seeds_list.append(val.encode("utf-8"))
                    elif kind == "account":
                        path = (s.get("path") or s.get("account") or "").strip()
                        if path == "oracle":
                            seeds_list.append(bytes(oracle_pubkey))
                        elif path == "wallet":
                            seeds_list.append(bytes(wallet_pubkey))
                break
            break

    if not seeds_list:
        # Match Anchor lib.rs: seeds = [b"trust_score", oracle.key().as_ref(), wallet.key().as_ref()]
        oracle_bytes = bytes(oracle_pubkey)
        wallet_bytes = bytes(wallet_pubkey)
        if len(oracle_bytes) != 32 or len(wallet_bytes) != 32:
            raise ValueError("Oracle and wallet pubkeys must be 32 bytes for PDA derivation")
        seeds_list = [TRUST_SCORE_PDA_SEED_PREFIX, oracle_bytes, wallet_bytes]

    pda, _ = Pubkey.find_program_address(seeds_list, program_id)
    return pda


def verify_pda_exists(client: Any, pda_pubkey: Any) -> bool:
    """Return True if account exists at pda_pubkey, else False and print clear error."""
    resp = client.get_account_info(pda_pubkey)
    acc = getattr(resp, "value", None) or (getattr(resp.result, "value", None) if hasattr(resp, "result") else None)
    if acc is not None and getattr(acc, "data", None) is not None:
        return True
    print("PDA not created — check init_if_needed in Anchor")
    logger.error("publish_one_wallet_pda_not_found", pda=str(pda_pubkey))
    return False

DEFAULT_SCORE = 75
CONFIRM_TIMEOUT_SEC = 60.0

_RISK_U8_TO_STR = {0: "low", 1: "medium", 2: "high", 3: "critical"}


def _risk_u8_to_str(risk_u8: int) -> str:
    return _RISK_U8_TO_STR.get(max(0, min(3, int(risk_u8))), "medium")
CONFIRM_POLL_INTERVAL_SEC = 2.0
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SEC = 2.0
DEFAULT_COMMITMENT = "confirmed"


def _rpc_url() -> str:
    u = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if u:
        return u
    if _parse_bool_env("SOLANA_DEVNET", False) or (os.getenv("SOLANA_CLUSTER") or "").strip().lower() == "devnet":
        return DEVNET_RPC_URL
    return MAINNET_RPC_URL


def _commitment() -> str:
    c = (os.getenv("SOLANA_COMMITMENT") or "").strip().lower()
    return c or DEFAULT_COMMITMENT


def _is_devnet() -> bool:
    url = _rpc_url()
    return "devnet" in url or _parse_bool_env("SOLANA_DEVNET", False) or (
        (os.getenv("SOLANA_CLUSTER") or "").strip().lower() == "devnet"
    )


def _explorer_link(signature: str) -> str:
    base = "https://explorer.solana.com/tx/" + signature
    if _is_devnet():
        return base + "?cluster=devnet"
    return base + "?cluster=mainnet-beta"


def wait_for_tx_confirmation(signature: str, client: Any) -> bool:
    """
    Poll for transaction confirmation until timeout. Treats Confirmed OR Finalized (enum) as success.
    If timeout is reached but last known status was Confirmed, logs a warning and returns True.
    Handles RPC None response and temporary RPC errors by continuing to poll.
    Logs signature, explorer link, confirmation_status, slot, and elapsed time on success.
    """
    from solders.signature import Signature
    from solders.transaction_status import TransactionConfirmationStatus

    Confirmed = TransactionConfirmationStatus.Confirmed
    Finalized = TransactionConfirmationStatus.Finalized

    sig = Signature.from_string(signature)
    deadline = time.monotonic() + CONFIRM_TIMEOUT_SEC
    start = time.monotonic()
    last_confirmation_status: Any = None
    last_slot: int | None = None

    while time.monotonic() < deadline:
        try:
            resp = client.get_signature_statuses([sig])
            statuses = getattr(resp, "value", None) or (
                getattr(resp.result, "value", None) if hasattr(resp, "result") else None
            )
            if not statuses or len(statuses) == 0:
                time.sleep(CONFIRM_POLL_INTERVAL_SEC)
                continue
            st = statuses[0]
            if st is None:
                time.sleep(CONFIRM_POLL_INTERVAL_SEC)
                continue

            err = getattr(st, "err", None)
            if err is not None:
                elapsed = time.monotonic() - start
                logger.error(
                    "publish_one_wallet_tx_failed_on_chain",
                    signature=signature,
                    explorer=_explorer_link(signature),
                    err=str(err),
                    elapsed_sec=round(elapsed, 2),
                )
                return False

            confirmation_status = getattr(st, "confirmation_status", None)
            slot = getattr(st, "slot", None)
            if slot is not None:
                last_slot = slot
            if confirmation_status is not None:
                last_confirmation_status = confirmation_status

            if confirmation_status is not None and confirmation_status in (Confirmed, Finalized):
                elapsed = time.monotonic() - start
                explorer = _explorer_link(signature)
                status_str = str(confirmation_status) if confirmation_status is not None else "unknown"
                logger.info(
                    "publish_one_wallet_confirmed",
                    signature=signature,
                    explorer=explorer,
                    confirmation_status=status_str,
                    slot=slot,
                    elapsed_sec=round(elapsed, 2),
                )
                print(f"explorer={explorer}")
                print(f"confirmation_status={status_str} slot={slot} elapsed_sec={round(elapsed, 2)}")
                return True

        except Exception as e:
            logger.warning("publish_one_wallet_confirm_poll_error", error=str(e), signature=signature)
            time.sleep(CONFIRM_POLL_INTERVAL_SEC)

    elapsed = time.monotonic() - start
    if last_confirmation_status is not None and last_confirmation_status == Confirmed:
        status_str = str(last_confirmation_status)
        logger.warning(
            "publish_one_wallet_confirm_timeout_but_confirmed",
            signature=signature,
            explorer=_explorer_link(signature),
            confirmation_status=status_str,
            slot=last_slot,
            elapsed_sec=round(elapsed, 2),
            timeout_sec=CONFIRM_TIMEOUT_SEC,
        )
        print(f"explorer={_explorer_link(signature)}")
        print(f"confirmation_status=confirmed (timeout before finalized) elapsed_sec={round(elapsed, 2)}")
        return True

    logger.error(
        "publish_one_wallet_confirm_timeout",
        signature=signature,
        explorer=_explorer_link(signature),
        elapsed_sec=round(elapsed, 2),
        timeout_sec=CONFIRM_TIMEOUT_SEC,
    )
    return False


def _send_with_retry(client: Any, tx: Any, keypair: Any) -> str | None:
    for attempt in range(RETRY_ATTEMPTS):
        try:
            result = client.send_transaction(tx, keypair)
            sig_val = getattr(result, "value", None) or (
                getattr(result.result, "value", None) if hasattr(result, "result") else None
            )
            if sig_val:
                return str(sig_val)
            err = getattr(result, "error", None) or getattr(result, "value", result)
            raise RuntimeError(str(err))
        except Exception as e:
            logger.warning("publish_one_wallet_send_failed", attempt=attempt + 1, error=str(e))
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_BACKOFF_SEC * (2 ** attempt))
            else:
                raise
    return None


def _read_on_chain_score(client: Any, program_id: Any, oracle_pubkey: Any, wallet_pubkey: Any) -> tuple[int, int] | None:
    pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    resp = client.get_account_info(pda, encoding="base64")
    acc = getattr(resp, "value", None) or (getattr(resp.result, "value", None) if hasattr(resp, "result") else None)
    if not acc or not getattr(acc, "data", None):
        return None
    raw = acc.data
    if isinstance(raw, (list, tuple)) and len(raw) > 0:
        raw = raw[0]
    if isinstance(raw, str):
        data = base64.b64decode(raw)
    elif hasattr(raw, "data"):
        data = getattr(raw, "data", b"")
        if isinstance(data, str):
            data = base64.b64decode(data)
    elif isinstance(raw, bytes):
        data = raw
    else:
        data = bytes(raw) if raw else b""
    return parse_trust_score_account_data(data)


def main(wallet: str | None = None, score: int | None = None, risk: int | None = None) -> int:
    """
    Publish trust score for one wallet. Call from CLI or import.

    Args:
        wallet: Wallet pubkey (or from WALLET env / argparse when None)
        score: Score 0-100 (or from SCORE env / argparse when None)
        risk: Risk level 0-3 (or derived from score when None)

    Returns:
        0 on success, 1 on failure.
    """
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.transaction import Transaction

    if wallet is not None:
        # Called programmatically (e.g. from batch_publish)
        wallet_str = str(wallet).strip()
        if not wallet_str:
            logger.error("wallet is required when called programmatically")
            return 1
        final_score = score
        if final_score is None:
            try:
                final_score = int(os.getenv("SCORE", str(DEFAULT_SCORE)))
            except ValueError:
                final_score = DEFAULT_SCORE
        final_score = max(0, min(100, final_score))
        risk_level = risk
        if risk_level is not None:
            risk_level = max(0, min(3, int(risk_level)))
        else:
            risk_level = _score_to_risk_level(float(final_score))
    else:
        # CLI mode: parse argparse
        parser = argparse.ArgumentParser(description="Publish trust score for one wallet and read it back.")
        parser.add_argument("wallet", nargs="?", default=os.getenv("WALLET", ""), help="Wallet pubkey (or set WALLET)")
        parser.add_argument("score", nargs="?", type=int, default=None, help="Score 0-100 (default from SCORE env or 75)")
        parser.add_argument("risk", nargs="?", type=int, default=None, help="Risk level 0-3 (optional; else derived from score)")
        args = parser.parse_args()

        wallet_str = (args.wallet or "").strip()
        if not wallet_str:
            logger.error("WALLET is required. Set WALLET in .env or pass wallet pubkey as the first argument.")
            return 1
        final_score = args.score
        if final_score is None:
            try:
                final_score = int(os.getenv("SCORE", str(DEFAULT_SCORE)))
            except ValueError:
                final_score = DEFAULT_SCORE
        final_score = max(0, min(100, final_score))
        risk_level = args.risk
        if risk_level is not None:
            risk_level = max(0, min(3, int(risk_level)))
        else:
            risk_level = _score_to_risk_level(float(final_score))

    rpc_url = _rpc_url()
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key:
        logger.error("ORACLE_PRIVATE_KEY is required. Set it in .env or the environment.")
        return 1
    if not program_id_str:
        logger.error("ORACLE_PROGRAM_ID required")
        return 1

    client = Client(rpc_url)
    keypair = _load_keypair(oracle_key)
    oracle_pubkey = keypair.pubkey()
    program_id = Pubkey.from_string(program_id_str)
    wallet_pubkey = Pubkey.from_string(wallet_str)
    sys_program_id = Pubkey.from_string("11111111111111111111111111111111")

    pda_pubkey = derive_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    print("PDA:", pda_pubkey)
    logger.debug("oracle_pda_derived", pda=str(pda_pubkey))

    ix, ix_pda = build_update_trust_score_instruction(
        program_id, oracle_pubkey, wallet_pubkey, final_score, risk_level, sys_program_id
    )

    resp = client.get_latest_blockhash()
    blockhash_value = getattr(resp, "value", None) or (
        getattr(resp.result, "value", None) if hasattr(resp, "result") else None
    )
    if not blockhash_value:
        logger.error("get_latest_blockhash failed")
        return 1
    blockhash = getattr(blockhash_value, "blockhash", blockhash_value)

    print("program_id=%s PDA=%s (before sending tx)" % (program_id, pda_pubkey))
    tx = Transaction(recent_blockhash=blockhash, fee_payer=oracle_pubkey)
    tx.add(ix)

    try:
        signature = _send_with_retry(client, tx, keypair)
    except Exception as e:
        logger.exception("publish_one_wallet_send_failed", error=str(e))
        return 1

    if not signature:
        return 1

    logger.info("oracle_tx_sent", signature=signature, wallet=wallet_str[:16] + "...", score=final_score)
    print(f"tx_signature={signature}")

    if not wait_for_tx_confirmation(signature, client):
        return 1

    if not verify_pda_exists(client, pda_pubkey):
        pass  # Error already printed in verify_pda_exists

    parsed = _read_on_chain_score(client, program_id, oracle_pubkey, wallet_pubkey)
    if parsed is None:
        logger.warning("publish_one_wallet_read_back_missing", signature=signature)
        print("read_back=account_not_found")
        return 0

    stored_score, stored_risk = parsed
    logger.info("publish_one_wallet_read_back", signature=signature, stored_score=stored_score, stored_risk=stored_risk)
    print(f"stored_score={stored_score} stored_risk={stored_risk}")

    # Add wallet to tracked_wallets after transaction confirmation and read-back success
    wallet_addr = str(wallet_pubkey)
    print("DEBUG: before add_wallet wallet=", wallet_addr[:20] + "...")
    try:
        added = add_wallet(wallet_addr, label="auto_added")
        print("DEBUG: after add_wallet added=", added)
        if added:
            logger.info("wallet_auto_tracked", wallet=wallet_addr[:16] + "...")
    except Exception as e:
        logger.warning("wallet_auto_track_failed", wallet=wallet_addr[:16] + "...", error=str(e))
        print("DEBUG: add_wallet exception=", e)

    try:
        db_path = Path((os.getenv("DB_PATH") or "blockid.db").strip() or "blockid.db")
        db = get_database(db_path)
        metadata: dict = {"risk": stored_risk}
        weighted_risk = get_weighted_risk_for_wallet(wallet_addr)
        if weighted_risk > 0:
            metadata["weighted_risk_score"] = round(weighted_risk, 2)
        db.insert_trust_score(
            wallet_str,
            float(stored_score),
            computed_at=int(time.time()),
            metadata=metadata,
        )
        logger.info("publish_one_wallet_db_saved", wallet=wallet_str[:16] + "...")
    except Exception as e:
        logger.warning("publish_one_wallet_db_save_failed", error=str(e))

    # Persist score, risk, and reason_codes to wallet tracking DB
    risk_str = _risk_u8_to_str(stored_risk)
    reason_codes: list[str] = []
    try:
        reason_cache = load_reason_cache()
        reason_codes = get_reason_codes_for_wallet(wallet_addr, cache=reason_cache)
    except Exception as e:
        logger.debug("reason_builder_lookup_skipped", wallet=wallet_str[:16] + "...", error=str(e))
    try:
        update_wallet_score(wallet_addr, int(stored_score), risk=risk_str, reason_codes=reason_codes if reason_codes else None)
        if reason_codes:
            logger.debug("wallet_reason_codes_persisted", wallet=wallet_addr[:16] + "...", n=len(reason_codes))
    except Exception as e:
        logger.warning("update_wallet_score_failed", wallet=wallet_addr[:16] + "...", error=str(e))

    return 0


if __name__ == "__main__":
    sys.exit(main())


