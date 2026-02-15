#!/usr/bin/env python3
"""
Publish trust score for one wallet and read it back.

Uses ORACLE_PROGRAM_ID and builds instruction from Anchor IDL. Retries failed tx;
logs signature; supports devnet (SOLANA_DEVNET=1 or SOLANA_CLUSTER=devnet).

Usage:
  python publish_one_wallet.py [WALLET_PUBKEY] [SCORE]
  # or set env: WALLET, SCORE (default 75)

Env: SOLANA_RPC_URL, ORACLE_PRIVATE_KEY, ORACLE_PROGRAM_ID, WALLET, SCORE.
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import time
from typing import Any

# Run from project root so backend_blockid is importable
if __name__ == "__main__" and not __package__:
    _root = os.path.abspath(os.path.dirname(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)

from backend_blockid.logging import get_logger
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

DEFAULT_SCORE = 75
CONFIRM_TIMEOUT_SEC = 45.0
CONFIRM_POLL_INTERVAL_SEC = 1.5
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SEC = 2.0


def _rpc_url() -> str:
    u = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if u:
        return u
    if _parse_bool_env("SOLANA_DEVNET", False) or (os.getenv("SOLANA_CLUSTER") or "").strip().lower() == "devnet":
        return DEVNET_RPC_URL
    return MAINNET_RPC_URL


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


def _wait_confirmed(client: Any, signature: str) -> bool:
    from solders.signature import Signature
    sig = Signature.from_string(signature)
    deadline = time.monotonic() + CONFIRM_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            resp = client.get_signature_statuses([sig])
            statuses = getattr(resp, "value", None) or (
                getattr(resp.result, "value", None) if hasattr(resp, "result") else None
            )
            if statuses and len(statuses) > 0 and statuses[0] is not None:
                st = statuses[0]
                if getattr(st, "err", None) is not None:
                    logger.error("publish_one_wallet_tx_failed_on_chain", signature=signature, err=str(st.err))
                    return False
                if (getattr(st, "confirmation_status", None) or "") in ("confirmed", "finalized"):
                    return True
            time.sleep(CONFIRM_POLL_INTERVAL_SEC)
        except Exception as e:
            logger.warning("publish_one_wallet_confirm_poll_error", error=str(e))
            time.sleep(CONFIRM_POLL_INTERVAL_SEC)
    logger.error("publish_one_wallet_confirm_timeout", signature=signature, timeout_sec=CONFIRM_TIMEOUT_SEC)
    return False


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


def main() -> int:
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.transaction import Transaction

    parser = argparse.ArgumentParser(description="Publish trust score for one wallet and read it back.")
    parser.add_argument("wallet", nargs="?", default=os.getenv("WALLET", ""), help="Wallet pubkey (or set WALLET)")
    parser.add_argument("score", nargs="?", type=int, default=None, help="Score 0-100 (default from SCORE env or 75)")
    args = parser.parse_args()

    wallet_str = (args.wallet or "").strip()
    if not wallet_str:
        logger.error("WALLET required: set WALLET env or pass wallet pubkey as first argument")
        return 1
    score = args.score
    if score is None:
        try:
            score = int(os.getenv("SCORE", str(DEFAULT_SCORE)))
        except ValueError:
            score = DEFAULT_SCORE
    score = max(0, min(100, score))

    rpc_url = _rpc_url()
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id_str = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()
    if not oracle_key:
        logger.error("ORACLE_PRIVATE_KEY required")
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

    risk_level = _score_to_risk_level(float(score))
    ix, _ = build_update_trust_score_instruction(
        program_id, oracle_pubkey, wallet_pubkey, score, risk_level, sys_program_id
    )

    resp = client.get_latest_blockhash()
    recent_blockhash = getattr(resp, "value", None) or (
        getattr(resp.result, "value", None) if hasattr(resp, "result") else None
    )
    if not recent_blockhash:
        logger.error("get_latest_blockhash failed")
        return 1

    tx = Transaction(recent_blockhash=recent_blockhash, fee_payer=oracle_pubkey)
    tx.add(ix)

    try:
        signature = _send_with_retry(client, tx, keypair)
    except Exception as e:
        logger.exception("publish_one_wallet_send_failed", error=str(e))
        return 1

    if not signature:
        return 1

    logger.info("oracle_tx_sent", signature=signature, wallet=wallet_str[:16] + "...", score=score)
    print(f"tx_signature={signature}")

    if not _wait_confirmed(client, signature):
        return 1

    parsed = _read_on_chain_score(client, program_id, oracle_pubkey, wallet_pubkey)
    if parsed is None:
        logger.warning("publish_one_wallet_read_back_missing", signature=signature)
        print("read_back=account_not_found")
        return 0

    stored_score, stored_risk = parsed
    logger.info("publish_one_wallet_read_back", signature=signature, stored_score=stored_score, stored_risk=stored_risk)
    print(f"stored_score={stored_score} stored_risk={stored_risk}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
