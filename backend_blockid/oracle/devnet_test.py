"""
Devnet publish tester: publish one trust score, read back on-chain, compare and report PASS/FAIL.

Use for CI or manual checks against a deployed trust oracle on devnet. Safe for repeated runs
(same score publish is idempotent). Config via env: SOLANA_RPC_URL, ORACLE_PRIVATE_KEY,
ORACLE_PROGRAM_ID, TEST_WALLET, optional TEST_SCORE (default 75).
"""

from __future__ import annotations

import base64
import os
import sys
import time

from backend_blockid.logging import get_logger
from backend_blockid.oracle.solana_publisher import (
    _load_keypair,
    _score_to_risk_level,
    build_update_trust_score_instruction,
    get_trust_score_pda,
    parse_trust_score_account_data,
)

logger = get_logger(__name__)

DEFAULT_TEST_SCORE = 75
CONFIRM_TIMEOUT_SEC = 45.0
CONFIRM_POLL_INTERVAL_SEC = 1.5


def _env(name: str, default: str | None = None) -> str:
    v = (os.getenv(name) or "").strip()
    if v:
        return v
    if default is not None:
        return default
    return ""


def _run_devnet_publish_test() -> bool:
    rpc_url = _env("SOLANA_RPC_URL")
    if not rpc_url:
        logger.error("SOLANA_RPC_URL is required")
        return False
    oracle_key = _env("ORACLE_PRIVATE_KEY")
    if not oracle_key:
        logger.error("ORACLE_PRIVATE_KEY is required")
        return False
    program_id_str = _env("ORACLE_PROGRAM_ID")
    if not program_id_str:
        logger.error("ORACLE_PROGRAM_ID is required")
        return False
    test_wallet = _env("TEST_WALLET")
    if not test_wallet:
        logger.error("TEST_WALLET is required (wallet pubkey to publish and read back)")
        return False
    try:
        test_score_val = int(_env("TEST_SCORE", str(DEFAULT_TEST_SCORE)))
    except ValueError:
        test_score_val = DEFAULT_TEST_SCORE
    test_score_val = max(0, min(100, test_score_val))

    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.transaction import Transaction

    client = Client(rpc_url)
    keypair = _load_keypair(oracle_key)
    oracle_pubkey = keypair.pubkey()
    program_id = Pubkey.from_string(program_id_str)
    wallet_pubkey = Pubkey.from_string(test_wallet)
    sys_program_id = Pubkey.from_string("11111111111111111111111111111111")

    trust_score_u8 = test_score_val
    risk_level_u8 = _score_to_risk_level(float(test_score_val))
    ix, pda = build_update_trust_score_instruction(
        program_id,
        oracle_pubkey,
        wallet_pubkey,
        trust_score_u8,
        risk_level_u8,
        sys_program_id,
    )
    print("program_id:", program_id_str, "PDA:", pda, "(before sending tx)")

    # Send transaction
    try:
        resp = client.get_latest_blockhash()
        recent_blockhash = getattr(resp, "value", None) or (
            getattr(resp.result, "value", None) if hasattr(resp, "result") else None
        )
        if not recent_blockhash:
            logger.error("get_latest_blockhash returned no blockhash")
            return False
        tx = Transaction(recent_blockhash=recent_blockhash, fee_payer=oracle_pubkey)
        tx.add(ix)
        result = client.send_transaction(tx, keypair)
        sig_val = getattr(result, "value", None) or (
            getattr(result.result, "value", None) if hasattr(result, "result") else None
        )
        if not sig_val:
            err = getattr(result, "error", None) or getattr(result, "value", result)
            logger.error("send_transaction failed", error=str(err))
            return False
        signature = str(sig_val)
        logger.info("devnet_test_tx_sent", signature=signature, wallet=test_wallet[:16] + "...", score=test_score_val)
    except Exception as e:
        logger.exception("devnet_test_send_failed", error=str(e))
        return False

    # Wait for confirmation
    from solders.signature import Signature
    sig = Signature.from_string(signature)
    deadline = time.monotonic() + CONFIRM_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            status_resp = client.get_signature_statuses([sig])
            statuses = getattr(status_resp, "value", None) or (
                getattr(status_resp.result, "value", None) if hasattr(status_resp, "result") else None
            )
            if statuses and len(statuses) > 0:
                st = statuses[0]
                if st is not None:
                    err = getattr(st, "err", None)
                    if err is not None:
                        logger.error("devnet_test_tx_failed_on_chain", signature=signature, err=str(err))
                        return False
                    confirm = getattr(st, "confirmation_status", None) or ""
                    if confirm in ("confirmed", "finalized"):
                        break
            time.sleep(CONFIRM_POLL_INTERVAL_SEC)
        except Exception as e:
            logger.warning("devnet_test_confirm_poll_error", error=str(e))
            time.sleep(CONFIRM_POLL_INTERVAL_SEC)
    else:
        logger.error("devnet_test_confirm_timeout", signature=signature, timeout_sec=CONFIRM_TIMEOUT_SEC)
        return False

    # Read back account (request base64 for consistent decoding)
    pda = get_trust_score_pda(program_id, oracle_pubkey, wallet_pubkey)
    try:
        acc_resp = client.get_account_info(pda, encoding="base64")
        acc = getattr(acc_resp, "value", None) or (
            getattr(acc_resp.result, "value", None) if hasattr(acc_resp, "result") else None
        )
        if not acc or not getattr(acc, "data", None):
            logger.error("devnet_test_account_missing", pda=str(pda))
            return False
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
        parsed = parse_trust_score_account_data(data)
        if parsed is None:
            logger.error("devnet_test_account_parse_failed", data_len=len(data) if data else 0)
            return False
        stored_score, stored_risk = parsed
    except Exception as e:
        logger.exception("devnet_test_read_failed", error=str(e))
        return False

    if stored_score != test_score_val:
        logger.error(
            "devnet_test_score_mismatch",
            expected=test_score_val,
            stored=stored_score,
            signature=signature,
        )
        return False
    logger.info(
        "devnet_test_match",
        score=stored_score,
        risk_level=stored_risk,
        signature=signature,
    )
    return True


def main() -> None:
    ok = _run_devnet_publish_test()
    if ok:
        print("PASS")
        sys.exit(0)
    else:
        print("FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
