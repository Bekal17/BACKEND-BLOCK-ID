"""
Rugpull token detection for BlockID analytics.

Scans wallet token accounts, extracts SPL token mint addresses, and compares
against a blacklist (scam_tokens.json). Used by the analytics pipeline and trust engine.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
DEFAULT_BLACKLIST_PATH = Path(__file__).resolve().parent.parent / "oracle" / "scam_tokens.json"


def _load_scam_tokens() -> set[str]:
    """Load rugpull/scam token mint addresses from JSON. Returns empty set on failure."""
    path_str = os.getenv("SCAM_TOKENS_PATH", "").strip() or str(DEFAULT_BLACKLIST_PATH)
    path = Path(path_str)
    if not path.is_file():
        logger.debug("rugpull_detector_blacklist_missing", path=path_str)
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("rugpull_detector_blacklist_load_failed", path=path_str, error=str(e))
        return set()
    if not isinstance(data, list):
        return set()
    return {str(m).strip() for m in data if m}


def _get_mints_from_token_accounts(token_accounts_resp: Any) -> list[str]:
    """From get_token_accounts_by_owner (jsonParsed) value, return list of mint addresses."""
    mints: list[str] = []
    value = getattr(token_accounts_resp, "value", token_accounts_resp)
    if value is None:
        return mints
    try:
        accounts = list(value)
    except TypeError:
        return mints
    for acct in accounts:
        mint = None
        try:
            parsed = getattr(acct, "account", None)
            if parsed is None and isinstance(acct, dict):
                parsed = acct.get("account")
            if parsed is None:
                continue
            data = getattr(parsed, "data", None)
            if data is None and isinstance(parsed, dict):
                data = parsed.get("data")
            if data is None:
                continue
            info = None
            if hasattr(data, "parsed"):
                p = data.parsed
                info = getattr(p, "info", None) if not isinstance(p, dict) else p.get("info")
            elif isinstance(data, dict):
                info = data.get("parsed", {}).get("info")
            if isinstance(info, dict):
                mint = info.get("mint")
        except (AttributeError, TypeError, KeyError):
            continue
        if not mint and isinstance(acct, dict):
            try:
                info = acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                mint = info.get("mint") if isinstance(info, dict) else None
            except (AttributeError, TypeError, KeyError):
                continue
        if mint:
            mints.append(str(mint))
    return mints


def _get_resp_value(resp: Any) -> Any:
    if resp is None:
        return None
    v = getattr(resp, "value", None)
    if v is not None:
        return v
    if hasattr(resp, "result"):
        return getattr(resp.result, "value", None)
    return None


def detect_rugpull_tokens(wallet: str) -> dict[str, Any]:
    """
    Scan wallet token accounts and detect holdings of blacklisted rugpull token mints.

    Returns:
        rugpull_tokens: distinct list of blacklisted token mints held by the wallet.
        rugpull_interactions: number of token accounts that hold a rugpull mint.
    """
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.rpc.types import TokenAccountOpts

    wallet = (wallet or "").strip()
    empty = {"rugpull_tokens": [], "rugpull_interactions": 0}
    if not wallet:
        return empty

    try:
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("rugpull_detector_invalid_wallet", wallet=wallet[:16] + "...", error=str(e))
        return empty

    blacklist = _load_scam_tokens()
    if not blacklist:
        return empty

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    try:
        client = Client(rpc_url)
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        resp = client.get_token_accounts_by_owner(
            pubkey,
            TokenAccountOpts(program_id=token_program, encoding="jsonParsed"),
        )
        token_value = _get_resp_value(resp)
    except Exception as e:
        logger.debug("rugpull_detector_token_accounts_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    mints = _get_mints_from_token_accounts(token_value)
    rugpull_tokens_set: set[str] = set()
    rugpull_interactions = 0
    for mint in mints:
        if mint in blacklist:
            rugpull_tokens_set.add(mint)
            rugpull_interactions += 1

    result = {
        "rugpull_tokens": sorted(rugpull_tokens_set),
        "rugpull_interactions": rugpull_interactions,
    }
    if rugpull_tokens_set:
        logger.info(
            "rugpull_detector_found",
            wallet=wallet[:16] + "...",
            rugpull_interactions=rugpull_interactions,
            rugpull_tokens=result["rugpull_tokens"],
        )
    return result
