"""
Scam program detection for BlockID analytics.

Scans recent wallet transactions, extracts program IDs from instructions,
and compares against a blacklist (scam_programs.json). Used by the analytics pipeline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

MAX_TXS_TO_SCAN = 30
DEFAULT_BLACKLIST_PATH = Path(__file__).resolve().parent.parent / "oracle" / "scam_programs.json"


def _load_scam_blacklist() -> set[str]:
    """Load scam program IDs from JSON. Returns empty set on failure."""
    path_str = os.getenv("SCAM_PROGRAMS_PATH", "").strip() or str(DEFAULT_BLACKLIST_PATH)
    path = Path(path_str)
    if not path.is_file():
        logger.debug("scam_detector_blacklist_missing", path=path_str)
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("scam_detector_blacklist_load_failed", path=path_str, error=str(e))
        return set()
    if not isinstance(data, list):
        return set()
    return {str(p).strip() for p in data if p}


def _program_ids_from_tx(tx_value: Any) -> set[str]:
    """Extract program IDs from tx.value (transaction.message.instructions + meta.inner)."""
    programs: set[str] = set()
    if tx_value is None:
        return programs
    try:
        tx_obj = getattr(tx_value, "transaction", None) or (tx_value.get("transaction") if isinstance(tx_value, dict) else None)
        if tx_obj is None:
            return programs
        msg = getattr(tx_obj, "message", None) or (tx_obj.get("message") if isinstance(tx_obj, dict) else None)
        if msg is None:
            return programs
        instructions = getattr(msg, "instructions", None) or (msg.get("instructions", []) if isinstance(msg, dict) else [])
        for ix in instructions or []:
            pid = getattr(ix, "program_id", None) or getattr(ix, "programId", None)
            if pid is None and isinstance(ix, dict):
                pid = ix.get("program_id") or ix.get("programId")
            if pid is not None:
                programs.add(str(pid))
        meta = getattr(tx_value, "meta", None) or (tx_value.get("meta") if isinstance(tx_value, dict) else None)
        if meta is not None:
            inner = getattr(meta, "inner_instructions", None) or getattr(meta, "innerInstructions", None)
            if inner is None and isinstance(meta, dict):
                inner = meta.get("inner_instructions") or meta.get("innerInstructions")
            for group in inner or []:
                inxs = getattr(group, "instructions", None)
                if inxs is None and isinstance(group, dict):
                    inxs = group.get("instructions", [])
                for inner_ix in inxs or []:
                    pid = getattr(inner_ix, "program_id", None) or getattr(inner_ix, "programId", None)
                    if pid is None and isinstance(inner_ix, dict):
                        pid = inner_ix.get("program_id") or inner_ix.get("programId")
                    if pid is not None:
                        programs.add(str(pid))
    except (AttributeError, TypeError, KeyError):
        pass
    return programs


def _get_resp_value(resp: Any) -> Any:
    if resp is None:
        return None
    v = getattr(resp, "value", None)
    if v is not None:
        return v
    if hasattr(resp, "result"):
        return getattr(resp.result, "value", None)
    return None


def detect_scam_interactions(wallet: str) -> dict[str, Any]:
    """
    Scan recent transactions for the wallet and count interactions with blacklisted programs.

    Returns: { "scam_interactions": int, "scam_programs": list[str] }.
    scam_interactions = number of recent txs that contained at least one scam program.
    scam_programs = distinct scam program IDs seen.
    """
    from solders.pubkey import Pubkey
    from solders.signature import Signature
    from solana.rpc.api import Client

    wallet = (wallet or "").strip()
    empty = {"scam_interactions": 0, "scam_programs": []}
    if not wallet:
        return empty

    try:
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("scam_detector_invalid_wallet", wallet=wallet[:16] + "...", error=str(e))
        return empty

    blacklist = _load_scam_blacklist()
    if not blacklist:
        return empty

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    try:
        client = Client(rpc_url)
    except Exception as e:
        logger.warning("scam_detector_client_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    scam_programs_seen: set[str] = set()
    scam_interaction_count = 0

    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_TO_SCAN)
        sigs_value = _get_resp_value(sigs_resp)
        if sigs_value is None:
            return empty
        sigs_list = list(sigs_value)[:MAX_TXS_TO_SCAN]
    except Exception as e:
        logger.debug("scam_detector_signatures_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    for sig_entry in sigs_list:
        sig_val = getattr(sig_entry, "signature", None) or (sig_entry.get("signature") if isinstance(sig_entry, dict) else None)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
            tx_value = _get_resp_value(tx) if tx else None
        except Exception:
            continue
        if tx_value is None:
            continue
        program_ids = _program_ids_from_tx(tx_value)
        matched = program_ids & blacklist
        if matched:
            scam_interaction_count += 1
            scam_programs_seen |= matched

    result = {
        "scam_interactions": scam_interaction_count,
        "scam_programs": sorted(scam_programs_seen),
    }
    if scam_programs_seen:
        logger.info(
            "scam_detector_found",
            wallet=wallet[:16] + "...",
            scam_interactions=scam_interaction_count,
            scam_programs=result["scam_programs"],
        )
    return result
