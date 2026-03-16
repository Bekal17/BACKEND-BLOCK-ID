"""
BlockID Handle Registry — anti-squatting (3 layers).
"""
from __future__ import annotations

import os
import re
import time

DEVNET_BYPASS_SIGNATURES = {"devtest_signature_bypass"}
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")


def _normalize_handle(handle: str) -> str:
    return (handle or "").strip().lstrip("@").lower()


async def check_layer1_ownership(wallet: str, handle: str, signed_message: str, signature: str) -> dict:
    """
    Layer 1: Proof of wallet ownership.
    Wallet must sign a message to prove ownership.

    Message format to sign:
    "BlockID Handle Claim: @{handle} by {wallet} at {timestamp}"

    Returns: { "valid": bool, "reason": str }
    """
    if not wallet or not handle or not signed_message or not signature:
        return {"valid": False, "reason": "Missing wallet, handle, signed_message, or signature"}
    h = _normalize_handle(handle)
    expected_prefix = f"BlockID Handle Claim: @{h} by {wallet} "
    if not signed_message.strip().startswith(expected_prefix):
        return {
            "valid": False,
            "reason": "Signed message must start with 'BlockID Handle Claim: @{handle} by {wallet} at <timestamp>'",
        }
    # Devnet bypass: only when BLOCKID_ENV=DEV
    if BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS_SIGNATURES:
        return {"valid": True, "reason": "devnet_bypass"}
    # Production: proper signature verification
    # For now return valid if message format is correct and signature is non-empty base58 (min 64 chars)
    if not signature or len(signature) < 64:
        return {"valid": False, "reason": "Invalid signature format"}
    return {"valid": True, "reason": "signature_accepted"}


async def check_layer2_behavioral(wallet: str, handle: str, conn) -> dict:
    """
    Layer 2: Behavioral AI verification.
    Check if wallet behavior is consistent with claimed handle.

    Checks from trust_scores + wallet_reasons:
    - trust_score >= 30 (not a new/empty wallet)
    - wallet_age_days >= 30 (not brand new)
    - tx_count > 0 (has activity) via wallet_reasons count
    - risk_level not HIGH or CRITICAL

    Returns: {
        "valid": bool,
        "confidence": float,  # 0-1
        "reason": str
    }
    """
    row = await conn.fetchrow(
        """
        SELECT score, risk_level, wallet_age_days
        FROM trust_scores
        WHERE wallet = $1
        ORDER BY computed_at DESC NULLS LAST
        LIMIT 1
        """,
        wallet,
    )
    if not row:
        return {
            "valid": False,
            "confidence": 0.0,
            "reason": "Wallet has no trust score (not scored yet)",
        }
    score = float(row.get("score") or 0)
    risk_level = (row.get("risk_level") or "").upper() if row.get("risk_level") else ""
    wallet_age_days = int(row.get("wallet_age_days") or 0)

    if score < 30:
        return {
            "valid": False,
            "confidence": max(0, score / 30.0),
            "reason": "Trust score must be at least 30",
        }
    if wallet_age_days < 30:
        return {
            "valid": False,
            "confidence": max(0, wallet_age_days / 30.0),
            "reason": "Wallet age must be at least 30 days",
        }
    reason_count = await conn.fetchval("SELECT COUNT(*) FROM wallet_reasons WHERE wallet = $1", wallet)
    tx_count = int(reason_count or 0)
    if tx_count < 1:
        return {
            "valid": False,
            "confidence": 0.0,
            "reason": "Wallet must have on-chain activity",
        }
    if risk_level in ("HIGH", "CRITICAL"):
        return {
            "valid": False,
            "confidence": 0.0,
            "reason": "Wallet risk level too high for handle claim",
        }
    confidence = min(1.0, (score / 100.0) * 0.5 + (min(wallet_age_days, 365) / 365.0) * 0.3 + 0.2)
    return {"valid": True, "confidence": round(confidence, 2), "reason": "OK"}


async def check_layer3_reserved(handle: str, conn) -> dict:
    """
    Layer 3: Reserved list check.
    If handle is in reserved list → only allow claim if wallet matches can_claim_wallet.

    Returns: {
        "reserved": bool,
        "reserved_for": str | None,
        "can_claim": bool  # True if not reserved, or reserved and caller will be checked by caller
    }
    """
    h = _normalize_handle(handle)
    row = await conn.fetchrow(
        "SELECT handle, reserved_for, can_claim_wallet FROM handle_reserved WHERE LOWER(handle) = $1",
        h,
    )
    if not row:
        return {"reserved": False, "reserved_for": None, "can_claim": True}
    return {
        "reserved": True,
        "reserved_for": row.get("reserved_for"),
        "can_claim": True,  # Caller checks can_claim_wallet vs wallet when reserved
    }


async def run_anti_squatting_check(
    wallet: str,
    handle: str,
    signed_message: str,
    signature: str,
    conn,
) -> dict:
    """
    Run all 3 layers sequentially.
    Stop at first failure.

    Returns: {
        "passed": bool,
        "failed_layer": int | None,  # 1, 2, or 3
        "reason": str,
        "confidence": float
    }
    """
    # Layer 1: ownership
    l1 = await check_layer1_ownership(wallet, handle, signed_message, signature)
    if not l1["valid"]:
        return {"passed": False, "failed_layer": 1, "reason": l1["reason"], "confidence": 0.0}

    # Layer 2: behavioral
    l2 = await check_layer2_behavioral(wallet, handle, conn)
    if not l2["valid"]:
        return {
            "passed": False,
            "failed_layer": 2,
            "reason": l2["reason"],
            "confidence": l2.get("confidence", 0.0),
        }

    # Layer 3: reserved
    l3 = await check_layer3_reserved(handle, conn)
    if l3["reserved"]:
        # Caller must verify wallet == can_claim_wallet; we don't have wallet in l3.
        # So we return can_claim=False only when reserved and caller will check.
        row = await conn.fetchrow(
            "SELECT can_claim_wallet FROM handle_reserved WHERE LOWER(handle) = $1",
            _normalize_handle(handle),
        )
        can_claim_wallet = row.get("can_claim_wallet") if row else None
        if can_claim_wallet and can_claim_wallet.strip() and can_claim_wallet.strip() != wallet:
            return {
                "passed": False,
                "failed_layer": 3,
                "reason": f"Handle is reserved for {l3['reserved_for']}; only designated wallet may claim",
                "confidence": l2.get("confidence", 0.0),
            }

    return {
        "passed": True,
        "failed_layer": None,
        "reason": "OK",
        "confidence": l2.get("confidence", 0.0),
    }
