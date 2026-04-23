"""
BlockID Behavioral Linking — Phase 3.
Detect that two wallets likely belong to the same person using on-chain signals + Bayesian confidence.
User must always confirm; never auto-link without consent.
Reuses Bayesian math from bayesian_risk.py.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

LINKING_LIKELIHOODS: dict[str, float] = {
    "COMMON_KYC_SOURCE": 12.0,
    "DIRECT_TRANSFER": 8.0,
    "CEX_WITHDRAWAL_SESSION": 7.0,
    "BRIDGE_SAME_SOURCE": 7.0,
    "MARKET_EVENT_COREACTION": 5.0,
    "SAME_DEX_PAIR_SESSION": 4.0,
    "SAME_COUNTERPARTIES": 3.0,
    "SAME_NFT_COLLECTION": 2.0,
    "SAME_PROGRAMS": 1.8,
    "DISTRIBUTION_PATTERN": 0.1,
    "HIGH_COUNTERPARTY_SPREAD": 0.2,
}

LINKING_PRIOR = 0.05
CONFIDENCE_THRESHOLD_SUGGEST = float(os.getenv("LINKING_CONFIDENCE_THRESHOLD", "0.75"))
CONFIDENCE_THRESHOLD_STRONG = float(os.getenv("LINKING_STRONG_THRESHOLD", "0.90"))
CONFIDENCE_CAP = 0.95
SUGGESTION_EXPIRY_DAYS = int(os.getenv("LINKING_SUGGESTION_EXPIRY_DAYS", "30"))
DISTRIBUTION_THRESHOLD = int(os.getenv("DISTRIBUTION_WALLET_THRESHOLD", "10"))

KNOWN_CEX_ADDRESSES: dict[str, str] = {
    # ============================================================
    # OKX (verified on Solscan as "OKX: Hot Wallet X")
    # Primary hot wallet confirmed by user transaction test
    # ============================================================
    "is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV": "OKX",  # OKX Hot Wallet 1 (primary)

    # ============================================================
    # Binance (verified on Solscan)
    # ============================================================
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance",  # Binance Hot Wallet 2
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Binance",  # Binance Hot Wallet (was Bybit - verify)
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",  # Binance Hot Wallet
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Binance",  # Binance (was Coinbase - verify)
    "AobVSwdW9BbpMdJvTqeCN4hPAmh4rHm7vwLnQ5ATSyrS": "Binance",  # Binance Hot Wallet 3

    # ============================================================
    # Coinbase (verified on Solscan)
    # ============================================================
    "FxteHmLwG9nk1eL4pjNve3Eub2goGkkz6g6TbvdmW46a": "Coinbase",  # Coinbase Hot Wallet
    "H3v2e6BDt8Py6VPBkxu7HFgNAgHnrGbV5iQ2JDLqBMPD": "Coinbase",  # Coinbase 2

    # ============================================================
    # Bybit (verified on Solscan)
    # ============================================================
    "AC5RDfQFmDS1deWZos921JhjEKMDSBCDM8KFTBgq9aUF": "Bybit",  # Bybit Hot Wallet (was Binance - verify)

    # ============================================================
    # Kraken (verified on Solscan)
    # ============================================================
    "BeAMHyvuBNgNhpaPmFhiQkTtFEBjUYnAQSmNKFkY1b4M": "Kraken",  # Kraken Hot Wallet

    # ============================================================
    # KuCoin (verified on Solscan)
    # ============================================================
    "BmFdpraQhkiDPE5PCEPhEQGNJgKdY18Mar2XSoQtBBxS": "KuCoin",  # KuCoin

    # ============================================================
    # Indodax (Indonesia CEX — unique to Southeast Asia users)
    # TODO: Verify via Solscan labels post-hackathon
    # ============================================================
    # (kosong dulu, verify later)

    # ============================================================
    # STALE/UNVERIFIED — removed:
    # "8FmGDmDDkHoFaT6SaXgmHRLBMCcJWwNXpFWJQJMAfmHo": "Binance"  (old)
    # "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "OKX"       (old/wrong)
    # ============================================================
}

KNOWN_ONRAMP_ADDRESSES: dict[str, str] = {}

DEX_PROGRAM_PREFIXES = ("JUP", "whirLb", "9W959Dp", "srmqPvym")


async def detect_signals(wallet_a: str, wallet_b: str, conn) -> list[str]:
    """Detect behavioral signals between two wallets from transactions."""
    out: list[str] = []
    wa, wb = (wallet_a or "").strip(), (wallet_b or "").strip()
    if not wa or not wb or wa == wb:
        return out

    try:
        has_tx = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'transactions'"
        )
        if not has_tx:
            return out
    except Exception:
        return out

    # 1. DIRECT_TRANSFER
    direct = await conn.fetchval(
        """
        SELECT 1 FROM transactions
        WHERE ((sender = $1 AND receiver = $2) OR (sender = $2 AND receiver = $1))
        LIMIT 1
        """,
        wa,
        wb,
    )
    if direct:
        out.append("DIRECT_TRANSFER")

    # 2. COMMON_KYC_SOURCE
    cex_list = list(KNOWN_CEX_ADDRESSES.keys())
    if cex_list:
        senders_a = await conn.fetch(
            "SELECT DISTINCT sender FROM transactions WHERE receiver = $1 AND sender = ANY($2)",
            wa,
            cex_list,
        )
        senders_b = await conn.fetch(
            "SELECT DISTINCT sender FROM transactions WHERE receiver = $1 AND sender = ANY($2)",
            wb,
            cex_list,
        )
        set_a = {r["sender"] for r in senders_a}
        set_b = {r["sender"] for r in senders_b}
        if set_a & set_b:
            out.append("COMMON_KYC_SOURCE")

    # 3. SAME_COUNTERPARTIES (common senders/receivers >= 3)
    cp_a_send = await conn.fetch(
        """
        SELECT DISTINCT receiver AS cp FROM transactions WHERE sender = $1
        UNION SELECT DISTINCT sender AS cp FROM transactions WHERE receiver = $1
        """,
        wa,
    )
    cp_b_send = await conn.fetch(
        """
        SELECT DISTINCT receiver AS cp FROM transactions WHERE sender = $1
        UNION SELECT DISTINCT sender AS cp FROM transactions WHERE receiver = $1
        """,
        wb,
    )
    cp_a = {r.get("cp") for r in cp_a_send if r.get("cp") and r.get("cp") not in (wa, wb)}
    cp_b = {r.get("cp") for r in cp_b_send if r.get("cp") and r.get("cp") not in (wa, wb)}
    if len(cp_a & cp_b) >= 3:
        out.append("SAME_COUNTERPARTIES")

    # 4. DISTRIBUTION_PATTERN (wallet_a sends to many unique receivers)
    unique_recv = await conn.fetchval(
        "SELECT COUNT(DISTINCT receiver) FROM transactions WHERE sender = $1",
        wa,
    )
    if (unique_recv or 0) > DISTRIBUTION_THRESHOLD:
        out.append("DISTRIBUTION_PATTERN")

    # 5. SAME_DEX_PAIR_SESSION (same program_id within 24h window — simplified: same program usage)
    try:
        programs_a = await conn.fetch(
            """
            SELECT DISTINCT program_id FROM transactions
            WHERE (sender = $1 OR receiver = $1) AND program_id IS NOT NULL AND program_id != ''
            """,
            wa,
        )
        programs_b = await conn.fetch(
            """
            SELECT DISTINCT program_id FROM transactions
            WHERE (sender = $2 OR receiver = $2) AND program_id IS NOT NULL AND program_id != ''
            """,
            wb,
        )
        pa = {str(r.get("program_id") or "")[:8] for r in programs_a}
        pb = {str(r.get("program_id") or "")[:8] for r in programs_b}
        for p in DEX_PROGRAM_PREFIXES:
            if any(x.startswith(p) for x in pa) and any(x.startswith(p) for x in pb):
                out.append("SAME_DEX_PAIR_SESSION")
                break
    except Exception:
        pass

    return out


def calculate_link_confidence(signals: list[str]) -> float:
    """Bayesian confidence (same math as bayesian_risk.update_scam_probability)."""
    prior = LINKING_PRIOR
    odds = prior / (1.0 - prior)
    for s in signals:
        odds *= LINKING_LIKELIHOODS.get(s, 1.0)
    confidence = odds / (1.0 + odds)
    return min(confidence, CONFIDENCE_CAP)


async def is_distribution_wallet(wallet: str, conn) -> bool:
    """True if wallet sends to many unique receivers (business/distributor)."""
    try:
        n = await conn.fetchval(
            "SELECT COUNT(DISTINCT receiver) FROM transactions WHERE sender = $1",
            (wallet or "").strip(),
        )
        return (n or 0) > DISTRIBUTION_THRESHOLD
    except Exception:
        return False


async def run_linking_scan(wallet_a: str, conn) -> list[dict[str, Any]]:
    """Scan for potential linked wallets; return suggestions with confidence >= threshold."""
    wa = (wallet_a or "").strip()
    if not wa:
        return []

    if await is_distribution_wallet(wa, conn):
        return []

    candidates: set[str] = set()
    try:
        has_tx = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'transactions'"
        )
        if not has_tx:
            return []
    except Exception:
        return []

    # Direct transfer partners
    rows = await conn.fetch(
        """
        SELECT receiver AS w FROM transactions WHERE sender = $1
        UNION
        SELECT sender AS w FROM transactions WHERE receiver = $1
        """,
        wa,
    )
    for r in rows:
        w = r.get("w")
        if w and w != wa and len(w) >= 32:
            candidates.add(w)

    # Wallets that share a common CEX source with wallet_a
    cex_list = list(KNOWN_CEX_ADDRESSES.keys())
    if cex_list:
        cex_senders_to_a = await conn.fetch(
            "SELECT DISTINCT sender FROM transactions WHERE receiver = $1 AND sender = ANY($2)",
            wa,
            cex_list,
        )
        cex_addresses_used_by_a = {r["sender"] for r in cex_senders_to_a}
        if cex_addresses_used_by_a:
            others = await conn.fetch(
                "SELECT DISTINCT receiver FROM transactions WHERE sender = ANY($1) AND receiver != $2",
                list(cex_addresses_used_by_a),
                wa,
            )
            for r in others:
                w = r.get("receiver")
                if w and len(w) >= 32:
                    candidates.add(w)

    suggestions: list[dict[str, Any]] = []
    for wb in candidates:
        signals = await detect_signals(wa, wb, conn)
        if "DISTRIBUTION_PATTERN" in signals and "DIRECT_TRANSFER" not in signals:
            continue
        confidence = calculate_link_confidence(signals)
        if confidence >= CONFIDENCE_THRESHOLD_SUGGEST:
            tier = "STRONG" if confidence >= CONFIDENCE_THRESHOLD_STRONG else "MEDIUM"
            suggestions.append({
                "wallet": wb,
                "confidence": round(confidence, 2),
                "signals": signals,
                "tier": tier,
            })
            # Persist signals for audit
            for sig in signals:
                try:
                    await conn.execute(
                        """
                        INSERT INTO wallet_link_signals (wallet_a, wallet_b, signal_type, signal_strength)
                        VALUES ($1, $2, $3, $4)
                        """,
                        wa,
                        wb,
                        sig,
                        LINKING_LIKELIHOODS.get(sig, 1.0),
                    )
                except Exception:
                    pass

    return suggestions


async def save_suggestions(
    owner_wallet: str,
    suggestions: list[dict],
    handle: str | None,
    conn,
) -> int:
    """Save suggestions to wallet_link_suggestions; set expires_at = now + 30 days. Return count saved."""
    if not suggestions:
        return 0
    now = datetime.utcnow()
    expires_at = now + timedelta(days=SUGGESTION_EXPIRY_DAYS)
    saved = 0
    for s in suggestions:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO wallet_link_suggestions
                (owner_wallet, suggested_wallet, confidence, signals, status, handle, expires_at)
                VALUES ($1, $2, $3, $4, 'PENDING', $5, $6)
                ON CONFLICT (owner_wallet, suggested_wallet) DO NOTHING
                RETURNING id
                """,
                owner_wallet,
                s["wallet"],
                s["confidence"],
                s.get("signals") or [],
                handle,
                expires_at,
            )
            if row:
                saved += 1
        except Exception:
            pass
    return saved


# ---------------------------------------------------------------------------
# Linking boost for trust score (Phase 3.1)
# ---------------------------------------------------------------------------

LINKING_BOOST_CAP_POSITIVE = 8
LINKING_BOOST_CAP_NEGATIVE = -40
AGE_LONG_CODES = {"AGE_3Y", "AGE_5Y", "AGE_7Y", "AGE_10Y"}
WHALE_CODES = {"WHALE_100_SOL", "WHALE_1K_SOL", "WHALE_5K_SOL", "WHALE_10K_SOL", "WHALE_50K_SOL"}
SCAM_CODES_PREFIX = "SCAM"


async def calculate_linking_boost(
    owner_wallet: str,
    conn,
) -> tuple[float, list[str]]:
    """
    Calculate trust score boost/penalty from verified linked wallets.
    Returns (boost: float, reason_codes: list[str]). Boost capped between -40 and +15.
    """
    try:
        owner_wallet = (owner_wallet or "").strip()
        if not owner_wallet:
            return (0.0, [])

        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            owner_wallet,
        )
        if not handle_row or not handle_row.get("handle"):
            return (0.0, [])

        handle = handle_row["handle"]
        links = await conn.fetch(
            """
            SELECT wallet, ai_confidence FROM handle_wallet_links
            WHERE handle = $1 AND link_status = 'VERIFIED'
            """,
            handle,
        )
        if not links:
            return (0.0, [])

        total_boost = 0.0
        has_whale = False
        has_long_history = False
        has_clean_history = False
        has_high_risk = False
        has_scam = False
        has_sanctioned = False

        for r in links:
            linked_wallet = (r.get("wallet") or "").strip()
            confidence = float(r.get("ai_confidence") or 0.5)
            if not linked_wallet:
                continue

            wallet_boost = 4.0  # VERIFIED_WALLET_LINK base

            ts_row = await conn.fetchrow(
                "SELECT score, risk_level FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1",
                linked_wallet,
            )
            reasons_row = await conn.fetch(
                "SELECT reason_code FROM wallet_reasons WHERE wallet = $1",
                linked_wallet,
            )
            reasons = {row.get("reason_code") for row in reasons_row if row.get("reason_code")}

            if "CLEAN_HISTORY" in reasons:
                wallet_boost += 8
                has_clean_history = True
            if AGE_LONG_CODES & reasons:
                wallet_boost += 5
                has_long_history = True
            if WHALE_CODES & reasons:
                wallet_boost += 5
                has_whale = True

            risk_level = (ts_row.get("risk_level") or "").upper() if ts_row else ""
            if risk_level == "HIGH":
                wallet_boost -= 20
                has_high_risk = True
            if "DAEMON_SANCTIONED" in reasons or "LINKED_SANCTIONED" in reasons:
                wallet_boost -= 40
                has_sanctioned = True
            if any(c.startswith(SCAM_CODES_PREFIX) for c in reasons):
                wallet_boost -= 30
                has_scam = True

            total_boost += wallet_boost * confidence

        if len(links) >= 2:
            total_boost += 7

        total_boost = max(LINKING_BOOST_CAP_NEGATIVE, min(LINKING_BOOST_CAP_POSITIVE, total_boost))

        reason_codes: list[str] = []
        if total_boost > 0:
            reason_codes.append("VERIFIED_WALLET_LINK")
            if len(links) >= 2:
                reason_codes.append("MULTI_WALLET_IDENTITY")
            if has_whale:
                reason_codes.append("LINKED_WHALE")
            if has_long_history:
                reason_codes.append("LINKED_LONG_HISTORY")
            if has_clean_history:
                reason_codes.append("LINKED_CLEAN_HISTORY")
        if total_boost < 0:
            if has_high_risk:
                reason_codes.append("LINKED_HIGH_RISK")
            if has_scam:
                reason_codes.append("LINKED_SCAM_HISTORY")
            if has_sanctioned:
                reason_codes.append("LINKED_SANCTIONED")

        return (round(total_boost, 2), reason_codes)
    except Exception:
        return (0.0, [])
