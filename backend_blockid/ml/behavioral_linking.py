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
    # ── OKX ──────────────────────────────────────────────────────────────
    "is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV": "OKX",   # Hot Wallet 1 (verified)
    "2YxQCXt9spMwoQZiwFLwdjHtscVvXi4nmxCkQCD6Rvgg": "OKX",   # Hot Wallet 2
    "8wM44Ryv9DFCSfkgUnPEPgnsc53arT4cnmXL6LnnC4UW": "OKX",   # Hot Wallet 4
    "EkeyuVghbRfGWPyEtm3vnjKZPbUXXrPqxdar8fsEMuXd": "OKX",   # Hot Wallet 5
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "OKX",   # Legacy

    # ── Binance ───────────────────────────────────────────────────────────
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",  # Binance 2
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance",  # Binance 3
    "6QJzieMYfp7yr3EdrePaQoG3Ghxs2wM98xSLRu8Xh56U": "Binance",  # Binance 11
    "3gd3dqgtJ4jWfBfLYTX67DALFetjc5iS72sCgRhCkW2u": "Binance",  # Binance 10
    "53unSgGWqEWANcPYRF35B2Bgf8BkszUtcccKiXwGGLyr": "Binance",  # Binance.US
    "3ADzk5YDP9sgorvPSs9YPxigJiSqhgddpwHwwPwmEFib": "Binance",  # Deposit address
    "8FmGDmDDkHoFaT6SaXgmHRLBMCcJWwNXpFWJQJMAfmHo": "Binance", # Legacy 1
    "AC5RDfQFmDS1deWZos921JhjEKMDSBCDM8KFTBgq9aUF": "Binance",  # Legacy 2

    # ── Bybit ─────────────────────────────────────────────────────────────
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Bybit",  # Hot Wallet
    "iGdFcQoyR2MwbXMHQskhmNsqddZ6rinsipHc4TNSdwu":  "Bybit",  # Wallet 10
    "5LZkATrLwHYCQj2YuVbjjgsDZzBk6YfL4pFQRJmtboT2": "Bybit",  # Wallet 15
    "7cAui6ADtxLnpRr2wYvwJWTkzwgmVF2LYKnjKTLx4xR8": "Bybit",  # Wallet
    "CK8i4zFXkDE2KWfyg7g9S748r6mwxajbcKcyGhQMR3qQ": "Bybit",  # Wallet 13
    "9ZifroknFoYu4r6DUk6nYoJiUQnEyyoUyeAwjXbPoL2x": "Bybit",  # Wallet 11
    "7ReR6syi6gr7qUrKCL1FB9VFzGhVgHwLJ8wtfNtH9Mv4": "Bybit",  # Wallet 9
    "BunaYnktTigcU1ovzVt9dG7NMv2gW5VX7MBfSS8J38s2": "Bybit",  # Wallet 6
    "CSSJFgoeqidqVtHKSNP7i7s6WX8APHfH2kYGdLV195Jb": "Bybit",  # Deposit
    "CMivUnnbDHxLq9ChV1bSuiQE5ycZf6JVvFFDePMHhHYK": "Bybit",  # Deposit
    "2qo8jvuc49pFmTjmUHLiARSV6ppPTaE7gw27ZJ6DnNZy": "Bybit",  # Wallet 12

    # ── Coinbase ──────────────────────────────────────────────────────────
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",  # Coinbase 1
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm": "Coinbase",  # Coinbase 2
    "9obNtb5GyUegcs3a1CbBkLuc5hEWynWfJC6gjz5uWQkE": "Coinbase",  # Coinbase 4
    "FpwQQhQQoEaVu3WU2qZMfF1hx48YyfwsLoRgXG83E99Q": "Coinbase",  # Hot Wallet 1
    "D89hHJT5Aqyx1trP6EnGY9jJUB3whgnq3aUvvCqedvzf": "Coinbase",  # Hot Wallet 3
    "DPqsobysNf5iA9w7zrQM8HLzCKZEDMkZsWbiidsAt1xo": "Coinbase",  # Hot Wallet 4
    "3vxheE5C46XzK4XftziRhwAf8QAfipD7HXXWj25mgkom": "Coinbase",  # Prime
    "AYXGC7sBnX2LnG4rKPx6Ejod5pUr7vut8vokPwSy5uy6": "Coinbase",  # Prime Deposit
    "2x4fzH8qzXQw923imiuc1sobuds1vT2TfNeomndfBAuh": "Coinbase",  # Deposit
    "DSo8tY2KdbmZC6Q9YNWgQ9745ugL1Q9K6FeyqovkGqRS": "Coinbase",  # Deposit
    "BCES4xJZvugXafnrsbK7nahh6DELuMj72J4hUChyTmKt": "Coinbase",  # Deposit
    "7PbYL5iBwcsae6U9KDcuAryVeU8VLwuJuQR5bq5ZMwje": "Coinbase",  # Deposit
    "8HWUVzK1JWDNCvyJQcRotfg4UwPb5RbKrmCa4hxyK8gP": "Coinbase",  # Deposit
    "Fue5m7uwemAhv1uyPSC44hd6UpBgAkubxiFCvBGmc8Ah": "Coinbase",  # Deposit
    "Ddy2b4kf6iuXe5tBBPXrnDkoBQgWUVaucQjxMQrTFS3M": "Coinbase",  # Deposit
    "3mvRNcMyrM91GrNUQMuZ3EW7feqGbwJg3p8JJKTsFKvp": "Coinbase",  # Deposit
    "9u2SjcfTUnSaJ1UHteESQLDaAo7KSJeBT9wMyoTqTkuJ": "Coinbase",  # Deposit
    "3eCq5SX86LCQzbTEctJEBc4tJ4iYcR8T2VB3ruNhxJVZ": "Coinbase",  # Deposit
    "6fBLQE41AkujAm3N1MLEqV1cWKey1RxR5cG9btDaeMHp": "Coinbase",  # Deposit
    "E1YG99HeNiRwfB6D2qjBYUAeMjpNA88WKEPjrBc8Kvhh": "Coinbase",  # Deposit
    "JspwVtWvxBwgHL4iCt2WCwFqWMGPZwYNtP9o6XtgdDs":  "Coinbase",  # Deposit
    "5anqJQxX2YoY4Z3M3H7yuFTiD3KCEeS5fzxHnuEZw8Ba": "Coinbase",  # Deposit
    "51ig4tkZD1kCcnXLRuFre1nxyJg83aFTfQb5UGLPwMHy": "Coinbase",  # Deposit
    "GNPXBUapDex941kStjpQRzcfMh2SEPhNhz6irSAaAeRN": "Coinbase",  # Deposit
    "8HzyPVwoExwtZvhHC2ynpsw7xFiEkr5HgR9emPnb8RgT": "Coinbase",  # Deposit
    "89FSaQLPMUCHf4N9iQbf6XjeGvYkbPhE7kitdZKqU1an": "Coinbase",  # Deposit
    "FQpC71EDo4m6L44Y7xt5uTPGo8qpXuFKXjiCgJ6GjYtL": "Coinbase",  # Deposit

    # ── Kraken ────────────────────────────────────────────────────────────
    "6LY1JzAFVZsP2a2xKrtU6znQMQ5h4i7tocWdgrkZzkzF": "Kraken",  # Main
    "HzKVUmEAuaf8nV3tcJk2uZKohmLwtk1351ASCdqT5B8q": "Kraken",  # Hot Wallet 2
    "8u6UYLGA8vFeCkZDoLmN9xS2wCUYVxdSKSRhthDzWSsN": "Kraken",  # Deposit
    "FcH9X4WtgZeJ6dkDgBehfzoxAoDg8XgjpxNg8u9JZrTk": "Kraken",  # Deposit
    "8bfcAwx9dZjLGkHcMtuZwaDthuWaQ7bPFMoZeniCSMLS": "Kraken",  # Deposit
    "3xCgDJQ3PSQpoMFnijF9sQNV3J6onmQk7U3ZkxjigeAk": "Kraken",  # Deposit
    "BeAMHyvuBNgNhpaPmFhiQkTtFEBjUYnAQSmNKFkY1b4M": "Kraken",  # Legacy

    # ── KuCoin ────────────────────────────────────────────────────────────
    "HVh6wHNBAsG3pq1Bj5oCzRjoWKVogEDHwUHkRz3ekFgt": "KuCoin",  # KuCoin 3
    "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6": "KuCoin",  # KuCoin 2
    "CAxKWUpSbsNsWu2gEFjed64jrNxiNYfRMVEMahshHotb": "KuCoin",  # Deposit 5
    "7gQ1CfjdysJkSEVSDXNjJHnzrqvP2zQYQDJHJP67o1bb": "KuCoin",  # Deposit 3
    "AGVhmrhDi3RKLu9nxnRqp3CUpaG3SVeYXWkWcygHAk8N": "KuCoin",  # KuCoin 5
    "6BhBoBB47wSGjK5uzcGWNcTf2oNRPQNuv6GVdkNyj9PB": "KuCoin",  # KuCoin 4
    "BmFdpraQhkiDPE5PCEPhEQGNJgKdY18Mar2XSoQtBBxS": "KuCoin",  # Legacy

    # ── Upbit ─────────────────────────────────────────────────────────────
    "7mhcgF1DVsj5iv4CxZDgp51H6MBBwqamsH1KnqXhSRc5": "Upbit",   # Hot Wallet
    "555oNTKdRECgyLn8fBvySoN6hXMCszFq1Y4oea9p3ZFB": "Upbit",   # Hot Wallet 2

    # ── Gate.io ───────────────────────────────────────────────────────────
    "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w":  "Gate.io",  # Main
    "GLC5DctxxSNUqaN6pzkRfH5A7wS8kZvN8kbomwZq2J3B": "Gate.io",  # Deposit
    "DNxB8gtBbo73giAFs7GtbFX2cfmKJ4CCko6CbsiRbNbr": "Gate.io",  # Deposit

    # ── MEXC ──────────────────────────────────────────────────────────────
    "5PAhQiYdLBd6SVdjzBQDxUAEFyDdF5ExNPQfcscnPRj5": "MEXC",  # MEXC 2
    "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ": "MEXC",  # Main

    # ── Bitget ────────────────────────────────────────────────────────────
    "A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR": "Bitget",  # Exchange

    # ── Crypto.com ────────────────────────────────────────────────────────
    "22Wnk8PwyWZV7BfkZGJEKT9jGGdtvu7xY6EXeRh7zkBa": "Crypto.com",  # Hot Wallet 3
    "6FEVkH17P9y8Q9aCkDdPcMDjvj7SVxrTETaYEm8f51Jy": "Crypto.com",  # Hot Wallet 1

    # ── Robinhood ─────────────────────────────────────────────────────────
    "4xLpwxgYuPwPvtQjE94RLS4WZ4aD8NJYYKr2AJk99Qdg": "Robinhood",  # Hot Wallet

    # ── Backpack ──────────────────────────────────────────────────────────
    "43DbAvKxhXh1oSxkJSqGosNw3HpBnmsWiak6tB5wpecN": "Backpack",  # Exchange
    "J16ovD5x6kZLYDYAa6CqfrwacHdM7fcKD9iKG5EoNeGR": "Backpack",  # Deposit
    "6wspq3nz3qPQ9X6rbLM5bEDHK525yPSNqyqeABXcSMHQ": "Backpack",  # Deposit

    # ── Bithumb ───────────────────────────────────────────────────────────
    "8Mm46CsqxiyAputDUp2cXHg41HE3BfynTeMBDwzrMZQH": "Bithumb",  # Hot Wallet

    # ── Bitfinex ──────────────────────────────────────────────────────────
    "FxteHmLwG9nk1eL4pjNve3Eub2goGkkz6g6TbvdmW46a": "Bitfinex",  # Hot Wallet

    # ── Ceffu (Binance Custody) ───────────────────────────────────────────
    "5SDrsMNTYdhmApjfqYHDvjoW92f2S42vcc7zNDVcQ9Ej": "Binance",  # Ceffu Custody

    # ── SwissBorg ─────────────────────────────────────────────────────────
    "DMe3ddj7awSR3LFC64rjmCPexsrSv33QAxFoJux4vGH3": "SwissBorg",  # Hot Wallet 3

    # ── Ourbit ────────────────────────────────────────────────────────────
    "3pjwKq9yuzpVYfD4h5jMZLLfV8oSd8YiwpoAaB5oZS3H": "Ourbit",  # Hot Wallet 2

    # ── Bullish ───────────────────────────────────────────────────────────
    "Bc5bth4Mn2n8DX1etZwvnq3uDEGg479s2D24TCEsnXHf": "Bullish",  # Hot Wallet

    # ── FalconX ───────────────────────────────────────────────────────────
    "AgsYPSd9jQZEpbTMsvBWKdiAux3eyghdSY355QVHH9Hs": "FalconX",  # Hot Wallet
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
