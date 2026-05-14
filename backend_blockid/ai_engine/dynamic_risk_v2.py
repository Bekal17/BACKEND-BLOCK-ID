from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ai_engine.priority_wallets import update_priority
from backend_blockid.utils.risk import score_to_risk
from backend_blockid.database.score_history import log_score_change

logger = get_logger(__name__)

DYNAMIC_RISK_THRESHOLD = 70


async def _get_ml_score(conn: Any, wallet: str) -> float:
    """
    New wallet (no prior): return 50.0 as neutral base.
    Established wallet (prior >= 20): return prior as ml_score
    so EMA = (0.6 * prior) + (0.4 * prior) = prior → STABLE.
    Score only moves from graph_penalty, activity_boost, reason_penalty.
    """
    row = await conn.fetchrow(
        "SELECT dynamic_risk, final_score FROM trust_scores WHERE wallet = $1 LIMIT 1",
        wallet,
    )
    if not row:
        return 50.0

    prior = row["dynamic_risk"] if row["dynamic_risk"] is not None else None
    if prior is None:
        prior = row["final_score"] if row["final_score"] is not None else None

    if prior is not None and float(prior) >= 20.0:
        return float(prior)

    return 50.0


async def _get_prior_risk(conn: Any, wallet: str) -> float:
    row = await conn.fetchrow(
        "SELECT dynamic_risk, final_score, score FROM trust_scores WHERE wallet = $1 LIMIT 1",
        wallet,
    )
    if not row:
        return 0.0

    dynamic_risk = row["dynamic_risk"]
    final_score = row["final_score"]
    score = row["score"]

    # If dynamic_risk exists and is reasonable (>= 20), use it as prior
    if dynamic_risk is not None and float(dynamic_risk) >= 20.0:
        return float(dynamic_risk)

    # Fallback: use final_score if available and reasonable
    if final_score is not None and float(final_score) >= 20.0:
        return float(final_score)

    # Last fallback: use score column
    if score is not None and float(score) >= 20.0:
        return float(score)

    # If all are too low or None, return 0 (new wallet behavior)
    return 0.0


async def _get_reason_penalty(conn: Any, wallet: str) -> float:
    """
    Only count reasons that were added AFTER the last score update.
    This prevents the same reasons from compounding on every recalculate.
    Already-existing reasons are already baked into the prior dynamic_risk.
    """
    # Get last_updated timestamp from trust_scores
    ts_row = await conn.fetchrow(
        "SELECT last_updated FROM trust_scores WHERE wallet = $1 LIMIT 1",
        wallet,
    )
    last_updated = 0
    if ts_row and ts_row["last_updated"] is not None:
        last_updated = int(ts_row["last_updated"])

    # Only fetch reasons created AFTER last score update
    rows = await conn.fetch(
        "SELECT weight, confidence_score FROM wallet_reasons WHERE wallet = $1 AND created_at > $2",
        wallet,
        last_updated,
    )
    if not rows:
        return 0.0
    total = 0.0
    for row in rows:
        w = float(row["weight"] or 0)
        c = float(row["confidence_score"] if row["confidence_score"] is not None else 1.0)
        total += w * c
    # Cap: max +20 boost, max -30 penalty per recalculate
    return max(-30.0, min(20.0, total))


async def _get_neighbors(wallet: str, max_hop: int = 2) -> dict[str, int]:
    """
    BFS neighbors up to max_hop using transactions table (sender -> receiver).
    Returns {wallet: hop_distance}.
    """
    neighbors: dict[str, int] = {}
    visited = {wallet}
    frontier = {wallet}
    conn = await get_conn()
    try:
        for hop in range(1, max_hop + 1):
            if not frontier:
                break
            next_frontier: set[str] = set()
            for w in frontier:
                rows = await conn.fetch(
                    """
                    SELECT sender, receiver
                    FROM transactions
                    WHERE sender = $1 OR receiver = $1
                    """,
                    w,
                )
                for row in rows:
                    sender = (row["sender"] if row else "") or ""
                    receiver = (row["receiver"] if row else "") or ""
                    for candidate in (sender, receiver):
                        candidate = str(candidate).strip()
                        if not candidate or candidate == wallet or candidate in visited:
                            continue
                        next_frontier.add(candidate)
            for n in next_frontier:
                neighbors[n] = hop
            visited |= next_frontier
            frontier = next_frontier
    finally:
        await release_conn(conn)
    return neighbors


async def _has_scam_neighbor(wallet: str) -> tuple[bool, bool]:
    neighbors = await _get_neighbors(wallet, max_hop=2)
    if not neighbors:
        return False, False
    conn = await get_conn()
    scam_set: set[str] = set()
    try:
        rows = await conn.fetch("SELECT wallet FROM scam_wallets")
        scam_set = {str(r["wallet"]).strip() for r in rows if r and r.get("wallet")}
    finally:
        await release_conn(conn)
    hop1 = any(w in scam_set and hop == 1 for w, hop in neighbors.items())
    hop2 = any(w in scam_set and hop == 2 for w, hop in neighbors.items())
    return hop1, hop2


async def _get_last_tx_time_and_count(conn: Any, wallet: str) -> tuple[int, int]:
    row = await conn.fetchrow(
        """
        SELECT MAX(timestamp) as max_ts FROM transactions
        WHERE sender = $1 OR receiver = $1
        """,
        wallet,
    )
    last_tx_time = int(row["max_ts"] if row and row["max_ts"] is not None else 0)

    cutoff = int(time.time()) - 86400
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) as cnt FROM transactions
        WHERE (sender = $1 OR receiver = $1)
          AND timestamp > $2
        """,
        wallet, cutoff,
    )
    tx_count = int(row["cnt"] if row and row["cnt"] is not None else 0)
    return last_tx_time, tx_count


async def _get_wallet_establishment_data(conn: Any, wallet: str) -> dict[str, Any]:
    """
    Returns wallet establishment metrics for cap scoring.
    Cap activates if 2 or more of these 3 conditions are unmet:
      - wallet_age_days >= 60
      - tx_count_total >= 30
      - usd_outgoing_total >= 1000
    """
    age_row = await conn.fetchrow(
        "SELECT wallet_age_days FROM trust_scores WHERE wallet = $1",
        wallet,
    )
    wallet_age_days = int(age_row["wallet_age_days"] or 0) if age_row and age_row["wallet_age_days"] else 0

    tx_row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) as tx_count,
            COALESCE(SUM(CASE WHEN usd_amount IS NOT NULL THEN usd_amount ELSE 0 END), 0) as usd_outgoing
        FROM transactions
        WHERE sender = $1
        """,
        wallet,
    )
    tx_count = int(tx_row["tx_count"] or 0) if tx_row else 0
    usd_outgoing = float(tx_row["usd_outgoing"] or 0.0) if tx_row else 0.0

    age_ok = wallet_age_days >= 60
    tx_ok = tx_count >= 30
    usd_ok = usd_outgoing >= 1000.0

    conditions_met = sum([age_ok, tx_ok, usd_ok])
    cap_active = conditions_met < 2

    return {
        "wallet_age_days": wallet_age_days,
        "tx_count_total": tx_count,
        "usd_outgoing_total": usd_outgoing,
        "age_ok": age_ok,
        "tx_ok": tx_ok,
        "usd_ok": usd_ok,
        "cap_active": cap_active,
        "conditions_met": conditions_met,
    }


async def _get_cyclops_penalty(conn: Any, wallet: str) -> float:
    """
    Returns penalty based on Cyclops/Daemon risk score from wallet_meta.
    Formula:
      base_penalty = -(cyclops_risk_score / 100) * 40   -> max -40
      sanction_bonus = -20 if cyclops_is_sanctioned else 0
      total = max(-50, base + sanction_bonus)

    Normal wallet (score ~10): penalty ~-4 (minimal)
    High risk wallet (score 80): penalty -32
    Sanctioned + score 100: penalty -50 (capped)
    """
    row = await conn.fetchrow(
        """
        SELECT cyclops_risk_score, cyclops_is_sanctioned
        FROM wallet_meta WHERE wallet = $1
        """,
        wallet,
    )
    if not row or row["cyclops_risk_score"] is None:
        return 0.0

    cyclops_score = float(row["cyclops_risk_score"] or 0)
    is_sanctioned = bool(row["cyclops_is_sanctioned"] or False)

    base_penalty = -(cyclops_score / 100.0) * 40.0
    sanction_bonus = -20.0 if is_sanctioned else 0.0
    total = max(-50.0, base_penalty + sanction_bonus)

    return total


async def _get_wallet_age_boost(conn: Any, wallet: str) -> float:
    """
    Returns a score boost based on wallet age.
    Older wallets are more likely to be legitimate.

    Boost scale:
      >= 365 days (1+ year)  → +20
      >= 180 days (6+ months) → +12
      >= 90 days (3+ months)  → +6
      < 90 days               → 0

    Does NOT apply if wallet is already flagged as high risk
    (cyclops sanctioned or in scam_wallets blacklist).
    """
    try:
        row = await conn.fetchrow(
            "SELECT wallet_age_days FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        if not row or row["wallet_age_days"] is None:
            return 0.0

        age_days = int(row["wallet_age_days"] or 0)

        if age_days >= 365:
            return 20.0
        elif age_days >= 180:
            return 12.0
        elif age_days >= 90:
            return 6.0
        else:
            return 0.0
    except Exception as e:
        logger.warning("wallet_age_boost_error", wallet=wallet[:16], error=str(e))
        return 0.0


async def _fetch_from_helius(conn: Any, wallet: str, now: int) -> dict:
    """
    Fetch tx_count and unique_counterparties from Helius RPC.
    Replace this function body with Birdeye API call when Premium is available.
    """
    import os

    import aiohttp

    HELIUS_KEY = os.environ.get("HELIUS_API_KEY", "")
    if not HELIUS_KEY:
        return {"tx_count": 0, "unique_counterparties": 0}

    tx_count = 0
    unique_counterparties: set[str] = set()
    sigs: list = []

    try:
        async with aiohttp.ClientSession() as session:
            # Get signatures (up to 100)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    wallet,
                    {"limit": 100},
                ],
            }
            async with session.post(
                f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    sigs = data.get("result") or []
                    if not isinstance(sigs, list):
                        sigs = []
                    tx_count = len(sigs)

            # Get unique counterparties from last 20 transactions
            if tx_count > 0:
                sigs_to_check = sigs[:20]
                for sig_info in sigs_to_check:
                    sig = ""
                    if isinstance(sig_info, dict):
                        sig = sig_info.get("signature", "") or ""
                    if not sig:
                        continue
                    try:
                        tx_payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "getTransaction",
                            "params": [
                                sig,
                                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
                            ],
                        }
                        async with session.post(
                            f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}",
                            json=tx_payload,
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as tx_resp:
                            if tx_resp.status == 200:
                                tx_data = await tx_resp.json()
                                tx = tx_data.get("result") or {}
                                account_keys = (
                                    (tx.get("transaction") or {})
                                    .get("message") or {}
                                ).get("accountKeys") or []
                                for ak in account_keys:
                                    addr = ak if isinstance(ak, str) else ak.get("pubkey", "")
                                    if addr and addr != wallet:
                                        unique_counterparties.add(str(addr))
                    except Exception:
                        continue

    except Exception as e:
        logger.warning(
            "helius_behavior_fetch_error",
            wallet=wallet[:16],
            error=str(e),
        )
        return {"tx_count": 0, "unique_counterparties": 0}

    result = {
        "tx_count": tx_count,
        "unique_counterparties": len(unique_counterparties),
    }

    # Cache to DB
    try:
        await conn.execute(
            """UPDATE trust_scores
               SET tx_count_helius = $2,
                   unique_counterparties_helius = $3,
                   behavior_fetched_at = $4
               WHERE wallet = $1""",
            wallet,
            tx_count,
            len(unique_counterparties),
            now,
        )
        logger.info(
            "wallet_behavior_cached",
            wallet=wallet[:16],
            tx_count=tx_count,
            unique_counterparties=len(unique_counterparties),
        )
    except Exception as e:
        logger.warning(
            "wallet_behavior_cache_error",
            wallet=wallet[:16],
            error=str(e),
        )

    return result


async def _fetch_wallet_behavior_features(conn: Any, wallet: str) -> dict:
    """
    Fetch wallet behavior features: tx_count and unique_counterparties.
    Currently uses Helius getSignaturesForAddress.
    Swap to Birdeye Premium when available by replacing _fetch_from_helius
    with _fetch_from_birdeye (same return schema).

    Results cached in trust_scores.tx_count_helius and
    unique_counterparties_helius with 7-day TTL.
    """
    import time as time_module

    CACHE_TTL = 7 * 86400  # 7 days
    now = int(time_module.time())

    # Check cache first
    try:
        row = await conn.fetchrow(
            """SELECT tx_count_helius, unique_counterparties_helius, behavior_fetched_at
               FROM trust_scores WHERE wallet = $1""",
            wallet,
        )
        if (
            row
            and row["tx_count_helius"] is not None
            and row["behavior_fetched_at"] is not None
            and (now - int(row["behavior_fetched_at"])) < CACHE_TTL
        ):
            return {
                "tx_count": int(row["tx_count_helius"]),
                "unique_counterparties": int(row["unique_counterparties_helius"] or 0),
            }
    except Exception:
        pass

    # Fetch from Helius (swap to Birdeye when Premium available)
    return await _fetch_from_helius(conn, wallet, now)


async def _get_wallet_behavior_score(conn: Any, wallet: str) -> float:
    """
    Uses wallet_classifier.pkl to score wallet behavior.
    Features: tx_count, account_age_days, unique_counterparties
    Returns adjustment: positive for legitimate behavior, negative for suspicious.
    """
    import joblib
    from pathlib import Path

    import numpy as np

    # Repo root: backend_blockid/ai_engine -> parents[2]
    MODEL_PATH = Path(__file__).resolve().parents[2] / "models" / "wallet_classifier.pkl"
    if not MODEL_PATH.exists():
        return 0.0

    try:
        # Get features
        behavior = await _fetch_wallet_behavior_features(conn, wallet)
        tx_count = float(behavior["tx_count"])
        unique_counterparties = float(behavior["unique_counterparties"])

        # Get wallet age from DB
        row = await conn.fetchrow(
            "SELECT wallet_age_days FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        account_age_days = float(row["wallet_age_days"] or 0) if row else 0.0

        # Predict
        model = joblib.load(MODEL_PATH)
        X = np.array([[tx_count, account_age_days, unique_counterparties]])
        prob_good = float(model.predict_proba(X)[0, 0])  # probability of "good"

        # Convert to score adjustment: -10 to +10
        # prob_good = 1.0 → +10 (very legitimate)
        # prob_good = 0.5 → 0 (neutral)
        # prob_good = 0.0 → -10 (very suspicious)
        adjustment = (prob_good - 0.5) * 20.0

        logger.info(
            "wallet_behavior_score",
            wallet=wallet[:16],
            tx_count=int(tx_count),
            account_age_days=int(account_age_days),
            unique_counterparties=int(unique_counterparties),
            prob_good=round(prob_good, 3),
            adjustment=round(adjustment, 2),
        )
        return float(adjustment)

    except Exception as e:
        logger.warning(
            "wallet_behavior_score_error",
            wallet=wallet[:16],
            error=str(e),
        )
        return 0.0


async def _get_wallet_token_holdings_helius(wallet: str) -> list[str]:
    """
    Fetch current token holdings for a wallet from Helius DAS API.
    Returns list of token mint addresses currently held by wallet.
    Uses getAssetsByOwner — same API already used in predict_wallet_score.py.
    """
    import os
    import aiohttp

    HELIUS_KEY = os.environ.get("HELIUS_API_KEY", "")
    if not HELIUS_KEY:
        return []

    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAssetsByOwner",
                "params": {
                    "ownerAddress": wallet,
                    "page": 1,
                    "limit": 50,
                    "displayOptions": {
                        "showFungible": True,
                        "showNativeBalance": False,
                    },
                },
            }
            async with session.post(
                f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                items = (data.get("result") or {}).get("items") or []

                # Extract mint addresses from fungible tokens only
                mints = []
                for item in items:
                    # Get mint address from id field (Helius DAS uses id as mint)
                    mint = item.get("id", "").strip()
                    token_info = item.get("token_info") or {}

                    # Only include fungible tokens with some balance
                    balance = float(token_info.get("balance") or 0)
                    if mint and balance > 0:
                        mints.append(mint)

                return mints[:20]  # limit to 20 tokens

    except Exception as e:
        logger.warning("helius_token_holdings_error",
                      wallet=wallet[:16], error=str(e))
        return []


async def _get_token_risk_penalty(conn: Any, wallet: str) -> float:
    """
    Checks tokens held/sent by wallet against Rugcheck and Birdeye.
    Returns penalty based on percentage of risky tokens.
    Results cached 24h per token to avoid excessive API calls.

    Penalty scale:
      > 50% tokens high risk (score_normalised >= 80) → -20
      > 30% tokens high risk → -15
      > 50% tokens suspicious (score_normalised >= 50) → -10
      otherwise → 0
    """
    import aiohttp
    import time as time_module

    CACHE_TTL = 86400  # 24 hours
    BIRDEYE_KEY = os.environ.get("BIRDEYE_API_KEY", "")
    now = int(time_module.time())

    # Try Birdeye wallet token list first (Premium tier required)
    # When Premium is active, this solves cold start problem for historical wallets
    # ACTIVATION: remove the "if False" condition below when Premium is confirmed
    birdeye_token_mints = []
    if False:  # Change to: if True  — when Premium access is confirmed
        birdeye_token_mints = await _get_wallet_token_holdings_birdeye(wallet)

    if birdeye_token_mints:
        token_mints = birdeye_token_mints
        logger.info("token_risk_using_birdeye_holdings",
                   wallet=wallet[:16], count=len(token_mints))
    else:
        # Try Helius DAS API to get current token holdings
        helius_token_mints = await _get_wallet_token_holdings_helius(wallet)

        if helius_token_mints:
            token_mints = helius_token_mints
            logger.info("token_risk_using_helius_holdings",
                       wallet=wallet[:16], count=len(token_mints))
        else:
            # Final fallback: transactions table
            rows = await conn.fetch(
                """
                SELECT DISTINCT token_mint
                FROM transactions
                WHERE sender = $1
                  AND token_mint IS NOT NULL
                  AND token_mint != ''
                LIMIT 20
                """,
                wallet,
            )
            if not rows:
                return 0.0
            token_mints = [r["token_mint"] for r in rows]
    total = len(token_mints)
    high_risk_count = 0
    suspicious_count = 0

    async with aiohttp.ClientSession() as session:
        for mint in token_mints:
            try:
                # Check cache first
                cached = await conn.fetchrow(
                    "SELECT rugcheck_score_normalised, is_high_risk, risk_label, cached_at FROM token_risk_cache WHERE token_mint = $1",
                    mint,
                )
                if cached and (now - int(cached["cached_at"] or 0)) < CACHE_TTL:
                    # Use cached result
                    score_norm = int(cached["rugcheck_score_normalised"] or 0)
                else:
                    # Fetch from Rugcheck
                    rugcheck_score = 0
                    score_norm = 0
                    try:
                        async with session.get(
                            f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary",
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                rugcheck_score = int(data.get("score", 0) or 0)
                                score_norm = int(data.get("score_normalised", 0) or 0)
                    except Exception:
                        pass

                    # Fetch from Birdeye token_overview
                    birdeye_liquidity = 0.0
                    birdeye_holder = 0
                    birdeye_price_change_24h = 0.0
                    birdeye_buy_24h = 0
                    birdeye_sell_24h = 0
                    birdeye_trade_24h = 0
                    birdeye_unique_wallet_24h = 0
                    if BIRDEYE_KEY:
                        try:
                            async with session.get(
                                f"https://public-api.birdeye.so/defi/token_overview?address={mint}",
                                headers={
                                    "X-API-KEY": BIRDEYE_KEY,
                                    "x-chain": "solana",
                                    "accept": "application/json",
                                },
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as resp:
                                if resp.status == 200:
                                    bdata = await resp.json()
                                    d = bdata.get("data") or {}
                                    birdeye_liquidity = float(d.get("liquidity") or 0)
                                    birdeye_holder = int(d.get("holder") or 0)
                                    birdeye_price_change_24h = float(d.get("priceChange24hPercent") or 0)
                                    birdeye_buy_24h = int(d.get("buy24h") or 0)
                                    birdeye_sell_24h = int(d.get("sell24h") or 0)
                                    birdeye_trade_24h = int(d.get("trade24h") or 0)
                                    birdeye_unique_wallet_24h = int(d.get("uniqueWallet24h") or 0)
                        except Exception:
                            pass

                    # Determine risk label
                    is_high_risk = score_norm >= 80

                    # Honeypot detection: people can buy but cannot sell
                    is_honeypot = (
                        birdeye_buy_24h > 50
                        and birdeye_sell_24h < 5
                        and birdeye_trade_24h > 50
                    )

                    # Wash trading detection: too many trades per unique wallet
                    wash_ratio = (
                        birdeye_trade_24h / birdeye_unique_wallet_24h
                        if birdeye_unique_wallet_24h > 0 else 0
                    )
                    is_wash_trading = wash_ratio > 20 and birdeye_trade_24h > 100

                    if is_honeypot:
                        risk_label = "honeypot"
                        is_high_risk = True
                    elif score_norm >= 80:
                        risk_label = "high_risk"
                    elif is_wash_trading:
                        risk_label = "wash_trading"
                        is_high_risk = True
                    elif score_norm >= 50:
                        risk_label = "suspicious"
                    elif birdeye_liquidity < 1000 and birdeye_price_change_24h < -80:
                        risk_label = "probable_rug"
                        is_high_risk = True
                    elif (birdeye_liquidity < 100000
                          and birdeye_price_change_24h < -50
                          and birdeye_holder > 0
                          and birdeye_holder < 5000):
                        risk_label = "probable_dump"
                        is_high_risk = True
                    else:
                        risk_label = "safe"

                    # Upsert cache
                    try:
                        await conn.execute(
                            """
                            INSERT INTO token_risk_cache (
                                token_mint, rugcheck_score, rugcheck_score_normalised,
                                birdeye_liquidity, birdeye_holder, birdeye_price_change_24h,
                                is_high_risk, risk_label, cached_at
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                            ON CONFLICT (token_mint) DO UPDATE SET
                                rugcheck_score = EXCLUDED.rugcheck_score,
                                rugcheck_score_normalised = EXCLUDED.rugcheck_score_normalised,
                                birdeye_liquidity = EXCLUDED.birdeye_liquidity,
                                birdeye_holder = EXCLUDED.birdeye_holder,
                                birdeye_price_change_24h = EXCLUDED.birdeye_price_change_24h,
                                is_high_risk = EXCLUDED.is_high_risk,
                                risk_label = EXCLUDED.risk_label,
                                cached_at = EXCLUDED.cached_at
                            """,
                            mint, rugcheck_score, score_norm,
                            birdeye_liquidity, birdeye_holder, birdeye_price_change_24h,
                            is_high_risk, risk_label, now,
                        )
                    except Exception:
                        pass

                # Count risks
                if score_norm >= 80:
                    high_risk_count += 1
                elif score_norm >= 50:
                    suspicious_count += 1

            except Exception as e:
                logger.warning("token_risk_check_error", mint=mint[:16], error=str(e))
                continue

    if total == 0:
        return 0.0

    high_risk_pct = high_risk_count / total
    suspicious_pct = suspicious_count / total

    # Conservative penalties: target users are likely victims not perpetrators
    # New user non-crypto lebih sering jadi korban token rug/dump/honeypot
    if high_risk_pct > 0.5:
        penalty = -10.0
    elif high_risk_pct > 0.3:
        penalty = -7.0
    elif suspicious_pct > 0.5:
        penalty = -5.0
    else:
        penalty = 0.0

    if penalty != 0.0:
        logger.info(
            "token_risk_penalty_applied",
            wallet=wallet[:16],
            total_tokens=total,
            high_risk_count=high_risk_count,
            suspicious_count=suspicious_count,
            penalty=penalty,
        )

    return penalty


async def _get_wallet_token_holdings_birdeye(wallet: str) -> list[str]:
    """
    Fetch current token holdings for a wallet from Birdeye /v1/wallet/token_list.
    Returns list of token mint addresses.

    Requires Birdeye Premium tier or above.
    When available, this replaces transactions table as source of token_mints,
    solving the cold start problem for historical wallets.

    Currently INACTIVE - uncomment the call in _get_token_risk_penalty()
    when Premium access is confirmed.
    """
    import aiohttp
    import os

    BIRDEYE_KEY = os.environ.get("BIRDEYE_API_KEY", "")
    if not BIRDEYE_KEY:
        return []

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://public-api.birdeye.so/v1/wallet/token_list?wallet={wallet}",
                headers={
                    "X-API-KEY": BIRDEYE_KEY,
                    "x-chain": "solana",
                    "accept": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = (data.get("data") or {}).get("items") or []
                    # Return list of token mint addresses, excluding SOL native
                    SOL_MINT = "So11111111111111111111111111111111111111112"
                    return [
                        item["address"]
                        for item in items
                        if item.get("address")
                        and item["address"] != SOL_MINT
                        and item.get("valueUsd", 0) > 0
                    ]
                else:
                    logger.warning(
                        "birdeye_wallet_token_list_error",
                        wallet=wallet[:16],
                        status=resp.status,
                    )
                    return []
    except Exception as e:
        logger.warning("birdeye_wallet_token_list_exception",
                      wallet=wallet[:16], error=str(e))
        return []


async def compute_dynamic_risk(wallet: str) -> dict[str, float]:
    conn = await get_conn()
    try:
        ml_score = await _get_ml_score(conn, wallet)
        prior = await _get_prior_risk(conn, wallet)
        last_tx_time, tx_count_24h = await _get_last_tx_time_and_count(conn, wallet)

        if prior == 0 and last_tx_time == 0:
            # New wallet with no transactions: neutral score, do not inherit ml_score
            updated = 50.0
            logger.info("dynamic_risk_new_wallet_neutral", wallet=wallet[:16])
        elif prior == 0:
            updated = ml_score
        else:
            updated = (0.6 * prior) + (0.4 * ml_score)

        logger.debug(
            "dynamic_risk_update",
            wallet=wallet[:16] + "...",
            ml_score=ml_score,
            prior=prior,
            updated=updated,
        )

        hop1, hop2 = await _has_scam_neighbor(wallet)
        graph_penalty = -30.0 if hop1 else (-15.0 if hop2 else 0.0)

        # Guard: wallets with no transactions should not be penalized
        if last_tx_time == 0:
            days_inactive = 0
        else:
            days_inactive = max(0, (int(time.time()) - last_tx_time) // 86400)

        logger.debug(
            "dynamic_risk_inactivity",
            wallet=wallet[:16] + "...",
            last_tx_time=last_tx_time,
            days_inactive=days_inactive,
        )

        # Only +1 score if wallet has 100+ transactions in last 24h
        # This prevents activity_boost from compounding on every recalculate
        activity_boost = 1.0 if tx_count_24h >= 100 else 0.0

        dynamic_risk = updated + graph_penalty + activity_boost
        dynamic_risk = max(0.0, min(100.0, dynamic_risk))

        return {
            "ml_score": float(ml_score),
            "prior": float(prior),
            "graph_penalty": float(graph_penalty),
            "decay": 0.0,
            "activity_boost": float(activity_boost),
            "dynamic_risk": float(dynamic_risk),
            "last_tx_time": float(last_tx_time),
            "tx_count_24h": float(tx_count_24h),
            "days_inactive": float(days_inactive),
        }
    finally:
        await release_conn(conn)


async def update_wallet_score_async(wallet: str) -> dict[str, float]:
    conn = await get_conn()
    now = int(time.time())
    try:
        details = await compute_dynamic_risk(wallet)
        ml_score = details["ml_score"]
        dynamic_risk = details["dynamic_risk"]
        reason_penalty = await _get_reason_penalty(conn, wallet)
        establishment = await _get_wallet_establishment_data(conn, wallet)
        cyclops_penalty = await _get_cyclops_penalty(conn, wallet)
        token_risk_penalty = await _get_token_risk_penalty(conn, wallet)
        age_boost = await _get_wallet_age_boost(conn, wallet)
        behavior_score = await _get_wallet_behavior_score(conn, wallet)
        final_score = (dynamic_risk + reason_penalty + cyclops_penalty + token_risk_penalty + age_boost + behavior_score)
        final_score = max(0.0, min(97.0, final_score))

        # Apply new wallet cap: if unestablished, score cannot exceed cap_value
        # If raw_ml_score < 50, use it as cap (ML already detected risk)
        # If raw_ml_score >= 50 or not available, use default cap of 50
        if establishment["cap_active"]:
            raw_ml = await conn.fetchval(
                "SELECT raw_ml_score FROM trust_scores WHERE wallet = $1",
                wallet,
            )
            if raw_ml is not None and float(raw_ml) < 50.0:
                cap_value = float(raw_ml)
            else:
                cap_value = 50.0
            # Allow age_boost to exceed cap — older wallets deserve higher score
            # even if ML score is low, because age is strong legitimacy signal
            effective_cap = min(cap_value + age_boost, 97.0)
            final_score = min(final_score, effective_cap)

        risk_level = score_to_risk(int(round(final_score)))

        # Fetch score_before for history (before any UPDATE)
        row_before = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        score_before = float(row_before["score"]) if row_before and row_before["score"] is not None else None

        # Log to history BEFORE updating trust_scores (non-fatal)
        logger.info(
            "score_history_hook_called",
            wallet=wallet[:16],
            score_before=score_before,
            score_after=float(final_score),
            final_score=float(final_score),
            age_boost=float(age_boost),
            behavior_score=float(behavior_score),
        )
        await log_score_change(
            wallet=wallet,
            score_before=score_before,
            score_after=float(final_score),
            change_category="BEHAVIORAL",
            triggered_by="realtime_pipeline",
            ml_score=float(ml_score),
            dynamic_risk=float(dynamic_risk),
            reason_penalty=float(reason_penalty),
            graph_penalty=float(details.get("graph_penalty", 0.0)),
            decay=0.0,
            activity_boost=float(details.get("activity_boost", 0.0)),
            risk_level=str(risk_level),
            metadata={
                "cyclops_penalty": float(cyclops_penalty),
                "cap_active": establishment["cap_active"],
                "conditions_met": establishment["conditions_met"],
            },
        )

        exists = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
        if exists:
            await conn.execute(
                """
                UPDATE trust_scores SET
                    score = $2,
                    risk_level = $3,
                    ml_score = $4,
                    dynamic_risk = $5,
                    final_score = $6,
                    last_updated = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE wallet = $1
                """,
                wallet,
                float(final_score),
                str(risk_level),
                float(ml_score),
                float(dynamic_risk),
                float(final_score),
                now,
            )
        else:
            await conn.execute(
                """
                INSERT INTO trust_scores (
                    wallet, score, risk_level, ml_score, dynamic_risk, final_score, last_updated, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
                """,
                wallet,
                float(final_score),
                str(risk_level),
                float(ml_score),
                float(dynamic_risk),
                float(final_score),
                now,
            )

        try:
            await conn.execute(
                """
                UPDATE trust_scores ts
                SET wallet_age_days = wm.wallet_age_days
                FROM wallet_meta wm
                WHERE ts.wallet = wm.wallet AND wm.wallet = $1 AND wm.wallet_age_days > 0
                """,
                wallet,
            )
        except Exception:
            pass  # wallet_meta may not exist

        if dynamic_risk > DYNAMIC_RISK_THRESHOLD:
            await update_priority(wallet, +20)
        if details["days_inactive"] >= 30:
            await update_priority(wallet, -10)

        details["final_score"] = float(final_score)
        details["reason_penalty"] = float(reason_penalty)
        details["cyclops_penalty"] = float(cyclops_penalty)
        details["token_risk_penalty"] = float(token_risk_penalty)
        details["age_boost"] = float(age_boost)
        details["behavior_score"] = float(behavior_score)
        details["cap_active"] = establishment["cap_active"]
        details["cap_conditions_met"] = establishment["conditions_met"]
        details["wallet_age_days"] = establishment["wallet_age_days"]
        details["tx_count_total"] = establishment["tx_count_total"]
        details["usd_outgoing_total"] = establishment["usd_outgoing_total"]
        details["risk_level"] = risk_level
        return details
    finally:
        await release_conn(conn)


def update_wallet_score(wallet: str) -> dict[str, float]:
    """Sync wrapper for update_wallet_score_async."""
    return asyncio.get_event_loop().run_until_complete(update_wallet_score_async(wallet))
