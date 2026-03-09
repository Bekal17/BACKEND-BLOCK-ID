"""
Bayesian Dynamic Risk Update for BlockID trust scoring.

Updates scam probability when new evidence arrives.
Odds-based: evidence multiplies scam odds.

Future upgrades:
* Token-specific likelihood
* Cluster-size likelihood
* Time-decay Bayesian update
* Online learning
"""
from __future__ import annotations

import asyncio
import json
import os
import time

LIKELIHOODS: dict[str, float] = {
    "SCAM_CLUSTER_MEMBER": 5.0,
    "SCAM_CLUSTER_MEMBER_SMALL": 3.0,
    "SCAM_CLUSTER_MEMBER_LARGE": 7.0,
    "DRAINER_INTERACTION": 8.0,
    "HIGH_VOLUME_TO_SCAM": 8.0,
    "DRAINER_FLOW": 10.0,
    "DRAINER_FLOW_DETECTED": 6.0,
    "HIGH_VALUE_OUTFLOW": 4.0,
    "BRIDGE_SCAM_INTERACTION": 6.0,
    "LOW_ACTIVITY": 1.2,
    "NEW_WALLET": 1.1,
    "CLEAN_HISTORY": 0.5,
    "NO_RISK_DETECTED": 0.3,
}

LIKELIHOOD_TABLE = dict(LIKELIHOODS)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
DEFAULT_PRIOR = 0.05


async def get_prior_async(wallet: str) -> float | None:
    """Load prior scam probability from wallet_risk_probabilities (latest posterior)."""
    try:
        from backend_blockid.database.pg_connection import get_conn, release_conn

        conn = await get_conn()
        try:
            row = await conn.fetchrow(
                "SELECT posterior FROM wallet_risk_probabilities WHERE wallet = $1 ORDER BY created_at DESC LIMIT 1",
                wallet,
            )
            if row and row["posterior"] is not None:
                return float(row["posterior"])
        finally:
            await release_conn(conn)
    except Exception:
        pass
    return None


def get_prior(wallet: str) -> float | None:
    """Sync wrapper for get_prior_async."""
    return asyncio.get_event_loop().run_until_complete(get_prior_async(wallet))


def update_scam_probability(
    prior: float | None,
    reason_codes: list[dict] | list[str],
) -> float:
    """
    Update scam probability given evidence (reason codes).
    Odds-based: odds = prior/(1-prior); odds *= LIKELIHOODS[code]; posterior = odds/(1+odds).

    reason_codes: list of dicts with "code" key, or list of code strings.
    """
    if prior is None and TEST_MODE:
        prior = DEFAULT_PRIOR
    prior = float(prior) if prior is not None else DEFAULT_PRIOR
    prior = max(0.001, min(0.999, prior))

    codes: list[str] = []
    for r in reason_codes:
        if isinstance(r, dict):
            c = (r.get("code") or r.get("reason_code") or "").strip()
        else:
            c = str(r).strip()
        if c:
            codes.append(c)

    # ---------------------------------------------------------
    # Guard: ignore Bayesian update for weak signals only
    # ---------------------------------------------------------
    WEAK_SIGNALS = {"LOW_ACTIVITY", "NEW_WALLET", "CLEAN_HISTORY", "NO_RISK_DETECTED"}
    if codes and all(c in WEAK_SIGNALS for c in codes):
        return prior if prior is not None else DEFAULT_PRIOR

    odds = prior / (1.0 - prior)
    for code in codes:
        mult = LIKELIHOODS.get(code, 1.0)
        odds *= mult
    posterior = odds / (1.0 + odds)
    return max(0.001, min(0.999, posterior))


def bayes_update_legacy(prior: float, likelihood: float) -> float:
    """Legacy: P(S|E) = (P(E|S)*P(S)) / (P(E|S)*P(S) + P(E|notS)*(1-P(S)))."""
    numerator = likelihood * prior
    denominator = numerator + (1 - likelihood) * (1 - prior)
    return numerator / denominator if denominator else prior


async def save_bayesian_history_async(
    wallet: str,
    prior: float,
    posterior: float,
    reason_codes: list[str],
) -> None:
    """Insert into wallet_history (or wallet_risk_probabilities). Adds prior, posterior if columns exist."""
    try:
        from backend_blockid.database.pg_connection import get_conn, release_conn

        conn = await get_conn()
        try:
            now = int(time.time())
            rc_json = json.dumps(reason_codes)
            
            try:
                await conn.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                            WHERE table_name='wallet_history' AND column_name='prior') THEN
                            ALTER TABLE wallet_history ADD COLUMN prior DOUBLE PRECISION;
                        END IF;
                    END $$;
                """)
            except Exception:
                pass
            try:
                await conn.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                            WHERE table_name='wallet_history' AND column_name='posterior') THEN
                            ALTER TABLE wallet_history ADD COLUMN posterior DOUBLE PRECISION;
                        END IF;
                    END $$;
                """)
            except Exception:
                pass

            cols_row = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name='wallet_history'"
            )
            cols = {r["column_name"] for r in cols_row}
            
            if "prior" in cols and "posterior" in cols:
                await conn.execute(
                    """
                    INSERT INTO wallet_history (wallet, prior, posterior, reason_codes, snapshot_at)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    wallet, prior, posterior, rc_json, now,
                )
            else:
                score = int((1.0 - posterior) * 100)
                await conn.execute(
                    """
                    INSERT INTO wallet_history (wallet, score, risk_level, reason_codes, snapshot_at)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    wallet, score, "1", rc_json, now,
                )
            print(f"[bayesian_update] wallet={wallet[:16]}... prior={prior:.2f} posterior={posterior:.2f}")
        finally:
            await release_conn(conn)
    except Exception:
        pass


def save_bayesian_history(
    wallet: str,
    prior: float,
    posterior: float,
    reason_codes: list[str],
) -> None:
    """Sync wrapper for save_bayesian_history_async."""
    asyncio.get_event_loop().run_until_complete(
        save_bayesian_history_async(wallet, prior, posterior, reason_codes)
    )


def update_and_save(
    wallet: str,
    prior: float | None,
    reason_codes: list[dict] | list[str],
) -> float:
    """
    Update scam probability, save to wallet_history, return posterior.
    TEST_MODE: prior defaults to 0.05 if no prior.
    """
    prior_val = get_prior(wallet) if prior is None else prior
    posterior = update_scam_probability(prior_val, reason_codes)
    codes = []
    for r in reason_codes:
        if isinstance(r, dict):
            c = (r.get("code") or r.get("reason_code") or "").strip()
        else:
            c = str(r).strip()
        if c:
            codes.append(c)
    save_bayesian_history(wallet, prior_val or DEFAULT_PRIOR, posterior, codes)
    return posterior
