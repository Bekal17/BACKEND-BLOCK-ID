"""
Predict trust/risk score for wallets using trained token_scam_model.joblib.

Merge ML predictions with reason penalties: final_score = ml_score - penalty (clamped 0–100).
Output: wallet_scores.csv with wallet, ml_score, penalty, final_score, risk_level.

Usage:
    py backend_blockid/ml/predict_wallet_score.py

Reads wallets from cluster_features.csv (valid Solana addresses only); loads reason_penalties.csv when present.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import time
from pathlib import Path

import joblib
import numpy as np

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.reason_codes import get_reason_weights
from backend_blockid.ai_engine.reason_summary import build_summary
from backend_blockid.utils.wallet_utils import is_valid_wallet
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.repositories import save_wallet_scores_from_csv, update_wallet_score, save_wallet_risk_probability
from backend_blockid.ml.bayesian_risk import (
    get_prior,
    save_bayesian_history,
    update_scam_probability,
    LIKELIHOOD_TABLE,
)
from backend_blockid.utils.risk import score_to_risk, risk_level_from_reasons
from backend_blockid.ml.dynamic_risk import compute_dynamic_penalty
from backend_blockid.ml.reputation_decay import apply_reputation_decay
from backend_blockid.ml.graph_risk import apply_graph_penalty
from backend_blockid.tools.review_queue_engine import check_for_review

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
print(f"[CONFIG] TEST_MODE = {TEST_MODE}")

HIGH_RISK_CODES = {
    "BLACKLISTED_CREATOR",
    "RUG_PULL_DEPLOYER",
    "DRAINER_FLOW_DETECTED",
    "DRAINER_FLOW",
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "MEGA_DRAINER",
    "HIGH_RISK_TOKEN_INTERACTION",
    "SUSPICIOUS_TOKEN_MINT",
}


def normalize_reason_weight(code: str, weight: int) -> int:
    """
    Ensure dangerous reasons always reduce trust score.
    """
    if code in HIGH_RISK_CODES:
        return -abs(weight)
    return weight


def safe_reasons(reasons):
    """Sanitize reasons list: filter out None, non-dict, or missing 'code'."""
    if not reasons:
        return []
    fixed = []
    for r in reasons:
        if isinstance(r, dict) and "code" in r:
            fixed.append(r)
    return fixed


def safe_num(x, default=0):
    """Coerce to float; return default on None or error."""
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


# Paths: script in backend_blockid/ml/, data and models in backend_blockid/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
_MODELS_DIR = _SCRIPT_DIR.parent / "models"
CLUSTER_FEATURES_CSV = _DATA_DIR / "cluster_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
REASON_PENALTIES_CSV = _DATA_DIR / "reason_penalties.csv"
REASON_CODES_CSV = _DATA_DIR / "reason_codes.csv"
WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"
FLOW_FEATURES_CSV = _DATA_DIR / "flow_features.csv"
GRAPH_CLUSTER_CSV = _DATA_DIR / "graph_cluster_features.csv"
MODEL_PATH = _MODELS_DIR / "token_scam_model.joblib"

# Feature order must match train_token_scam_model.py
FEATURE_ORDER = [
    "mint_authority_exists",
    "freeze_authority_exists",
    "metadata_missing",
    "decimals",
    "supply",
]

def _load_valid_wallets_from_cluster(path: Path) -> list[str]:
    """Load valid Solana wallet addresses from cluster_features.csv."""
    out: list[str] = []
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            w = r.get("wallet", "").strip()
            if w and is_valid_wallet(w):
                out.append(w)
    return out


def _load_scam_wallets(path: Path) -> set[str]:
    """Load scam wallet addresses from CSV (first column)."""
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w:
                out.add(w)
    return out


def _get_token_history_mock(wallet: str) -> list[dict]:
    """
    Mock token history for a wallet. Returns list of feature dicts per token.
    Replace with Helius (e.g. getAssetsByOwner or creator-based) later.
    """
    # One synthetic token per wallet: no red flags (all zeros)
    return [
        {
            "mint_authority_exists": 0,
            "freeze_authority_exists": 0,
            "metadata_missing": 0,
            "decimals": 9,
            "supply": 0,
        }
    ]


def _feature_vector_from_tokens(tokens: list[dict]) -> np.ndarray:
    """Aggregate token features into one vector (max of binary flags, mean of numeric)."""
    if not tokens:
        return np.array([[0, 0, 0, 0, 0]], dtype=np.float64)
    mint_max = max(t.get("mint_authority_exists", 0) for t in tokens)
    freeze_max = max(t.get("freeze_authority_exists", 0) for t in tokens)
    meta_max = max(t.get("metadata_missing", 0) for t in tokens)
    dec_mean = np.mean([t.get("decimals", 0) for t in tokens])
    supply_mean = np.mean([t.get("supply", 0) for t in tokens])
    return np.array([[mint_max, freeze_max, meta_max, dec_mean, supply_mean]], dtype=np.float64)


def _load_reason_penalties(path: Path) -> dict[str, int]:
    """Load reason_penalties.csv; return {wallet: penalty_score}. Missing wallets = 0."""
    out: dict[str, int] = {}
    if not path.exists():
        logger.debug("predict_wallet_score_penalties_missing", path=str(path))
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        penalty_col = "penalty_score"
        wallet_col = "wallet"
        for row in reader:
            w = (row.get(wallet_col) or "").strip()
            if not w:
                continue
            try:
                p = int(row.get(penalty_col, 0) or 0)
            except (TypeError, ValueError):
                p = 0
            out[w] = max(0, p)
    logger.info("predict_wallet_score_penalties_loaded", path=str(path), wallets=len(out))
    return out


def _load_reason_codes(path: Path) -> dict[str, tuple[list[str], list[str], dict[str, int]]]:
    """
    Load reason codes from CSV.
    Returns dict[wallet, (codes, texts, code_to_days_old)].
    code_to_days_old: min days_old per code when duplicated.
    """
    out: dict[str, tuple[list[str], list[str], dict[str, int]]] = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        has_days_old = "days_old" in fieldnames
        for r in reader:
            w = r.get("wallet", "").strip()
            if not w:
                continue
            prev = out.get(w, ([], [], {}))
            codes, texts, code_to_days = list(prev[0]), list(prev[1]), dict(prev[2])
            if "reason_code" in r:
                code = (r.get("reason_code") or "").strip()
                if code:
                    codes.append(code)
                    if has_days_old:
                        try:
                            d = int(float(r.get("days_old", 0) or 0))
                            code_to_days[code] = min(code_to_days.get(code, 9999), max(0, d))
                        except (TypeError, ValueError):
                            code_to_days[code] = code_to_days.get(code, 0)
                texts = [c.replace("_", " ").title() for c in codes]
                out[w] = (codes, texts, code_to_days)
                continue
            try:
                codes = json.loads(r.get("reason_codes", "[]"))
            except Exception:
                codes = []
            texts = [c.replace("_", " ").title() for c in codes]
            out[w] = (codes, texts, code_to_days)
    return out


def _load_dynamic_risk_features() -> dict[str, tuple[float, float, float]]:
    """Load cluster_size, flow_amount, tx_count per wallet. Returns dict[wallet, (cluster_size, flow_amount, tx_count)]."""
    out: dict[str, tuple[float, float, float]] = {}
    # cluster_features or graph_cluster for cluster_size
    for path in [CLUSTER_FEATURES_CSV, GRAPH_CLUSTER_CSV]:
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    w = (r.get("wallet") or "").strip()
                    if not w:
                        continue
                    cs = float(r.get("cluster_size", 0) or 0)
                    prev = out.get(w, (0, 0, 0))
                    out[w] = (cs, prev[1], prev[2])
            break
    # flow_features for tx_count (total_tx); flow_amount defaults to 0
    if FLOW_FEATURES_CSV.exists():
        with open(FLOW_FEATURES_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                w = (r.get("wallet") or "").strip()
                if not w:
                    continue
                tc = float(r.get("total_tx", 0) or 0)
                prev = out.get(w, (0, 0, 0))
                out[w] = (prev[0], prev[1], tc)
    return out


def _load_wallet_meta() -> dict[str, dict]:
    """Load wallet_age_days, last_scam_days per wallet. Returns dict[wallet, {wallet_age_days, last_scam_days}]."""
    out: dict[str, dict] = {}
    for path in [CLUSTER_FEATURES_CSV, GRAPH_CLUSTER_CSV, FLOW_FEATURES_CSV]:
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                w = (r.get("wallet") or "").strip()
                if not w:
                    continue
                prev = out.get(w, {"wallet_age_days": 0, "last_scam_days": 9999, "graph_distance": 999, "is_test_wallet": 0})
                if "wallet_age_days" in r:
                    prev["wallet_age_days"] = int(float(r.get("wallet_age_days", 0) or 0))
                if "account_age_days" in r and prev["wallet_age_days"] == 0:
                    prev["wallet_age_days"] = int(float(r.get("account_age_days", 0) or 0))
                if "last_scam_days" in r:
                    prev["last_scam_days"] = int(float(r.get("last_scam_days", 9999) or 9999))
                if "graph_distance" in r:
                    d = int(float(r.get("graph_distance", 999) or 999))
                    prev["graph_distance"] = d if d >= 0 else 999
                elif "distance_to_scam" in r:
                    d = int(float(r.get("distance_to_scam", 999) or 999))
                    prev["graph_distance"] = d if d >= 0 else 999
                prev["is_test_wallet"] = 1 if w.startswith("TEST_") else 0
                out[w] = prev
    return out


async def predict_wallet_score_for_wallet(wallet: str) -> float:
    """
    Score a single wallet using the ML model and update trust_scores.
    Does NOT read cluster_features.csv. For realtime use.
    """
    logger.info("predict_wallet_score_realtime_start", wallet=wallet)

    if not MODEL_PATH.exists():
        logger.warning("predict_wallet_score_realtime_model_missing", path=str(MODEL_PATH))
        return 50.0  # Neutral fallback

    model = joblib.load(MODEL_PATH)
    scam_wallets = _load_scam_wallets(SCAM_WALLETS_CSV)

    tokens = _get_token_history_mock(wallet)

    if not tokens:
        ml_score = 50.0
        scam_prob = 0.5
    else:
        X = _feature_vector_from_tokens(tokens)
        if wallet in scam_wallets:
            scam_prob = 1.0
        else:
            scam_prob = float(model.predict_proba(X)[0, 1])
        risk_score = round(scam_prob * 100)
        ml_score = float(100 - risk_score)

    # Guard: mock token always returns 0 risk for any wallet.
    # New wallets with no real data must not receive inflated scores.
    if scam_prob < 0.05 and len(tokens) <= 1:
        ml_score = 50.0
        logger.info("realtime_base_score_normalized_to_50", wallet=wallet)

    conn = await get_conn()
    try:
        await conn.execute(
            """
            UPDATE trust_scores
            SET ml_score = $2
            WHERE wallet = $1
            """,
            wallet,
            ml_score,
        )
    finally:
        await release_conn(conn)

    logger.info(
        "predict_wallet_score_realtime_done",
        wallet=wallet,
        ml_score=ml_score,
    )

    return ml_score


def predict_wallet_score_batch() -> int:
    """
    Batch scoring used by run_full_pipeline.
    Loads wallets from cluster_features.csv.
    """
    print("[predict_wallet_score] loading wallets from", CLUSTER_FEATURES_CSV)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not CLUSTER_FEATURES_CSV.exists():
        print("[predict_wallet_score] ERROR: cluster_features.csv not found:", CLUSTER_FEATURES_CSV)
        return 1

    wallets = _load_valid_wallets_from_cluster(CLUSTER_FEATURES_CSV)
    print("[predict_wallet_score] valid wallets:", len(wallets))
    print("Sample wallets:", wallets[:10])
    print("Wallet length check:", [(w, len(w)) for w in wallets[:10]])
    scam_wallets = _load_scam_wallets(SCAM_WALLETS_CSV)
    penalty_map = _load_reason_penalties(REASON_PENALTIES_CSV)
    reason_map = _load_reason_codes(REASON_CODES_CSV)
    dynamic_features = _load_dynamic_risk_features()
    wallet_meta_map = _load_wallet_meta()

    if not MODEL_PATH.exists():
        print("[predict_wallet_score] ERROR: model not found:", MODEL_PATH)
        return 1

    print("[predict_wallet_score] loading model", MODEL_PATH)
    model = joblib.load(MODEL_PATH)

    rows: list[dict] = []
    for wallet in wallets:
        logger.info("predict_wallet_score_processing", wallet=wallet)
        wallet_meta = wallet_meta_map.get(wallet, {})
        if wallet_meta.get("is_test_wallet"):
            continue
        if not is_valid_wallet(wallet):
            print("[predict_wallet_score] skip invalid wallet:", wallet)
            continue
        tokens = _get_token_history_mock(wallet)

        # If wallet has no token history, treat as neutral wallet
        if not tokens:
            logger.debug("predict_wallet_score_empty_wallet", wallet=wallet[:16] + "...")
            scam_prob = 0.5
            risk_score = 50
            ml_score = 50
        else:
            X = _feature_vector_from_tokens(tokens)

            # Known scam creator override
            if wallet in scam_wallets:
                scam_prob = 1.0
            else:
                scam_prob = float(model.predict_proba(X)[0, 1])

            risk_score = round(scam_prob * 100)
            ml_score = 100 - risk_score

        logger.debug(
            "predict_wallet_score_result",
            wallet=wallet[:16] + "...",
            scam_prob=scam_prob,
            ml_score=ml_score,
        )

        # ---------------------------------------------------------
        # Guard: brand new wallets should not be scored by ML
        # ---------------------------------------------------------
        wallet_age_days = int(wallet_meta.get("wallet_age_days", 0) or 0)
        cluster_size, flow_amount, tx_count = dynamic_features.get(wallet, (0, 0, 0))

        if wallet_age_days == 0 and tx_count == 0:
            ml_score = 50
            logger.info(
                "ml_score_override_new_wallet",
                wallet=wallet,
                wallet_age_days=wallet_age_days,
                tx_count=tx_count,
            )
        # If model uses mock data or low evidence, normalize base score
        elif scam_prob < 0.05 and len(tokens) <= 1:
            ml_score = 50
            logger.info("base_score_normalized_to_50", wallet=wallet)
        penalty = -penalty_map.get(wallet, 0)  # negative so base + penalty reduces score
        final_score = max(0, min(100, ml_score + penalty))

        reason_data = reason_map.get(wallet, ([], [], {}))
        reason_codes, reason_text = reason_data[0], reason_data[1]
        code_to_days_old = reason_data[2] if len(reason_data) > 2 else {}

        # Deduplicate to prevent inflated scores (e.g. duplicate CLEAN_HISTORY)
        unique_reasons = {}
        for code in reason_codes:
            if code not in unique_reasons:
                unique_reasons[code] = code
        reason_codes = list(unique_reasons.keys())

        reason_list = []
        for code in reason_codes:
            raw_weight = get_reason_weights().get(code, 0)
            fixed_weight = normalize_reason_weight(code, raw_weight)

            days_old_val = code_to_days_old.get(code, 0) if code_to_days_old else 0
            graph_distance = int(wallet_meta.get("graph_distance", 999) or 999)
            reason_list.append({
                "code": code,
                "weight": fixed_weight,
                "confidence": 1,
                "tx_hash": None,
                "solscan": None,
                "days_old": days_old_val,
                "graph_distance": graph_distance,
            })

        logger.info(
            "reason_weights_normalized",
            wallet=wallet,
            reasons=[(r["code"], r["weight"]) for r in reason_list],
        )
        reasons = safe_reasons(reason_list or [])

        # Remove duplicate reason codes (credit-score safety)
        unique = {}
        for r in reasons:
            unique[r["code"]] = r
        reasons = list(unique.values())

        base_score = ml_score
        logger.info(
            "predict_wallet_score_running",
            base_score=base_score,
            reason_count=len(reasons),
        )
        # --- apply confidence ---
        positive_sum = sum(
            r["weight"] * (r.get("confidence") or 1)
            for r in reasons
            if r["weight"] > 0
        )

        positive_sum = min(positive_sum, 40)

        logger.info(
            "anti_spam_score_check",
            wallet=wallet,
            unique_reason_count=len(reasons),
            positive_sum=positive_sum,
        )

        negative_sum = sum(
            r["weight"] * (r.get("confidence") or 1)
            for r in reasons
            if r["weight"] < 0
        )

        # Bayesian scam probability update
        prior = get_prior(wallet)
        if prior is None and TEST_MODE:
            prior = 0.05
        prior = prior if prior is not None else 0.05
        wallet_age_days = int(wallet_meta.get("wallet_age_days", 0) or 0)
        cluster_size, flow_amount, tx_count = dynamic_features.get(wallet, (0, 0, 0))

        # Guard: do NOT apply Bayesian risk to brand new wallets
        if wallet_age_days == 0 and tx_count == 0:
            posterior = 0.0
            logger.info(
                "bayesian_skip_new_wallet",
                wallet=wallet,
                wallet_age_days=wallet_age_days,
                tx_count=tx_count,
            )
        else:
            posterior = update_scam_probability(prior, reasons)
        reasons_for_log = [
            {
                "code": r.get("code"),
                "likelihood": LIKELIHOOD_TABLE.get(r.get("code"), 0.05),
                "confidence": r.get("confidence", 1),
            }
            for r in reasons
        ]
        try:
            save_wallet_risk_probability(wallet, prior, posterior, reasons_for_log)
        except Exception as e:
            logger.exception("save_wallet_risk_probability_failed", wallet=wallet, error=str(e))
        try:
            save_bayesian_history(wallet, prior, posterior, [r.get("code", "") for r in reasons if r.get("code")])
        except Exception:
            pass

        # Sanitize inputs to prevent None crash
        base_score = safe_num(base_score, 70)
        positive_sum = safe_num(positive_sum, 0)
        negative_sum = safe_num(negative_sum, 0)
        logger.info(
            "score_inputs",
            wallet=wallet,
            base_score=base_score,
            positive=positive_sum,
            negative=negative_sum,
        )

        # Professional credit-score style: soft penalty + max penalty limit
        adjusted_base = base_score + min(positive_sum, 40)
        total_weight_neg = abs(negative_sum)
        risk_ratio = min(total_weight_neg / 100, 0.9)
        max_penalty = 0.7 * adjusted_base
        total_penalty = min(adjusted_base * risk_ratio, max_penalty)
        penalty_amt = -total_penalty  # negative so base + penalty reduces score
        final_score = int(adjusted_base + penalty_amt)

        if TEST_MODE and wallet.startswith("TEST_"):
            reasons = [
                {
                    "code": "SCAM_CLUSTER_MEMBER",
                    "weight": -30,
                    "confidence": 1,
                    "days_old": 1,
                }
            ]

        penalty, reasons = compute_dynamic_penalty(
            reasons, wallet, cluster_size, flow_amount, tx_count
        )
        reasons = safe_reasons(reasons)
        if not reasons and TEST_MODE:
            reasons = [{"code": "NO_RISK_DETECTED"}]
        logger.info("reason_count", wallet=wallet, count=len(reasons))
        print("DEBUG penalty:", penalty)
        time_weighted_penalty = penalty
        final_score += penalty  # penalty is negative for scam; adding reduces score

        # Apply Bayesian posterior penalty
        final_score -= int(posterior * 100)

        logger.info(
            "dynamic_risk_result",
            wallet=wallet,
            applied=penalty < 0,
            penalty=penalty,
        )

        final_score = safe_num(final_score, 0)
        final_score = max(0, min(100, final_score))

        logger.info(
            "scoring_debug",
            wallet=wallet,
            base_score=base_score,
            positive=positive_sum,
            negative=negative_sum,
            final_score=final_score,
        )
        reasons = safe_reasons(reasons)
        reason_codes = [r["code"] for r in reasons]
        reason_text = [c.replace("_", " ").title() for c in reason_codes]

        codes = {r["code"] for r in reasons}
        if codes == {"CLEAN_HISTORY"}:
            before = final_score
            final_score = min(final_score, 80)
            if final_score != before:
                logger.info("final_score_capped", wallet=wallet, reason="CLEAN_HISTORY", capped_to=final_score)
        if "NEW_WALLET" in codes:
            before = final_score
            final_score = min(final_score, 60)
            if final_score != before:
                logger.info("final_score_capped", wallet=wallet, reason="NEW_WALLET", capped_to=final_score)
        if "LOW_ACTIVITY" in codes:
            before = final_score
            final_score = min(final_score, 70)
            if final_score != before:
                logger.info("final_score_capped", wallet=wallet, reason="LOW_ACTIVITY", capped_to=final_score)

        # Reputation decay based on wallet age and scam recency
        wallet_meta = wallet_meta_map.get(wallet, {})
        wallet_age_days = int(wallet_meta.get("wallet_age_days", 0) or 0)
        last_scam_days = int(wallet_meta.get("last_scam_days", 9999) or 9999)
        final_score, decay_adjustment = apply_reputation_decay(
            final_score,
            wallet_age_days,
            last_scam_days,
        )

        # Graph distance penalty
        graph_distance = int(wallet_meta.get("graph_distance", 999) or 999)
        final_score, graph_penalty = apply_graph_penalty(final_score, graph_distance)

        # Risk level from reason severity, not score
        risk_level = risk_level_from_reasons(reasons)
        risk = score_to_risk(final_score)
        summary = build_summary(reasons, final_score)

        # Queue for manual review if rules trigger
        cluster_sz = int(cluster_size) if cluster_size is not None else None
        conf = 1.0 - min(1.0, abs(final_score - 50) / 50) if final_score is not None else 0.5
        try:
            check_for_review(
                wallet,
                float(final_score),
                confidence=conf,
                reasons=reasons,
                cluster_size=cluster_sz,
            )
        except Exception as e:
            logger.debug("review_queue_check_skip", wallet=wallet[:16], error=str(e))

        metadata = json.dumps({"risk": risk, "summary": summary})
        try:
            update_wallet_score(
                wallet,
                final_score,
                risk_level,
                metadata,
                wallet_age_days=wallet_age_days,
                last_scam_days=last_scam_days,
                decay_adjustment=decay_adjustment,
                graph_distance=graph_distance,
                graph_penalty=graph_penalty,
                time_weighted_penalty=time_weighted_penalty,
            )
        except Exception as e:
            logger.exception("predict_wallet_score_update_db_error", wallet=wallet, error=str(e))

        rows.append({
            "wallet": wallet,
            "ml_score": ml_score,
            "penalty": penalty,
            "final_score": final_score,
            "risk_level": risk_level,
            "reason_codes": json.dumps(reason_codes),
            "reason_text": json.dumps(reason_text),
            "summary": summary,
        })

    if len(rows) == 0:
        print("[predict_wallet_score] No valid wallets → skip writing wallet_scores.csv")
        return 0

    with open(WALLET_SCORES_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "wallet",
                "ml_score",
                "penalty",
                "final_score",
                "risk_level",
                "reason_codes",
                "reason_text",
                "summary",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    logger.info("predict_wallet_score_done", path=str(WALLET_SCORES_CSV), rows=len(rows))
    print("[predict_wallet_score] wrote", len(rows), "rows to", WALLET_SCORES_CSV)

    try:
        inserted = save_wallet_scores_from_csv(str(WALLET_SCORES_CSV))
        print("[predict_wallet_score] saved to DB:", inserted, "rows")
    except Exception as e:
        logger.exception("predict_wallet_score_save_db_error", path=str(WALLET_SCORES_CSV), error=str(e))
        print("[predict_wallet_score] ERROR saving to DB:", e)

    async def _fill_missing_scores():
        conn = await get_conn()
        try:
            reason_rows = await conn.fetch("SELECT DISTINCT wallet FROM wallet_reasons WHERE wallet IS NOT NULL")
            reason_wallets = {r["wallet"] for r in reason_rows}
            score_rows = await conn.fetch("SELECT wallet FROM trust_scores WHERE wallet IS NOT NULL")
            scored_wallets = {r["wallet"] for r in score_rows}
            missing_wallets = sorted(reason_wallets - scored_wallets)
            inserted_missing = 0
            for wallet in missing_wallets:
                computed_at = int(time.time())
                await conn.execute(
                    "INSERT INTO trust_scores(wallet, score, computed_at) VALUES ($1, $2, $3) ON CONFLICT (wallet) DO NOTHING",
                    wallet, 50, computed_at,
                )
                inserted_missing += 1
            if inserted_missing:
                logger.info(
                    "predict_wallet_score_missing_trust_scores_added",
                    count=inserted_missing,
                )
        finally:
            await release_conn(conn)

    try:
        asyncio.run(_fill_missing_scores())
    except Exception as e:
        logger.exception("predict_wallet_score_missing_scores_error", error=str(e))

    print("[predict_wallet_score] done")
    return 0


def main() -> int:
    """Entrypoint for run_full_pipeline. Invokes batch scoring."""
    return predict_wallet_score_batch()


if __name__ == "__main__":
    raise SystemExit(main())
