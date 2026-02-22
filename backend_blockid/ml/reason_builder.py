"""
BlockID Reason Engine — derive reason_codes from suspicious tx / behavioral feature data.

Consumes merged feature datasets (drainer, flow, graph clustering), groups by wallet,
applies rule-based thresholds to generate reason codes, counts frequency per reason,
and persists to wallet_reason_codes.csv for downstream publish and DB integration.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

_ML_DIR = Path(__file__).resolve().parent
_DATA_DIR = _ML_DIR.parent / "data"
DEFAULT_OUTPUT_CSV = _DATA_DIR / "wallet_reason_codes.csv"
DEFAULT_WEIGHTS_PATH = _DATA_DIR / "reason_weights.json"
TOP_N_REASONS = 3

# Reason codes (align with blockid_spec, trust_engine, API)
REASON_NEAR_SCAM_CLUSTER = "NEAR_SCAM_CLUSTER"
REASON_HIGH_RAPID_TX = "HIGH_RAPID_TX"
REASON_MULTI_VICTIM_PATTERN = "MULTI_VICTIM_PATTERN"
REASON_NEW_CONTRACT_INTERACTION = "NEW_CONTRACT_INTERACTION"
REASON_HIGH_APPROVAL_RISK = "HIGH_APPROVAL_RISK"
REASON_SUDDEN_DRAIN_PATTERN = "SUDDEN_DRAIN_PATTERN"
REASON_NEW_WALLET = "NEW_WALLET"
REASON_LOW_ACTIVITY = "LOW_ACTIVITY"
REASON_SWAP_THEN_TRANSFER = "SWAP_THEN_TRANSFER"
REASON_HIGH_CLUSTER_CONTAMINATION = "HIGH_CLUSTER_CONTAMINATION"
# New rules
REASON_SCAM_CLUSTER_MEMBER = "SCAM_CLUSTER_MEMBER"
REASON_DRAINER_INTERACTION = "DRAINER_INTERACTION"
REASON_RUGPULL_DEPLOYER = "RUGPULL_DEPLOYER"
REASON_LARGE_OUTFLOW_TO_NEW_WALLET = "LARGE_OUTFLOW_TO_NEW_WALLET"
REASON_RAPID_TOKEN_DUMP = "RAPID_TOKEN_DUMP"
REASON_WASH_TRADE_PATTERN = "WASH_TRADE_PATTERN"

# Thresholds for rule-based mapping
THRESHOLD_RAPID_TX = 5
THRESHOLD_TOTAL_TX_NEW = 3
THRESHOLD_UNIQUE_DEST_LOW = 2
THRESHOLD_APPROVAL_LIKE = 1
THRESHOLD_DISTANCE_TO_SCAM = 1
THRESHOLD_SCAM_NEIGHBOR = 1
THRESHOLD_PERCENT_SAME_CLUSTER = 50.0
THRESHOLD_PERCENT_TO_NEW_WALLETS = 70.0
THRESHOLD_RAPID_OUTFLOW_DUMP = 3
THRESHOLD_WASH_TRADE = 1


def _rule_neighbor_scam(row: pd.Series) -> list[str]:
    """Graph clustering: proximity to known scams."""
    codes = []
    if "scam_neighbor_count" in row.index:
        val = pd.to_numeric(row.get("scam_neighbor_count", 0), errors="coerce")
        if (val or 0) >= THRESHOLD_SCAM_NEIGHBOR:
            codes.append(REASON_NEAR_SCAM_CLUSTER)
    if "distance_to_scam" in row.index:
        val = pd.to_numeric(row.get("distance_to_scam", -1), errors="coerce")
        if val is not None and 0 <= val <= THRESHOLD_DISTANCE_TO_SCAM:
            codes.append(REASON_NEAR_SCAM_CLUSTER)
    if "percent_to_same_cluster" in row.index:
        val = pd.to_numeric(row.get("percent_to_same_cluster", 0), errors="coerce")
        if (val or 0) >= THRESHOLD_PERCENT_SAME_CLUSTER:
            codes.append(REASON_HIGH_CLUSTER_CONTAMINATION)
    # SCAM_CLUSTER_MEMBER: wallet is in a known scam cluster
    for col in ("is_scam_cluster_member", "scam_cluster_member", "cluster_is_scam"):
        if col in row.index:
            val = row.get(col)
            if pd.notna(val) and (val == 1 or (isinstance(val, str) and val.lower() in ("true", "1", "yes"))):
                codes.append(REASON_SCAM_CLUSTER_MEMBER)
                break
    return codes


def _rule_flow(row: pd.Series) -> list[str]:
    """Flow features: rapid tx, new wallet, low activity, large outflow to new wallets."""
    codes = []
    total_tx = pd.to_numeric(row.get("total_tx", 0), errors="coerce") or 0
    rapid_tx = pd.to_numeric(row.get("rapid_tx_count", 0), errors="coerce") or 0
    unique_dest = pd.to_numeric(row.get("unique_destinations", 0), errors="coerce") or 0
    pct_new = pd.to_numeric(row.get("percent_to_new_wallets", 0), errors="coerce") or 0
    rapid_out = pd.to_numeric(row.get("rapid_outflow_count", 0), errors="coerce") or 0

    if total_tx < THRESHOLD_TOTAL_TX_NEW:
        codes.append(REASON_NEW_WALLET)
    if total_tx < THRESHOLD_TOTAL_TX_NEW or unique_dest < THRESHOLD_UNIQUE_DEST_LOW:
        if REASON_LOW_ACTIVITY not in codes:
            codes.append(REASON_LOW_ACTIVITY)
    if rapid_tx >= THRESHOLD_RAPID_TX:
        codes.append(REASON_HIGH_RAPID_TX)
    # LARGE_OUTFLOW_TO_NEW_WALLET: high % of outflow to new wallets + rapid outflow
    if pct_new >= THRESHOLD_PERCENT_TO_NEW_WALLETS and rapid_out > 0:
        codes.append(REASON_LARGE_OUTFLOW_TO_NEW_WALLET)
    # WASH_TRADE_PATTERN: self-trade or circular flow
    wash = pd.to_numeric(row.get("wash_trade_count", 0), errors="coerce") or 0
    if wash >= THRESHOLD_WASH_TRADE:
        codes.append(REASON_WASH_TRADE_PATTERN)
    return codes


def _rule_drainer(row: pd.Series) -> list[str]:
    """Drainer heuristics: approval risk, multi-victim, new contract, swap-then-transfer."""
    codes = []
    approval = pd.to_numeric(row.get("approval_like_count", 0), errors="coerce") or 0
    rapid_out = pd.to_numeric(row.get("rapid_outflow_count", 0), errors="coerce") or 0
    multi_victim = pd.to_numeric(row.get("multi_victim_pattern", 0), errors="coerce") or 0
    new_contract = pd.to_numeric(row.get("new_contract_interaction_count", 0), errors="coerce") or 0
    swap_transfer = pd.to_numeric(row.get("swap_then_transfer_pattern", 0), errors="coerce") or 0

    if approval >= THRESHOLD_APPROVAL_LIKE:
        codes.append(REASON_HIGH_APPROVAL_RISK)
    if rapid_out > 0:
        codes.append(REASON_SUDDEN_DRAIN_PATTERN)
    if rapid_out >= THRESHOLD_RAPID_OUTFLOW_DUMP:
        codes.append(REASON_RAPID_TOKEN_DUMP)
    if multi_victim > 0:
        codes.append(REASON_MULTI_VICTIM_PATTERN)
    if new_contract > 0:
        codes.append(REASON_NEW_CONTRACT_INTERACTION)
    if swap_transfer > 0:
        codes.append(REASON_SWAP_THEN_TRANSFER)
    # DRAINER_INTERACTION: direct interaction with known drainer contract
    drainer_col = pd.to_numeric(row.get("drainer_interaction_count", 0), errors="coerce") or 0
    if drainer_col > 0:
        codes.append(REASON_DRAINER_INTERACTION)
    # RUGPULL_DEPLOYER: deployed contract involved in rugpull
    rugpull = pd.to_numeric(row.get("rugpull_deployer", 0), errors="coerce") or 0
    if rugpull > 0:
        codes.append(REASON_RUGPULL_DEPLOYER)
    return codes


def _reasons_for_row(row: pd.Series) -> list[str]:
    """Aggregate all rule outputs for one wallet row."""
    seen: set[str] = set()
    codes: list[str] = []
    for rule in (_rule_neighbor_scam, _rule_flow, _rule_drainer):
        for c in rule(row):
            if c not in seen:
                seen.add(c)
                codes.append(c)
    return codes


def load_reason_weights(path: Path | None = None) -> dict[str, float]:
    """Load reason_weights.json. Keys are reason codes; values are weights. Unknown codes use _default."""
    p = path or DEFAULT_WEIGHTS_PATH
    weights: dict[str, float] = {}
    default = 0.5
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            default = float(data.get("_default", 0.5))
            for k, v in data.items():
                if k.startswith("_"):
                    continue
                try:
                    weights[k] = float(v)
                except (TypeError, ValueError):
                    pass
        except Exception as e:
            logger.warning("reason_weights_load_failed", path=str(p), error=str(e))
    return weights


def compute_weighted_risk_score(
    reason_freq: dict[str, int],
    weights: dict[str, float] | None = None,
    *,
    default_weight: float = 0.5,
) -> float:
    """
    Compute weighted risk score 0–100 from reason frequencies.
    score = min(100, sum(weight[code] * freq[code]) * scale). Higher = riskier.
    """
    if not reason_freq:
        return 0.0
    w = weights or load_reason_weights()
    total = 0.0
    for code, freq in reason_freq.items():
        weight = w.get(code, default_weight)
        total += weight * freq
    # Scale: typical max ~5 reasons * 1.0 weight = 5; target 0–100
    scale = 20.0
    return min(100.0, total * scale)


def load_suspicious_dataset(
    path: Path | None = None,
    *,
    cluster_path: Path | None = None,
    flow_path: Path | None = None,
    drainer_path: Path | None = None,
) -> pd.DataFrame:
    """
    Load suspicious tx / feature dataset. Prefer single merged CSV; else merge cluster + flow + drainer.
    """
    if path is not None and path.exists():
        df = pd.read_csv(path)
        logger.info("reason_builder_loaded", path=str(path), rows=len(df))
        return df

    data_dir = cluster_path.parent if cluster_path else _DATA_DIR
    cluster_p = cluster_path or data_dir / "cluster_features.csv"
    cluster_alt = data_dir / "graph_cluster_features.csv"
    flow_p = flow_path or data_dir / "flow_features.csv"
    drainer_p = drainer_path or data_dir / "drainer_features.csv"
    if not drainer_p.exists() and (data_dir / "devnet_dummy" / "drainer_features.csv").exists():
        drainer_p = data_dir / "devnet_dummy" / "drainer_features.csv"
    if not flow_p.exists() and (data_dir / "devnet_dummy" / "flow_features.csv").exists():
        flow_p = data_dir / "devnet_dummy" / "flow_features.csv"

    dfs: list[pd.DataFrame] = []
    for p in [cluster_p, cluster_alt]:
        if p.exists():
            dfs.append(pd.read_csv(p))
            logger.debug("reason_builder_loaded_part", path=str(p))
            break
    if flow_p.exists():
        dfs.append(pd.read_csv(flow_p))
    if drainer_p.exists():
        dfs.append(pd.read_csv(drainer_p))

    if not dfs:
        return pd.DataFrame()

    base = dfs[0].copy()
    for other in dfs[1:]:
        if "wallet" not in other.columns:
            continue
        join_cols = [c for c in other.columns if c != "wallet"]
        if join_cols:
            base = base.merge(other[["wallet"] + join_cols], on="wallet", how="outer")
            base[join_cols] = base[join_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    return base


def build_reason_codes(
    df: pd.DataFrame,
    weights_path: Path | None = None,
) -> pd.DataFrame:
    """
    Group by wallet, generate reason_codes list, count frequency, compute weighted risk score.
    Returns DataFrame with columns: wallet, reason_codes, reason_freq, top_3_reasons, weighted_risk_score.
    """
    if df.empty or "wallet" not in df.columns:
        logger.warning("reason_builder_empty_input")
        return pd.DataFrame(columns=["wallet", "reason_codes", "reason_freq", "top_3_reasons", "weighted_risk_score"])

    weights = load_reason_weights(weights_path)
    default_weight = 0.5
    if DEFAULT_WEIGHTS_PATH.exists():
        try:
            with open(DEFAULT_WEIGHTS_PATH, encoding="utf-8") as f:
                default_weight = float(json.load(f).get("_default", 0.5))
        except Exception:
            pass

    wallet_reasons: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        w = str(row.get("wallet", "")).strip()
        if not w or w.lower() == "nan":
            continue
        codes = _reasons_for_row(row)
        if w not in wallet_reasons:
            wallet_reasons[w] = []
        wallet_reasons[w].extend(codes)

    rows = []
    for wallet, codes in wallet_reasons.items():
        freq = dict(Counter(codes))
        ordered = sorted(freq.keys(), key=lambda k: -freq[k])
        top3 = ordered[:TOP_N_REASONS]
        top_3_str = ",".join(top3)
        weighted_score = compute_weighted_risk_score(freq, weights, default_weight=default_weight)
        rows.append({
            "wallet": wallet,
            "reason_codes": json.dumps(ordered),
            "reason_freq": json.dumps(freq),
            "top_3_reasons": top_3_str,
            "weighted_risk_score": round(weighted_score, 2),
        })

    out = pd.DataFrame(rows)
    total_reasons = sum(len(json.loads(r["reason_codes"])) for r in rows)
    logger.info("reason_builder_built", wallets=len(out), total_reasons=total_reasons)
    return out


def save_wallet_reason_codes(df: pd.DataFrame, path: Path | None = None) -> Path:
    """Persist to wallet_reason_codes.csv. Returns output path."""
    out_path = path or DEFAULT_OUTPUT_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info("reason_builder_saved", path=str(out_path), rows=len(df))
    return out_path


def run(
    input_path: Path | None = None,
    output_path: Path | None = None,
    weights_path: Path | None = None,
) -> tuple[pd.DataFrame, Path]:
    """
    Full pipeline: load dataset → build reasons → save CSV.
    Returns (result DataFrame, output path).
    """
    df = load_suspicious_dataset(input_path)
    if df.empty:
        return pd.DataFrame(), output_path or DEFAULT_OUTPUT_CSV
    result = build_reason_codes(df, weights_path=weights_path)
    if result.empty:
        return result, output_path or DEFAULT_OUTPUT_CSV
    saved = save_wallet_reason_codes(result, output_path)
    return result, saved


def load_reason_cache(
    path: Path | None = None,
    *,
    top_n: int | None = TOP_N_REASONS,
) -> dict[str, list[str]]:
    """
    Load wallet_reason_codes.csv and return {wallet: [reason_codes]}.
    When top_n is set, returns top N reasons per wallet (from top_3_reasons column when present).
    Used by publish_one_wallet to persist reason_codes to DB.
    """
    p = path or DEFAULT_OUTPUT_CSV
    cache: dict[str, list[str]] = {}
    if not p.exists():
        return cache
    try:
        df = pd.read_csv(p)
        if "wallet" not in df.columns:
            return cache
        has_top = "top_3_reasons" in df.columns and top_n is not None
        has_codes = "reason_codes" in df.columns
        for _, row in df.iterrows():
            w = str(row.get("wallet", "")).strip()
            if not w:
                continue
            if has_top and top_n:
                raw = row.get("top_3_reasons", "")
                codes = [c.strip() for c in str(raw).split(",") if c.strip()] if raw else []
            elif has_codes:
                raw = row.get("reason_codes", "[]")
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) else (raw or [])
                    codes = [str(c) for c in parsed[: top_n or 999]] if top_n else [str(c) for c in parsed]
                except (json.JSONDecodeError, TypeError):
                    codes = []
            else:
                codes = []
            cache[w] = codes
        logger.debug("reason_builder_cache_loaded", path=str(p), wallets=len(cache))
    except Exception as e:
        logger.warning("reason_builder_cache_failed", path=str(p), error=str(e))
    return cache


def get_reason_codes_for_wallet(
    wallet: str,
    cache: dict[str, list[str]] | None = None,
    *,
    top_n: int | None = TOP_N_REASONS,
) -> list[str]:
    """Lookup reason codes for a wallet (top N by default). Use preloaded cache or load from CSV."""
    w = (wallet or "").strip()
    if not w:
        return []
    if cache is not None:
        return cache.get(w, [])
    return load_reason_cache(top_n=top_n).get(w, [])


def get_weighted_risk_for_wallet(wallet: str, path: Path | None = None) -> float:
    """Lookup weighted_risk_score for a wallet from CSV. Returns 0.0 if not found."""
    p = path or DEFAULT_OUTPUT_CSV
    if not p.exists():
        return 0.0
    try:
        df = pd.read_csv(p)
        if "wallet" not in df.columns or "weighted_risk_score" not in df.columns:
            return 0.0
        w = (wallet or "").strip()
        row = df[df["wallet"].astype(str).str.strip() == w]
        if row.empty:
            return 0.0
        return float(pd.to_numeric(row.iloc[0]["weighted_risk_score"], errors="coerce") or 0.0)
    except Exception:
        return 0.0


def main() -> int:
    """CLI entrypoint. Run: py -m backend_blockid.ml.reason_builder [--input PATH] [--output PATH] [--weights PATH]"""
    import argparse

    parser = argparse.ArgumentParser(description="BlockID Reason Engine — build wallet_reason_codes from features")
    parser.add_argument("--input", type=Path, default=None, help="Merged features CSV (default: merge cluster+flow+drainer)")
    parser.add_argument("--output", type=Path, default=None, help="Output CSV (default: data/wallet_reason_codes.csv)")
    parser.add_argument("--weights", type=Path, default=None, help="reason_weights.json path")
    args = parser.parse_args()

    result, out_path = run(input_path=args.input, output_path=args.output, weights_path=args.weights)
    if result.empty:
        print("[reason_builder] No data; run pipeline STEP 1-3 first")
        return 1
    print(f"[reason_builder] Saved {len(result)} wallets to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
