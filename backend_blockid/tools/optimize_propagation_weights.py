"""
BlockID Propagation Weight Optimizer.

Grid search over propagation parameters to improve scam detection and reduce false positives.
Output: backend_blockid/models/propagation_config.json
Chart: backend_blockid/charts/propagation_tuning.png

Usage:
  py -m backend_blockid.tools.optimize_propagation_weights

Future upgrades:
* Bayesian optimization
* Genetic algorithm tuning
* Per-token propagation weights
* Adaptive propagation
"""
from __future__ import annotations

import csv
import itertools
import json
import sys
from pathlib import Path

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection
from backend_blockid.tools.propagation_engine_v1 import run_propagation_simulation

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_MODELS_DIR = _BACKEND_DIR / "models"
_CHARTS_DIR = _BACKEND_DIR / "charts"
OUTPUT_JSON = _MODELS_DIR / "propagation_config.json"
SCAM_CSV = _DATA_DIR / "scam_wallets.csv"
MANUAL_CSV = _DATA_DIR / "manual_wallets.csv"

BASE_SCORE = 50
SCAM_THRESHOLD = 20
DAYS_BACK = 60

# Grid ranges (coarse to keep runtime manageable)
D1_RANGE = list(range(-60, -19, 10))   # -60, -50, -40, -30, -20
D2_RANGE = list(range(-30, -4, 8))     # -30, -22, -14, -6
D3_RANGE = list(range(-10, 0, 3))      # -10, -7, -4, -1
VOLUME_RANGE = [0.3, 0.5, 0.7, 0.9, 1.0]
TIME_DECAY_RANGE = [15, 30, 45, 60, 90]
MAX_DEPTH_RANGE = [2, 3, 4]


def _load_labels() -> dict[str, int]:
    """Return {wallet: label}. 1=scam, 0=safe."""
    labels: dict[str, int] = {}
    if SCAM_CSV.exists():
        with open(SCAM_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
                if w:
                    labels[w] = 1
    if MANUAL_CSV.exists():
        with open(MANUAL_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or "").strip()
                is_test = (row.get("is_test_wallet") or "").strip() in ("1", "true", "True", "yes")
                if w and not is_test:
                    labels[w] = 0
    return labels


def _evaluate(penalties: dict[str, float], labels: dict[str, int]) -> tuple[float, float, float, float]:
    """Return (precision, recall, fpr, f1). Predict scam if score < SCAM_THRESHOLD."""
    tp = fp = tn = fn = 0
    all_wallets = set(penalties) | set(labels)
    for wallet in all_wallets:
        pen = penalties.get(wallet, 0.0)
        score = max(0.0, BASE_SCORE + pen)
        pred = 1 if score < SCAM_THRESHOLD else 0
        label = labels.get(wallet)
        if label is None:
            continue
        if label == 1 and pred == 1:
            tp += 1
        elif label == 0 and pred == 1:
            fp += 1
        elif label == 0 and pred == 0:
            tn += 1
        else:
            fn += 1
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    return precision, recall, fpr, f1


def main() -> int:
    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    _CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    labels = _load_labels()
    if len(labels) < 5:
        print("[optimize_propagation] WARNING: Few labeled wallets. Need scam_wallets.csv and manual_wallets.csv.")
        return 0

    conn = get_connection()

    best_prec = 0.0
    best_rec = 0.0
    best_config: dict | None = None
    best_metrics: tuple[float, float, float, float] = (0, 0, 0, 0)
    results: list[dict] = []

    count = 0
    total = len(D1_RANGE) * len(D2_RANGE) * len(D3_RANGE) * len(VOLUME_RANGE) * len(TIME_DECAY_RANGE) * len(MAX_DEPTH_RANGE)

    for d1, d2, d3, vol, tdecay, mdepth in itertools.product(
        D1_RANGE, D2_RANGE, D3_RANGE, VOLUME_RANGE, TIME_DECAY_RANGE, MAX_DEPTH_RANGE
    ):
        count += 1
        if count % 50 == 0:
            print(f"[optimize_propagation] {count}/{total} configs evaluated...")
        config = {
            "distance_penalty": {"1": d1, "2": d2, "3": d3},
            "volume_scale": vol,
            "time_decay_days": tdecay,
            "max_depth": mdepth,
        }
        try:
            penalties = run_propagation_simulation(conn, days_back=DAYS_BACK, config=config)
        except Exception:
            continue
        prec, rec, fpr, f1 = _evaluate(penalties, labels)
        results.append({
            "config": config,
            "precision": prec,
            "recall": rec,
            "fpr": fpr,
            "f1": f1,
        })
        if prec >= 0.95 and rec >= 0.7:
            if prec > best_prec or (prec == best_prec and rec > best_rec):
                best_prec = prec
                best_rec = rec
                best_config = config
                best_metrics = (prec, rec, fpr, f1)
        elif best_config is None and f1 > best_metrics[3]:
            best_config = config
            best_metrics = (prec, rec, fpr, f1)

    conn.close()

    if best_config is None and results:
        best = max(results, key=lambda r: r["f1"])
        best_config = best["config"]
        best_metrics = (best["precision"], best["recall"], best["fpr"], best["f1"])

    if best_config is None:
        print("[optimize_propagation] No valid config found (no transactions or scam wallets).")
        return 0

    prec, rec, fpr, f1 = best_metrics
    print()
    print("=" * 55)
    print("PROPAGATION OPTIMIZATION")
    print("=" * 55)
    print(f"Labeled wallets: {len(labels)}")
    print("Best propagation config found.")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {rec:.4f}")
    print(f"  FPR:       {fpr:.4f}")
    print(f"  F1:        {f1:.4f}")
    print("Config:")
    for k, v in best_config.items():
        print(f"  {k}: {v}")
    print("=" * 55)

    save_config = {
        "distance_penalty": {str(k): int(v) for k, v in best_config["distance_penalty"].items()},
        "volume_scale": float(best_config["volume_scale"]),
        "time_decay_days": int(best_config["time_decay_days"]),
        "max_depth": int(best_config["max_depth"]),
    }
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(save_config, f, indent=2)
    print(f"Saved: {OUTPUT_JSON}")

    # Plot precision vs recall
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        precs = [r["precision"] for r in results]
        recs = [r["recall"] for r in results]
        f1s = [r["f1"] for r in results]
        fig, ax = plt.subplots(figsize=(8, 6))
        sc = ax.scatter(recs, precs, c=f1s, cmap="viridis", alpha=0.7, s=20)
        ax.scatter([best_metrics[1]], [best_metrics[0]], c="red", s=100, marker="*", label="best")
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.set_title("Precision vs Recall (propagation tuning)")
        plt.colorbar(sc, ax=ax, label="F1")
        ax.legend()
        ax.set_xlim(-0.05, 1.05)
        ax.set_ylim(-0.05, 1.05)
        plt.tight_layout()
        chart_path = _CHARTS_DIR / "propagation_tuning.png"
        plt.savefig(chart_path, dpi=100)
        plt.close()
        print(f"Chart: {chart_path}")
    except ImportError:
        print("[optimize_propagation] matplotlib not installed, skipping chart")

    return 0


if __name__ == "__main__":
    sys.exit(main())
