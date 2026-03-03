"""
BlockID Reason Weight Optimizer.

Grid search over reason weights to minimize false positives and maximize scam detection.
Output: backend_blockid/models/reason_weights_optimized.csv
Chart: backend_blockid/charts/reason_weight_tuning.png

Usage:
  py -m backend_blockid.tools.optimize_reason_weights

Future upgrades:
* Bayesian optimization
* Gradient descent tuning
* Per-cluster weights
* Time-decay weights
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_MODELS_DIR = _BACKEND_DIR / "models"
_CHARTS_DIR = _BACKEND_DIR / "charts"
OUTPUT_CSV = _MODELS_DIR / "reason_weights_optimized.csv"
SCAM_CSV = _DATA_DIR / "scam_wallets.csv"
MANUAL_CSV = _DATA_DIR / "manual_wallets.csv"

# Tunable reason codes and grid ranges
TUNABLE_CODES = [
    ("SCAM_CLUSTER_MEMBER", range(-60, -9, 5)),   # -55, -50, ..., -10
    ("DRAINER_INTERACTION", range(-80, -9, 5)),   # HIGH_VOLUME_TO_SCAM equivalent
]
BASE_SCORE = 50
SCAM_THRESHOLD = 20


def _load_labels() -> dict[str, int]:
    """Return {wallet: label}. 1=scam, 0=safe. Only wallets in both CSVs get labels."""
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


def _load_wallet_reasons(conn, cur) -> dict[str, list[tuple[str, int]]]:
    """Return {wallet: [(reason_code, db_weight), ...]}."""
    cur.execute(
        "SELECT wallet, reason_code, weight FROM wallet_reasons WHERE wallet IS NOT NULL AND reason_code IS NOT NULL"
    )
    out: dict[str, list[tuple[str, int]]] = {}
    for row in cur.fetchall():
        w = (row[0] or "").strip()
        code = (row[1] or "").strip()
        db_w = int(row[2]) if row[2] is not None else 0
        if w and code:
            out.setdefault(w, []).append((code, db_w))
    return out


def _load_default_weights() -> dict[str, int]:
    from backend_blockid.ml.reason_codes import REASON_WEIGHTS
    return dict(REASON_WEIGHTS)


def _score_wallet(
    reasons: list[tuple[str, int]],
    weights: dict[str, int],
    tunable_overrides: dict[str, int] | None = None,
) -> float:
    """Compute score: base + sum(weights). Positive bonuses capped at 40."""
    w = dict(weights)
    if tunable_overrides:
        w.update(tunable_overrides)
    pos = 0
    neg = 0
    for code, _ in reasons:
        val = w.get(code, 0)
        if val > 0:
            pos += val
        else:
            neg += val
    pos = min(pos, 40)
    return max(0, min(100, BASE_SCORE + pos + neg))


def _evaluate(
    wallet_reasons: dict[str, list[tuple[str, int]]],
    labels: dict[str, int],
    default_weights: dict[str, int],
    overrides: dict[str, int],
) -> tuple[float, float, float]:
    """Return (precision, recall, false_positive_rate)."""
    tp = fp = tn = fn = 0
    for wallet, label in labels.items():
        reasons = wallet_reasons.get(wallet, [])
        score = _score_wallet(reasons, default_weights, overrides)
        pred = 1 if score < SCAM_THRESHOLD else 0
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
    return precision, recall, fpr


def main() -> int:
    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    _CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT wallet FROM trust_scores WHERE wallet IS NOT NULL")
    db_wallets = {row[0] for row in cur.fetchall()}
    labels = _load_labels()
    wallet_reasons = _load_wallet_reasons(conn, cur)
    default_weights = _load_default_weights()

    # Wallets that have both labels and reasons
    labeled = {w for w in labels if w in wallet_reasons or w in db_wallets}
    if len(labeled) < 5:
        print("[optimize_reason_weights] WARNING: Few labeled wallets. Need scam_wallets.csv and manual_wallets.csv with overlapping trust_scores.")
        print(f"  Labeled: {len(labels)}, In DB: {len(db_wallets)}, With reasons: {len(wallet_reasons)}")
        conn.close()
        return 0

    code1, range1 = TUNABLE_CODES[0]
    code2, range2 = TUNABLE_CODES[1]

    best_f1 = -1.0
    best_overrides: dict[str, int] = {}
    best_metrics: tuple[float, float, float] = (0, 0, 0)
    results: list[dict] = []

    for w1 in range1:
        for w2 in range2:
            overrides = {code1: w1, code2: w2}
            prec, rec, fpr = _evaluate(wallet_reasons, labels, default_weights, overrides)
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
            results.append({
                code1: w1,
                code2: w2,
                "precision": prec,
                "recall": rec,
                "fpr": fpr,
                "f1": f1,
            })
            if f1 > best_f1:
                best_f1 = f1
                best_overrides = dict(overrides)
                best_metrics = (prec, rec, fpr)

    conn.close()

    prec, rec, fpr = best_metrics
    print()
    print("=" * 55)
    print("REASON WEIGHT OPTIMIZATION")
    print("=" * 55)
    print(f"Labeled wallets: {len(labels)}")
    print(f"Best F1: {best_f1:.4f}")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {rec:.4f}")
    print(f"  FPR:       {fpr:.4f}")
    print("Best weights:")
    for k, v in best_overrides.items():
        print(f"  {k} {v}")
    print("=" * 55)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["reason_code", "weight"])
        w.writeheader()
        for k, v in best_overrides.items():
            w.writerow({"reason_code": k, "weight": v})

    print(f"Saved: {OUTPUT_CSV}")

    # Plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np

        # 2D heatmap: code1 vs code2, color = F1
        r1_vals = sorted(set(r[code1] for r in results))
        r2_vals = sorted(set(r[code2] for r in results))
        z = np.zeros((len(r2_vals), len(r1_vals)))
        for r in results:
            i = r1_vals.index(r[code1])
            j = r2_vals.index(r[code2])
            z[j, i] = r["f1"]

        fig, ax = plt.subplots(figsize=(10, 8))
        im = ax.imshow(z, aspect="auto", cmap="viridis", vmin=0, vmax=1)
        ax.set_xticks(range(len(r1_vals)))
        ax.set_xticklabels(r1_vals)
        ax.set_yticks(range(len(r2_vals)))
        ax.set_yticklabels(r2_vals)
        ax.set_xlabel(code1)
        ax.set_ylabel(code2)
        ax.set_title("F1 Score vs Reason Weights")
        plt.colorbar(im, ax=ax, label="F1")
        plt.tight_layout()
        chart_path = _CHARTS_DIR / "reason_weight_tuning.png"
        plt.savefig(chart_path, dpi=100)
        plt.close()
        print(f"Chart: {chart_path}")
    except ImportError:
        print("[optimize_reason_weights] matplotlib not installed, skipping chart")

    return 0


if __name__ == "__main__":
    sys.exit(main())
