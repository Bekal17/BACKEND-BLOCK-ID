"""
Dataset analysis tool for BlockID ML Trust Model.

Loads a training CSV (f1..fN, label), computes stats, and prints a report
with class distribution, feature statistics, and actionable warnings
(class imbalance, zero variance, missing values).
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import numpy as np

# Label display names for report (numeric label -> string)
LABEL_NAMES = {0: "scam", 1: "good", 2: "suspicious"}

# Thresholds for warnings
IMBALANCE_RATIO = 2.0       # warn if max_class / min_class > this
MISSING_PCT_WARN = 10.0     # warn if > 10% NaN
ZERO_VAR_EPS = 1e-9         # treat std < this as zero variance


def _parse_float(x: str) -> float | None:
    s = (x or "").strip()
    if s.lower() in ("", "nan", "na"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_dataset(path: Path) -> tuple[list[str], np.ndarray, np.ndarray]:
    """
    Load CSV with feature columns (f1, f2, ...) and label column.
    Returns (feature_column_names, X, y).
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError("CSV has no header")
        rows = list(reader)

    # Last column = label; rest = feature columns
    label_col_idx = len(header) - 1
    feat_cols = [c.strip() for c in header[:-1]]
    n_features = len(feat_cols)
    if n_features == 0:
        raise ValueError("No feature columns found")

    X_list: list[list[float]] = []
    y_list: list[float] = []
    for row in rows:
        if len(row) <= label_col_idx:
            continue
        try:
            label_val = float(row[label_col_idx].strip())
        except ValueError:
            continue
        feats: list[float] = []
        for i in range(n_features):
            val = _parse_float(row[i]) if i < len(row) else None
            feats.append(val if val is not None else np.nan)
        X_list.append(feats)
        y_list.append(label_val)

    X = np.array(X_list, dtype=np.float64)
    y = np.array(y_list, dtype=np.float64)
    return feat_cols, X, y


def analyze(
    path: Path,
) -> None:
    """Load dataset, compute stats, print report and warnings."""
    if not path.is_file():
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        sys.exit(1)
    try:
        feat_cols, X, y = load_dataset(path)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    n_wallets = len(y)
    n_features = X.shape[1]
    unique_labels = sorted(np.unique(y[~np.isnan(y)].astype(int)))

    # Class distribution
    class_counts: dict[int, int] = {}
    for k in unique_labels:
        class_counts[k] = int(np.sum(y == k))
    class_names = [LABEL_NAMES.get(k, f"label_{k}") for k in unique_labels]

    # Feature stats
    means = np.nanmean(X, axis=0)
    stds = np.nanstd(X, axis=0)
    mins = np.nanmin(X, axis=0)
    maxs = np.nanmax(X, axis=0)
    nan_counts = np.isnan(X).sum(axis=0)
    nan_pcts = (nan_counts / n_wallets * 100) if n_wallets else np.zeros(n_features)

    # --- Report ---
    print()
    print("DATASET REPORT")
    print("--------------")
    print(f"wallets: {n_wallets}")
    for k, name in zip(unique_labels, class_names):
        print(f"{name}: {class_counts[k]}")
    print()

    print("Feature statistics:")
    print("-" * 60)
    warnings: list[str] = []
    for i in range(n_features):
        col = feat_cols[i] if i < len(feat_cols) else f"f{i+1}"
        std_val = float(stds[i])
        mean_val = float(means[i])
        min_val = float(mins[i])
        max_val = float(maxs[i])
        n_nan = int(nan_counts[i])
        pct_nan = float(nan_pcts[i])

        if np.isnan(std_val) or std_val < ZERO_VAR_EPS:
            status = " -> BAD (zero variance or all NaN)"
            warnings.append(f"Zero variance: {col} has no variation; consider dropping or collecting more data.")
        else:
            status = " -> OK"
        print(f"{col}  mean={mean_val:.4f}  std={std_val:.4f}  min={min_val:.4f}  max={max_val:.4f}  NaN={n_nan} ({pct_nan:.1f}%){status}")

        if pct_nan > MISSING_PCT_WARN:
            warnings.append(f"Too many missing values: {col} has {pct_nan:.1f}% NaN (>{MISSING_PCT_WARN}%). Impute or drop.")

    print()
    print("Problem checks:")
    print("-" * 60)

    # Class imbalance
    if len(class_counts) >= 2:
        counts = list(class_counts.values())
        max_c, min_c = max(counts), min(counts)
        if min_c == 0:
            warnings.append("Severe class imbalance: at least one class has 0 samples. Add data or use a different split.")
            print("class imbalance: BAD (one class has 0 samples)")
        elif max_c / min_c > IMBALANCE_RATIO:
            ratio = max_c / min_c
            warnings.append(f"Class imbalance: max/min = {ratio:.1f}. Consider oversampling minority class or using class_weight.")
            print(f"class imbalance: WARN (max/min = {ratio:.1f})")
        else:
            print("class imbalance: OK")
    else:
        print("class imbalance: N/A (single class)")

    zero_var_cols = [
        feat_cols[i] if i < len(feat_cols) else f"f{i+1}"
        for i in range(n_features)
        if np.isnan(stds[i]) or stds[i] < ZERO_VAR_EPS
    ]
    if zero_var_cols:
        print(f"zero variance features: BAD -> {zero_var_cols}")
    else:
        print("zero variance features: OK")

    high_missing = [feat_cols[i] if i < len(feat_cols) else f"f{i+1}" for i in range(n_features) if nan_pcts[i] > MISSING_PCT_WARN]
    if high_missing:
        print(f"too many missing values: WARN -> {high_missing}")
    else:
        print("too many missing values: OK")

    if warnings:
        print()
        print("Actionable warnings:")
        print("-" * 60)
        for w in warnings:
            print(" -", w)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze ML training dataset (feature stats, class distribution, problems).",
    )
    parser.add_argument(
        "dataset_csv",
        type=Path,
        help="Path to dataset CSV (e.g. f1..f9,label)",
    )
    args = parser.parse_args()
    analyze(args.dataset_csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
