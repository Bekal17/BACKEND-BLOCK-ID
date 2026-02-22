"""
Dataset builder for BlockID ML Trust Model.

Reads a CSV of (wallet, label), runs analytics + feature extraction per wallet,
and writes a training CSV with normalized features and binary label (good=1, scam=0).
Invalid wallets and unknown labels are skipped; summary is logged.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.feature_builder import build_features, get_feature_names

logger = get_logger(__name__)

LABEL_GOOD = 1
LABEL_SCAM = 0


def _parse_label(raw: str) -> int | None:
    """Map 'good' -> 1, 'scam' -> 0; return None for unknown."""
    s = (raw or "").strip().lower()
    if s == "good":
        return LABEL_GOOD
    if s == "scam":
        return LABEL_SCAM
    return None


def build_dataset(
    input_path: Path,
    output_path: Path,
) -> tuple[int, int, int]:
    """
    Read wallet,label CSV; run analytics + build_features per row; write f1..fN,label CSV.

    Returns (valid_count, skipped_count, error_count).
    """
    names = get_feature_names()
    n_features = len(names)
    header = [f"f{i+1}" for i in range(n_features)] + ["label"]

    valid_count = 0
    skipped_count = 0
    error_count = 0
    rows_out: list[list[float | int]] = []

    if not input_path.is_file():
        logger.error("build_dataset_input_missing", path=str(input_path))
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "wallet" not in (reader.fieldnames or []):
            raise ValueError("Input CSV must have a 'wallet' column")
        if "label" not in (reader.fieldnames or []):
            raise ValueError("Input CSV must have a 'label' column")
        for row in reader:
            wallet = (row.get("wallet") or "").strip()
            if not wallet:
                error_count += 1
                logger.warning("build_dataset_empty_wallet", row=row)
                continue
            label_val = _parse_label(row.get("label") or "")
            if label_val is None:
                error_count += 1
                logger.warning("build_dataset_unknown_label", wallet=wallet, label=row.get("label"))
                continue
            try:
                data = run_wallet_analysis(wallet)
                feats = build_features(data)
            except ValueError as e:
                skipped_count += 1
                logger.debug("build_dataset_skip_invalid", wallet=wallet, error=str(e))
                continue
            except Exception as e:
                error_count += 1
                logger.warning("build_dataset_error", wallet=wallet, error=str(e))
                continue
            if len(feats) != n_features:
                error_count += 1
                logger.warning("build_dataset_feature_mismatch", wallet=wallet, len_feats=len(feats))
                continue
            rows_out.append(feats.tolist() + [label_val])
            valid_count += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_out)

    logger.info(
        "build_dataset_done",
        output=str(output_path),
        valid=valid_count,
        skipped=skipped_count,
        errors=error_count,
    )
    return valid_count, skipped_count, error_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build ML training dataset from wallet,label CSV (good/scam).",
    )
    parser.add_argument(
        "input_csv",
        type=Path,
        help="Input CSV with columns: wallet, label (good|scam)",
    )
    parser.add_argument(
        "output_csv",
        type=Path,
        help="Output CSV with columns: f1..f9, label (1=good, 0=scam)",
    )
    args = parser.parse_args()
    try:
        valid, skipped, err = build_dataset(args.input_csv, args.output_csv)
    except (FileNotFoundError, ValueError) as e:
        print("ERROR:", e, file=sys.stderr)
        return 1
    print("SUMMARY: valid wallets", valid, "| skipped (invalid)", skipped, "| errors", err)
    return 0


if __name__ == "__main__":
    sys.exit(main())
