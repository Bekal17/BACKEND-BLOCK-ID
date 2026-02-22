"""
Merge multiple wallet label CSV datasets into a single master dataset.

Inputs (if present):
  - wallet_labels_auto.csv                 (typically at project root)
  - backend_blockid/data/scam_wallets.csv
  - backend_blockid/data/manual_wallets.csv

Rules:
  - Deduplicate by wallet
  - Scam labels override good labels
  - Unknown labels default to \"good\"
  - Master output: wallet_labels_master.csv (at project root)

Usage (from project root):

    python -m backend_blockid.tools.merge_wallet_datasets
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # Optional; safe to continue without .env
    pass

import psycopg2
from psycopg2.extras import RealDictCursor

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)


# Labels that are treated as \"scam\" for override logic
SCAM_LABELS = {
    "scam",
    "rug_pull_deployer",
    "rug-pull",
    "rugpull",
    "phishing_drain",
    "phishing",
    "malicious",
    "blacklist",
}


def _normalize_label(raw: str | None) -> Tuple[bool, str]:
    """
    Normalize label and determine if it's a scam.

    Returns (is_scam, normalized_label).
      - Scam labels: any value in SCAM_LABELS (case-insensitive) => (True, original or 'scam')
      - 'good' or empty/None => (False, 'good')
      - Unknown labels => (False, 'good')
    """
    if raw is None:
        return False, "good"
    s = raw.strip()
    if not s:
        return False, "good"
    low = s.lower()
    if low in SCAM_LABELS:
        # Preserve specific scam subtype when present, otherwise use generic 'scam'
        return True, s if low != "scam" else "scam"
    if low == "good":
        return False, "good"
    # Unknown label => default to good
    return False, "good"


def _read_label_csv(path: Path, source_name: str) -> List[Tuple[str, bool, str]]:
    """
    Read a wallet,label CSV and return list of (wallet, is_scam, normalized_label).
    """
    rows: List[Tuple[str, bool, str]] = []
    if not path.is_file():
        logger.info("merge_dataset_input_missing", source=source_name, path=str(path))
        return rows
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if "wallet" not in (reader.fieldnames or []):
                logger.warning("merge_dataset_missing_wallet_column", source=source_name, path=str(path))
                return rows
            for row in reader:
                wallet = (row.get("wallet") or "").strip()
                if not wallet:
                    continue
                raw_label = row.get("label")
                is_scam, norm_label = _normalize_label(raw_label)
                rows.append((wallet, is_scam, norm_label))
    except Exception as e:  # noqa: BLE001
        logger.exception("merge_dataset_read_failed", source=source_name, path=str(path), error=str(e))
        return []

    logger.info(
        "merge_dataset_input_loaded",
        source=source_name,
        path=str(path),
        rows=len(rows),
    )
    return rows


def load_scam_wallets_from_db() -> List[Tuple[str, str]]:
    """
    Load scam wallets from Postgres scam_wallets table using DATABASE_URL.

    Table schema:
        scam_wallets(wallet TEXT, label TEXT)

    Returns:
        List of (wallet, label) with first label per wallet winning (duplicates ignored).
    """
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        logger.warning("merge_dataset_db_url_missing", message="DATABASE_URL not set; skipping DB scam wallets")
        return []

    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:  # noqa: BLE001
        logger.warning("merge_dataset_db_connect_failed", error=str(e))
        return []

    seen: Dict[str, str] = {}
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT wallet, label FROM scam_wallets;")
            for row in cur.fetchall():
                wallet = (row.get("wallet") or "").strip()
                if not wallet or wallet in seen:
                    continue
                label = (row.get("label") or "scam").strip() or "scam"
                seen[wallet] = label
    except Exception as e:  # noqa: BLE001
        logger.warning("merge_dataset_db_query_failed", error=str(e))
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    scams = [(w, lbl) for w, lbl in seen.items()]
    logger.info("scam_wallets_loaded_from_db", count=len(scams))
    return scams


def _merge_datasets(
    auto_rows: Iterable[Tuple[str, bool, str]],
    manual_rows: Iterable[Tuple[str, bool, str]],
    scam_rows: Iterable[Tuple[str, bool, str]],
) -> Dict[str, str]:
    """
    Merge three sources into a single mapping: wallet -> label.

    Precedence:
      - Scam labels override good labels (from any source)
      - When both are good, later sources may overwrite but result is 'good'
      - Unknown labels are normalized to 'good' before merge

    Merge order:
      1. auto (baseline)
      2. manual
      3. scam (last to ensure override)
    """
    merged: Dict[str, Tuple[bool, str]] = {}

    def apply_rows(rows: Iterable[Tuple[str, bool, str]], source: str) -> None:
        for wallet, is_scam, label in rows:
            current = merged.get(wallet)
            if current is None:
                merged[wallet] = (is_scam, label)
                continue
            cur_is_scam, cur_label = current
            if cur_is_scam:
                # Never downgrade a scam wallet to good
                continue
            if is_scam:
                # Upgrade from good to scam
                merged[wallet] = (True, label)
            else:
                # Both good -> override label (still 'good')
                merged[wallet] = (False, label)

    apply_rows(auto_rows, "auto")
    apply_rows(manual_rows, "manual")
    apply_rows(scam_rows, "scam")

    logger.info(
        "merge_dataset_merge_done",
        total_wallets=len(merged),
        scam_wallets=sum(1 for v in merged.values() if v[0]),
    )

    # Strip to wallet -> label
    return {w: lbl for w, (_is_scam, lbl) in merged.items()}


def _save_master(path: Path, labels: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "label"])
        for wallet, label in labels.items():
            writer.writerow([wallet, label])
    logger.info(
        "merge_dataset_master_saved",
        path=str(path),
        total_wallets=len(labels),
        scam_wallets=sum(1 for lbl in labels.values() if _normalize_label(lbl)[0]),
    )


def main() -> int:
    # Project root = two levels above this file (backend_blockid/tools/..)
    project_root = Path(__file__).resolve().parents[2]

    auto_path = project_root / "wallet_labels_auto.csv"
    manual_path = project_root / "backend_blockid" / "data" / "manual_wallets.csv"
    master_path = project_root / "wallet_labels_master.csv"

    import argparse

    parser = argparse.ArgumentParser(
        description="Merge wallet label datasets (auto, manual, DB scam) into wallet_labels_master.csv",
    )
    parser.add_argument(
        "--use-db-scam",
        action="store_true",
        default=True,
        help="Use scam_wallets from Postgres DATABASE_URL (default: True)",
    )
    args, _ = parser.parse_known_args()

    scam_csv_path = project_root / "backend_blockid" / "data" / "scam_wallets.csv"

    logger.info(
        "merge_dataset_start",
        auto=str(auto_path),
        manual=str(manual_path),
        scam_csv=str(scam_csv_path),
        use_db_scam=args.use_db_scam,
        master=str(master_path),
    )

    auto_rows = _read_label_csv(auto_path, "auto")
    logger.info("auto_wallets_loaded", count=len(auto_rows))

    manual_rows = _read_label_csv(manual_path, "manual")
    logger.info("manual_wallets_loaded", count=len(manual_rows))

    # Load scam wallets from CSV (if present)
    scam_csv_rows = _read_label_csv(scam_csv_path, "scam_csv")
    logger.info("scam_wallets_loaded_from_csv", count=len(scam_csv_rows))

    if args.use_db_scam:
        db_scams = load_scam_wallets_from_db()
        scam_rows_db = [(w, True, lbl) for w, lbl in db_scams]
    else:
        scam_rows_db = []
    logger.info("scam_wallets_loaded_from_db", count=len(scam_rows_db))

    # Combine CSV and DB scam rows; order matters (CSV first, DB then overrides)
    scam_rows: List[Tuple[str, bool, str]] = []
    scam_rows.extend(scam_csv_rows)
    scam_rows.extend(scam_rows_db)

    if not auto_rows and not manual_rows and not scam_rows:
        logger.warning("merge_dataset_no_inputs", message="No input datasets found; nothing to merge")
        print("No input datasets found; nothing to merge.", file=sys.stderr)
        return 1

    merged_labels = _merge_datasets(auto_rows, manual_rows, scam_rows)
    _save_master(master_path, merged_labels)

    print(f"Merged master dataset written to: {master_path}")
    print(f"Total wallets: {len(merged_labels)}")
    return 0


if __name__ == "__main__":
    # Simple DB test when running this file directly
    try:
        scams = load_scam_wallets_from_db()
        print("Loaded scam wallets:", len(scams))
    except Exception as exc:  # noqa: BLE001
        print("Loaded scam wallets: 0 (error:", exc, ")", file=sys.stderr)
    # Still allow CLI-style merging if desired
    raise SystemExit(main())


