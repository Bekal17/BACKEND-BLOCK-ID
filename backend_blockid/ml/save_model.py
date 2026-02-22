"""
Versioned model saving for BlockID ML pipeline.

Saves model (.joblib) and metadata (.json) with timestamp in filename.
Never overwrites existing models.

Usage:
    from backend_blockid.ml.save_model import save_model, load_latest_model
    save_model(model, "blockid_model", metrics={...}, feature_list=[...])
    model, metadata = load_latest_model()
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

_ML_DIR = Path(__file__).resolve().parent
_MODELS_DIR = _ML_DIR / "models"

logger = logging.getLogger(__name__)


def _timestamp() -> str:
    """Return YYYYMMDD_HHMM in UTC."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def save_model(
    model: Any,
    name: str,
    metrics: dict[str, Any] | None = None,
    feature_list: list[str] | None = None,
    *,
    models_dir: Path | None = None,
) -> tuple[Path, Path, str]:
    """
    Save model and metadata with versioned filenames. Does not overwrite.

    Args:
        model: sklearn-compatible model (joblib-serializable).
        name: Base name, e.g. "blockid_model", "token_scam_model", "trust_model".
        metrics: Dict with optional keys: accuracy, precision, recall, dataset_size.
        feature_list: Ordered feature names for inference reproducibility.
        models_dir: Override output directory (default: backend_blockid/ml/models/).

    Returns:
        (model_path, metadata_path, base_name) e.g. base_name="blockid_model_20260219_0348".

    Raises:
        OSError: On write failure.
    """
    models_dir = Path(models_dir or _MODELS_DIR)
    models_dir.mkdir(parents=True, exist_ok=True)

    ts = _timestamp()
    base = f"{name}_{ts}"
    model_path = models_dir / f"{base}.joblib"
    metadata_path = models_dir / f"{base}.json"

    joblib.dump(model, model_path)
    logger.info("model_saved", path=str(model_path))

    metrics = metrics or {}
    metadata = {
        "model_name": name,
        "training_date": datetime.now(timezone.utc).isoformat(),
        "feature_count": len(feature_list) if feature_list else None,
        "dataset_size": metrics.get("dataset_size"),
        "accuracy": metrics.get("accuracy"),
        "precision": metrics.get("precision"),
        "recall": metrics.get("recall"),
    }
    if feature_list is not None:
        metadata["feature_list"] = feature_list

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    logger.info("metadata_saved", path=str(metadata_path))

    return model_path, metadata_path, base


def load_latest_model(
    models_dir: Path | None = None,
) -> tuple[Any, dict[str, Any] | None]:
    """
    Load the newest .joblib model from backend_blockid/ml/models/ and its metadata.

    Skips *_scaler.joblib files. Sorts by filename (timestamp); newest first.
    Returns (model, metadata). metadata is the JSON dict; None if no metadata file.
    Returns (None, None) when folder is empty, missing, or has no .joblib files.
    """
    models_dir = Path(models_dir or _MODELS_DIR)
    if not models_dir.is_dir():
        logger.warning("load_latest_model_models_dir_missing", path=str(models_dir))
        return None, None

    candidates = [
        p for p in models_dir.glob("*.joblib")
        if p.is_file() and not p.stem.endswith("_scaler")
    ]
    if not candidates:
        logger.warning("load_latest_model_no_joblib", path=str(models_dir))
        return None, None

    latest = max(candidates, key=lambda p: p.name)
    try:
        model = joblib.load(latest)
    except Exception as e:
        logger.error("load_latest_model_load_failed", path=str(latest), error=str(e))
        return None, None

    metadata_path = latest.with_suffix(".json")
    metadata: dict[str, Any] | None = None
    if metadata_path.is_file():
        try:
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.warning("load_latest_model_metadata_failed", path=str(metadata_path), error=str(e))

    logger.info("load_latest_model_loaded", path=str(latest), has_metadata=metadata is not None)
    return model, metadata
