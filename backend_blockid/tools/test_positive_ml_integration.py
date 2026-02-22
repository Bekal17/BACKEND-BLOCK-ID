"""
Test positive reason integration in ML scoring.

Usage:
  py -m backend_blockid.tools.test_positive_ml_integration WALLET
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

from backend_blockid.ai_engine.positive_reasons import default_positive_reason
from backend_blockid.database.repositories import insert_wallet_reason, get_wallet_reasons
from backend_blockid.ml.predict_wallet_score import (
    _feature_vector_from_tokens,
    _get_token_history_mock,
)
from backend_blockid.ml.predictor import score_to_risk_label


def main() -> int:
    wallet = sys.argv[1] if len(sys.argv) > 1 else ""
    if not wallet:
        print("[test_positive_ml_integration] ERROR: wallet required")
        return 1

    # Mock score computation (same path as predict_wallet_score)
    tokens = _get_token_history_mock(wallet)
    X = _feature_vector_from_tokens(tokens)
    # Dummy baseline score for test (safe wallet)
    base_score = 90
    risk_level = score_to_risk_label(base_score)

    print(f"[test_positive_ml_integration] before score={base_score} risk={risk_level}")

    existing = get_wallet_reasons(wallet)
    if not existing:
        positive = default_positive_reason()
        insert_wallet_reason(
            wallet,
            positive["code"],
            positive["weight"],
            confidence=positive["confidence"],
            tx_hash=None,
            tx_link=None,
        )
        base_score = min(100, max(0, base_score + positive["weight"]))
        risk_level = score_to_risk_label(base_score)

    reasons = get_wallet_reasons(wallet)
    print(f"[test_positive_ml_integration] after score={base_score} risk={risk_level}")
    print(json.dumps({"wallet": wallet, "reasons": reasons}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
