"""
Reason weight engine — convert reason_codes into score penalties per wallet.

Delegates to ml.reason_weight_engine.

Usage:
  py -m backend_blockid.oracle.reason_weight_engine
"""

from __future__ import annotations

from backend_blockid.ml.reason_weight_engine import main


if __name__ == "__main__":
    raise SystemExit(main())
