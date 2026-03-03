"""
Database configuration for BlockID (SQLite).
DB_PATH env overrides for Docker/production.
"""
from __future__ import annotations

import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(_PROJECT_ROOT / "blockid.db"))).resolve()
