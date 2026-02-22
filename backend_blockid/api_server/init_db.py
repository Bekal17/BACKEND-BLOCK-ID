"""
Create BlockID wallet tracking database tables.

Usage:
    py -m backend_blockid.api_server.init_db
"""

from __future__ import annotations

import os
from pathlib import Path

# Load .env before importing db_wallet_tracking so DB_URL respects env vars
try:
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parents[2]
    load_dotenv(_root / ".env")
except Exception:
    pass

from backend_blockid.api_server.db_wallet_tracking import (
    Base,
    DB_URL,
    engine,
    init_db as wallet_tracking_init_db,
)

# Ensure SQLite file directory exists
if DB_URL.startswith("sqlite"):
    path = DB_URL.replace("sqlite:///", "").split("?")[0]
    if path:
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

print("DB URL:", DB_URL)
print("Creating BlockID DB tables...")
wallet_tracking_init_db()
print("Done.")
