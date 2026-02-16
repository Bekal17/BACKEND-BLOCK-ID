"""
Pytest fixtures for BlockID tests. Uses temporary SQLite DB for wallet tracking.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def wallet_tracking_db(tmp_path, monkeypatch):
    """
    Point wallet tracking at a temporary SQLite DB and init tables.
    Resets engine cache so each test gets a fresh DB. Unset DATABASE_URL so we use SQLite.
    """
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("WALLET_TRACKING_DB_PATH", str(tmp_path / "wallet_tracking.db"))

    import backend_blockid.api_server.db_wallet_tracking as db

    db.reset_engine_for_test()
    db.init_db()
    return db


@pytest.fixture
def client(wallet_tracking_db):
    """FastAPI TestClient. Depends on wallet_tracking_db so temp DB is set before app runs."""
    from fastapi.testclient import TestClient

    from backend_blockid.api_server.server import app

    return TestClient(app)
