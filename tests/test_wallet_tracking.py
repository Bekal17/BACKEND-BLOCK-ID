"""
Pytest tests for BlockID wallet tracking (db_wallet_tracking + FastAPI endpoints).

Uses temporary SQLite DB via conftest fixtures. Mocks publish_wallet where needed.
"""

from __future__ import annotations

import io

import pytest

# Valid Solana pubkey (base58, 32 bytes)
VALID_WALLET = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"
VALID_WALLET_2 = "7F1WzVNQ1Qpurqxxdyv3UrFQR3uoNepULVW9A4bAJ5nZ"


def test_track_wallet_inserts_db(client, wallet_tracking_db):
    """POST /track-wallet inserts wallet into DB; second call is duplicate and ignored."""
    # First call: insert
    r1 = client.post("/track-wallet", json={"wallet": VALID_WALLET})
    assert r1.status_code == 201
    data1 = r1.json()
    assert data1["wallet"] == VALID_WALLET
    assert data1["registered"] is True
    # DB contains wallet
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 1
    assert wallets[0]["wallet"] == VALID_WALLET
    # Second call: duplicate ignored
    r2 = client.post("/track-wallet", json={"wallet": VALID_WALLET})
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["wallet"] == VALID_WALLET
    assert data2["registered"] is False
    # Still only one row
    wallets2 = wallet_tracking_db.list_wallets()
    assert len(wallets2) == 1


def test_add_wallet_valid(wallet_tracking_db):
    """Adding a valid Solana wallet returns True and wallet appears in list."""
    added = wallet_tracking_db.add_wallet(VALID_WALLET, "my wallet")
    assert added is True
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 1
    assert wallets[0]["wallet"] == VALID_WALLET
    assert wallets[0]["label"] == "my wallet"
    assert wallets[0]["is_active"] is True
    assert wallets[0]["last_score"] is None


def test_add_wallet_invalid(wallet_tracking_db):
    """Adding an invalid wallet raises ValueError."""
    with pytest.raises(ValueError, match="Invalid Solana wallet"):
        wallet_tracking_db.add_wallet("not-a-valid-pubkey")
    with pytest.raises(ValueError, match="non-empty"):
        wallet_tracking_db.add_wallet("")
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 0


def test_list_wallets(wallet_tracking_db):
    """List returns all tracked wallets in order."""
    wallet_tracking_db.add_wallet(VALID_WALLET, "first")
    wallet_tracking_db.add_wallet(VALID_WALLET_2, "second")
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 2
    assert wallets[0]["wallet"] == VALID_WALLET
    assert wallets[0]["label"] == "first"
    assert wallets[1]["wallet"] == VALID_WALLET_2
    assert wallets[1]["label"] == "second"


def test_update_score(wallet_tracking_db):
    """update_wallet_score updates last_score/last_risk and they appear in list_wallets."""
    wallet_tracking_db.add_wallet(VALID_WALLET)
    wallet_tracking_db.update_wallet_score(VALID_WALLET, 88, "1")
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 1
    assert wallets[0]["last_score"] == 88
    assert wallets[0]["last_risk"] == "1"
    assert wallets[0]["last_checked"] is not None
    # Second update
    wallet_tracking_db.update_wallet_score(VALID_WALLET, 90, "0")
    wallets = wallet_tracking_db.list_wallets()
    assert wallets[0]["last_score"] == 90
    assert wallets[0]["last_risk"] == "0"


def test_update_score_with_reason_codes(wallet_tracking_db):
    """update_wallet_score with reason_codes stores JSON list; get_wallet_info returns it."""
    wallet_tracking_db.add_wallet(VALID_WALLET)
    wallet_tracking_db.update_wallet_score(
        VALID_WALLET, 70, "LOW", reason_codes=["NEW_WALLET", "LOW_ACTIVITY"]
    )
    info = wallet_tracking_db.get_wallet_info(VALID_WALLET)
    assert info is not None
    assert info["last_score"] == 70
    assert info["reason_codes"] is not None
    import json
    codes = json.loads(info["reason_codes"]) if isinstance(info["reason_codes"], str) else info["reason_codes"]
    assert codes == ["NEW_WALLET", "LOW_ACTIVITY"]


def test_csv_import(client):
    """POST /import_wallets_csv imports wallet,label CSV and returns imported/duplicates/invalid."""
    csv_content = "wallet,label\n"
    csv_content += f"{VALID_WALLET},Main\n"
    csv_content += f"{VALID_WALLET_2},Secondary\n"
    csv_content += "bad-pubkey,Invalid\n"
    files = {"file": ("wallets.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv")}
    response = client.post("/import_wallets_csv", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["imported"] == 2
    assert data["duplicates"] == 0
    assert "bad-pubkey" in data["invalid"]
    # GET tracked_wallets should show the two valid ones
    list_resp = client.get("/tracked_wallets")
    assert list_resp.status_code == 200
    wallets = list_resp.json()
    assert len(wallets) == 2
    wallets_by_addr = {w["wallet"]: w for w in wallets}
    assert VALID_WALLET in wallets_by_addr
    assert VALID_WALLET_2 in wallets_by_addr
    assert wallets_by_addr[VALID_WALLET]["label"] == "Main"


def test_run_batch_once_mocked(wallet_tracking_db, monkeypatch):
    """run_batch_once loads from DB and calls update_wallet_score on success when publish is mocked."""
    wallet_tracking_db.add_wallet(VALID_WALLET)
    wallet_tracking_db.update_wallet_score(VALID_WALLET, 50, "0")  # so last_score exists
    success_count = 0

    def fake_publish(
        wallet: str,
        score: int,
        risk_level: int | None = None,
    ) -> tuple[bool, int | None, int | None]:
        nonlocal success_count
        success_count += 1
        return True, 88, 1  # success, stored_score, stored_risk

    import batch_publish as bp

    monkeypatch.setattr(bp, "_publish_wallet", fake_publish)
    ok, fail = bp.run_batch_once()
    assert ok == 1
    assert fail == 0
    assert success_count == 1
    wallets = wallet_tracking_db.list_wallets()
    assert len(wallets) == 1
    assert wallets[0]["last_score"] == 88
    # batch_publish stores risk_label from analytics (e.g. MEDIUM)
    assert wallets[0]["last_risk"] in ("1", "MEDIUM")
