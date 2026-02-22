"""
BlockID API Python client example.

Uses the requests library. Generated from FastAPI OpenAPI schema.
Run: pip install requests

Usage:
    from docs.python_sdk_example import BlockIDClient
    client = BlockIDClient("http://localhost:8000")
    score = client.get_trust_score("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests


class BlockIDClientError(Exception):
    """Raised when the API returns an error response."""

    def __init__(self, message: str, status_code: int | None = None, response: requests.Response | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class BlockIDClient:
    """Client for the BlockID trust score API."""

    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        files: dict[str, tuple[str, bytes] | tuple[str, bytes, str]] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self._session.request(
            method, url, params=params, json=json, files=files, timeout=self.timeout
        )
        if not resp.ok:
            detail = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            raise BlockIDClientError(
                f"API error: {detail}",
                status_code=resp.status_code,
                response=resp,
            )
        return resp

    def get_trust_score(self, wallet: str) -> dict[str, Any]:
        """Get trust score for a wallet."""
        r = self._request("GET", f"/api/trust-score/{wallet}")
        return r.json()

    def list_trust_scores(self, wallets: list[str]) -> list[dict[str, Any]]:
        """Batch fetch trust scores for multiple wallets (max 100)."""
        if len(wallets) > 100:
            raise ValueError("max 100 wallets per request")
        r = self._request("POST", "/api/trust-score/list", json={"wallets": wallets})
        return r.json()

    def get_wallet(self, address: str) -> dict[str, Any]:
        """Get wallet trust score and flags (legacy endpoint)."""
        r = self._request("GET", f"/wallet/{address}")
        return r.json()

    def health(self) -> dict[str, str]:
        """Liveness probe."""
        r = self._request("GET", "/health")
        return r.json()

    def track_wallet(self, wallet: str) -> dict[str, Any]:
        """Register wallet for monitoring (main scheduler)."""
        r = self._request("POST", "/track-wallet", json={"wallet": wallet})
        return r.json()

    def track_wallet_step2(self, wallet: str, label: str | None = None) -> dict[str, Any]:
        """Add wallet to Step 2 tracking with optional label."""
        body: dict[str, Any] = {"wallet": wallet}
        if label is not None:
            body["label"] = label
        r = self._request("POST", "/track_wallet", json=body)
        return r.json()

    def get_tracked_wallets(self) -> list[dict[str, Any]]:
        """List all tracked wallets."""
        r = self._request("GET", "/tracked_wallets")
        return r.json()

    def import_wallets_csv(self, csv_path: str | Path) -> dict[str, Any]:
        """Import wallets from CSV. Expected columns: wallet, label (optional)."""
        path = Path(csv_path)
        if not path.is_file():
            raise FileNotFoundError(f"CSV not found: {path}")
        with open(path, "rb") as f:
            files = {"file": (path.name, f.read(), "text/csv")}
            r = self._request("POST", "/import_wallets_csv", files=files)
        return r.json()

    def get_wallet_report(self, wallet: str) -> dict[str, Any]:
        """Run analytics pipeline for wallet (no publish)."""
        r = self._request("GET", f"/wallet_report/{wallet}")
        return r.json()

    def debug_wallet_status(self, wallet: str) -> dict[str, Any]:
        """Debug: check if wallet is tracked and PDA exists on-chain."""
        r = self._request("GET", f"/debug/wallet_status/{wallet}")
        return r.json()


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    client = BlockIDClient("http://localhost:8000")

    # Health check
    print("Health:", client.health())

    # Single trust score
    try:
        score = client.get_trust_score("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")
        print("Trust score:", score.get("score"), "reason_codes:", score.get("reason_codes"))
    except BlockIDClientError as e:
        if e.status_code == 404:
            print("Wallet not scored")
        else:
            raise

    # Batch trust scores
    batch = client.list_trust_scores(["addr1", "addr2"])
    print("Batch:", len(batch), "results")

    # Track wallet
    result = client.track_wallet_step2("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", label="test")
    print("Tracked:", result.get("registered"))

    # List tracked
    wallets = client.get_tracked_wallets()
    print("Tracked count:", len(wallets))
