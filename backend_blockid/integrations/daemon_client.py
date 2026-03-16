"""
Daemon Protocol client for BlockID.

Fetches risk score and sanctions data for a given wallet address.
Used as input layer for trust_engine.py.

Endpoint: GET /v1/risk/{wallet_address}
Docs: daemonprotocol.com/products
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

DAEMON_BASE_URL = os.getenv("DAEMON_API_URL", "https://api.daemonprotocol.com").rstrip("/")
DAEMON_API_KEY = os.getenv("DAEMON_API_KEY", "")
DAEMON_TIMEOUT = float(os.getenv("DAEMON_TIMEOUT_SEC", "10"))


class DaemonResult:
    """Structured result from Daemon Protocol API."""

    def __init__(
        self,
        wallet: str,
        risk_score: int,           # 0-100 from Daemon
        risk_level: str,           # LOW / MEDIUM / HIGH / CRITICAL
        risk_description: str,
        is_sanctioned: bool,       # OFAC, EU, global watchlist
        sanction_programs: list[str],
        matched_entities: list[str],
        labels: list[str],         # e.g. ["GOVERNMENT", "EXCHANGE"]
        raw: dict[str, Any],
    ) -> None:
        self.wallet = wallet
        self.risk_score = risk_score
        self.risk_level = risk_level
        self.risk_description = risk_description
        self.is_sanctioned = is_sanctioned
        self.sanction_programs = sanction_programs
        self.matched_entities = matched_entities
        self.labels = labels
        self.raw = raw

    @property
    def is_critical(self) -> bool:
        return self.risk_level == "CRITICAL" or self.risk_score >= 90

    @property
    def penalty_score(self) -> int:
        """
        Convert Daemon risk_score (0-100) to a penalty for trust_engine.
        Daemon 100 → penalty 40, Daemon 0 → penalty 0. Linear scale.
        Sanctioned wallet gets additional flat penalty of 30.
        """
        base_penalty = int(self.risk_score * 0.4)
        sanction_penalty = 30 if self.is_sanctioned else 0
        return min(70, base_penalty + sanction_penalty)


def _parse_response(wallet: str, data: dict[str, Any]) -> DaemonResult:
    """Parse raw Daemon API response into DaemonResult."""
    inner = data.get("data") or data  # handle both {success, data} and flat response
    sanctions = inner.get("sanctions") or {}
    labels_raw = inner.get("labels") or {}
    categories = labels_raw.get("categories") or []

    return DaemonResult(
        wallet=wallet,
        risk_score=int(inner.get("riskScore") or 0),
        risk_level=str(inner.get("riskLevel") or "LOW").upper(),
        risk_description=str(inner.get("riskLevelDescription") or ""),
        is_sanctioned=bool(sanctions.get("isSanctioned") or False),
        sanction_programs=list(sanctions.get("programs") or []),
        matched_entities=list(sanctions.get("matchedEntities") or []),
        labels=list(categories),
        raw=data,
    )


def get_wallet_risk(wallet: str) -> DaemonResult | None:
    """
    Fetch wallet risk data from Daemon Protocol.

    Returns DaemonResult on success, None on failure (non-blocking).
    Failure is logged but never raises — trust_engine degrades gracefully.
    """
    if not DAEMON_API_KEY:
        logger.warning("daemon_client_no_api_key", wallet=wallet[:16])
        return None

    url = f"{DAEMON_BASE_URL}/v1/risk/{wallet}"
    headers = {
        "Authorization": f"Bearer {DAEMON_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        with httpx.Client(timeout=DAEMON_TIMEOUT) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

        result = _parse_response(wallet, data)
        logger.info(
            "daemon_client_success",
            wallet=wallet[:16] + "...",
            risk_score=result.risk_score,
            risk_level=result.risk_level,
            is_sanctioned=result.is_sanctioned,
            is_critical=result.is_critical,
        )
        return result

    except httpx.TimeoutException:
        logger.warning("daemon_client_timeout", wallet=wallet[:16], url=url)
        return None
    except httpx.HTTPStatusError as e:
        logger.warning(
            "daemon_client_http_error",
            wallet=wallet[:16],
            status_code=e.response.status_code,
        )
        return None
    except Exception as e:
        logger.error("daemon_client_unexpected_error", wallet=wallet[:16], error=str(e))
        return None
