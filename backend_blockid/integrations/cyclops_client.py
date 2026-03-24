"""
Cyclops wallet risk analysis client — Daemon Protocol.
Docs: https://cyclops-api.daemonprotocol.com
"""
from __future__ import annotations
import os
import httpx
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

CYCLOPS_BASE = "https://cyclops-api.daemonprotocol.com"
CYCLOPS_API_KEY = (os.getenv("CYCLOPS_API_KEY") or "").strip()
CYCLOPS_TIMEOUT = float(os.getenv("CYCLOPS_TIMEOUT", "25"))

def _headers() -> dict:
    if CYCLOPS_API_KEY:
        return {"Authorization": f"Bearer {CYCLOPS_API_KEY}"}
    return {}

async def analyze_wallet(wallet: str, max_depth: int = 3) -> dict | None:
    """
    Analyze wallet risk via Cyclops.
    Returns simplified risk data or None if failed.
    """
    try:
        async with httpx.AsyncClient(timeout=CYCLOPS_TIMEOUT) as client:
            res = await client.get(
                f"{CYCLOPS_BASE}/api/v1/analyze/{wallet}",
                params={"maxDepth": max_depth},
                headers=_headers(),
            )
            if res.status_code == 200:
                data = res.json()
                if data.get("success") and data.get("data"):
                    d = data["data"]
                    sanctions = d.get("sanctions", {})
                    labels = d.get("labels", {})
                    graph_analysis = d.get("graphAnalysis", {})
                    return {
                        "risk_score": d.get("riskScore", 0),
                        "risk_level": d.get("riskLevel", "UNKNOWN"),
                        "risk_level_description": d.get("riskLevelDescription", ""),
                        "is_sanctioned": sanctions.get("isSanctioned", False),
                        "sanctions_programs": sanctions.get("programs", []),
                        "matched_entities": sanctions.get("matchedEntities", []),
                        "categories": labels.get("categories", []),
                        "attributes": labels.get("attributes", []),
                        "risk_indicators": labels.get("riskIndicators", []),
                        "entity": d.get("entity"),
                        "graph_score": graph_analysis.get("graphScore", 0),
                        "graph_confidence": graph_analysis.get("graphConfidence", 0),
                        "nodes_count": len(d.get("graph", {}).get("nodes", [])),
                        "edges_count": len(d.get("graph", {}).get("edges", [])),
                        "sources_queried": d.get("metadata", {}).get("sourcesQueried", []),
                    }
            logger.warning("cyclops_non_200", wallet=wallet[:16], status=res.status_code)
    except httpx.TimeoutException:
        logger.warning("cyclops_timeout", wallet=wallet[:16])
    except Exception as e:
        logger.warning("cyclops_error", wallet=wallet[:16], error=str(e))
    return None
