"""
Wallet graph and cluster detection for BlockID analytics.

Scans recent transactions, collects counterparty wallets, builds a simple
interaction graph, and assigns cluster_id / cluster_risk (HIGH if any node
is in the scam_wallets blacklist). Used by the analytics pipeline and trust engine.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

MAX_TXS_TO_SCAN = 30
DEFAULT_SCAM_WALLETS_PATH = Path(__file__).resolve().parent.parent / "oracle" / "scam_wallets.json"

# Well-known program IDs to exclude from counterparty set (not wallets)
KNOWN_PROGRAM_IDS = frozenset({
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "MetaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
})


def _load_scam_wallets() -> set[str]:
    """Load known scam wallet addresses from JSON. Returns empty set on failure."""
    path_str = os.getenv("SCAM_WALLETS_PATH", "").strip() or str(DEFAULT_SCAM_WALLETS_PATH)
    path = Path(path_str)
    if not path.is_file():
        logger.debug("wallet_graph_scam_wallets_missing", path=path_str)
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("wallet_graph_scam_wallets_load_failed", path=path_str, error=str(e))
        return set()
    if not isinstance(data, list):
        return set()
    return {str(w).strip() for w in data if w}


def _get_resp_value(resp: Any) -> Any:
    if resp is None:
        return None
    v = getattr(resp, "value", None)
    if v is not None:
        return v
    if hasattr(resp, "result"):
        return getattr(resp.result, "value", None)
    return None


def _account_keys_from_tx(tx_value: Any) -> list[str]:
    """Extract account keys from tx.value (transaction.message.accountKeys + loadedAddresses)."""
    out: list[str] = []
    if tx_value is None:
        return out
    try:
        tx_obj = getattr(tx_value, "transaction", None) or (tx_value.get("transaction") if isinstance(tx_value, dict) else None)
        if tx_obj is None:
            return out
        msg = getattr(tx_obj, "message", None) or (tx_obj.get("message") if isinstance(tx_obj, dict) else None)
        if msg is None:
            return out
        keys = getattr(msg, "account_keys", None) or getattr(msg, "accountKeys", None)
        if keys is None and isinstance(msg, dict):
            keys = msg.get("account_keys") or msg.get("accountKeys")
        if keys is not None:
            for k in keys:
                if isinstance(k, str) and k:
                    out.append(k)
                elif isinstance(k, dict):
                    pk = k.get("pubkey")
                    if pk is not None:
                        out.append(str(pk))
                elif hasattr(k, "pubkey"):
                    out.append(str(getattr(k, "pubkey")))
        meta = getattr(tx_value, "meta", None) or (tx_value.get("meta") if isinstance(tx_value, dict) else None)
        if meta is not None:
            loaded = getattr(meta, "loaded_addresses", None) or getattr(meta, "loadedAddresses", None)
            if loaded is None and isinstance(meta, dict):
                loaded = meta.get("loaded_addresses") or meta.get("loadedAddresses")
            if isinstance(loaded, dict):
                for role in ("writable", "readonly"):
                    arr = loaded.get(role) or []
                    for addr in arr:
                        if isinstance(addr, str) and addr:
                            out.append(addr)
                        elif isinstance(addr, dict):
                            pk = addr.get("pubkey")
                            if pk is not None:
                                out.append(str(pk))
    except (AttributeError, TypeError, KeyError):
        pass
    return out


def _neighbors_from_txs(tx_values: list[Any], wallet: str) -> set[str]:
    """From a list of tx values, collect unique counterparty addresses (exclude wallet and program IDs)."""
    neighbors: set[str] = set()
    wallet_lower = wallet.strip()
    for tx_value in tx_values:
        keys = _account_keys_from_tx(tx_value)
        for k in keys:
            k = k.strip()
            if not k or k == wallet_lower:
                continue
            if k in KNOWN_PROGRAM_IDS:
                continue
            if len(k) >= 32 and len(k) <= 48:
                neighbors.add(k)
    return neighbors


def _build_cluster_id(nodes: set[str]) -> str:
    """Deterministic cluster id from sorted node set."""
    if not nodes:
        return "cluster_0"
    h = hashlib.sha256(" ".join(sorted(nodes)).encode()).hexdigest()[:12]
    return f"cluster_{h}"


def detect_wallet_cluster(wallet: str) -> dict[str, Any]:
    """
    Build interaction graph from recent txs, assign cluster_id and cluster_risk.

    Returns:
        cluster_id: deterministic id for the connected component containing the wallet.
        cluster_size: number of wallets in the cluster (this wallet + direct counterparts).
        cluster_risk: "HIGH" if any wallet in the cluster is in the scam_wallets blacklist, else "LOW".
    """
    from solders.pubkey import Pubkey
    from solders.signature import Signature
    from solana.rpc.api import Client

    wallet = (wallet or "").strip()
    empty = {"cluster_id": "cluster_0", "cluster_size": 1, "cluster_risk": "LOW"}
    if not wallet:
        return empty

    try:
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("wallet_graph_invalid_wallet", wallet=wallet[:16] + "...", error=str(e))
        return empty

    scam_wallets = _load_scam_wallets()
    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    try:
        client = Client(rpc_url)
    except Exception as e:
        logger.warning("wallet_graph_client_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    tx_values: list[Any] = []
    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_TO_SCAN)
        sigs_value = _get_resp_value(sigs_resp)
        if sigs_value is None:
            return empty
        sigs_list = list(sigs_value)[:MAX_TXS_TO_SCAN]
    except Exception as e:
        logger.debug("wallet_graph_signatures_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    for sig_entry in sigs_list:
        sig_val = getattr(sig_entry, "signature", None) or (sig_entry.get("signature") if isinstance(sig_entry, dict) else None)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
            tx_value = _get_resp_value(tx) if tx else None
        except Exception:
            continue
        if tx_value is not None:
            tx_values.append(tx_value)

    neighbors = _neighbors_from_txs(tx_values, wallet)
    nodes: set[str] = {wallet} | neighbors
    cluster_size = len(nodes)
    cluster_id = _build_cluster_id(nodes)
    cluster_risk = "HIGH" if (scam_wallets & nodes) else "LOW"

    result = {
        "cluster_id": cluster_id,
        "cluster_size": cluster_size,
        "cluster_risk": cluster_risk,
    }
    if cluster_risk == "HIGH":
        logger.info(
            "wallet_graph_scam_cluster",
            wallet=wallet[:16] + "...",
            cluster_id=cluster_id,
            cluster_size=cluster_size,
        )
    return result
