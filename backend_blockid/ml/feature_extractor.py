import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from solana.rpc.api import Client
from solders.pubkey import Pubkey
from solders.signature import Signature

# Load .env from project root (works from Windows / WSL / any CWD)
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

cluster = os.getenv("SOLANA_CLUSTER", "devnet").strip().lower()
if cluster == "mainnet":
    RPC = os.getenv("SOLANA_RPC_URL") or "https://mainnet.helius-rpc.com"
else:
    RPC = "https://api.devnet.solana.com"

print("Cluster:", cluster)
print("RPC:", RPC)

client = Client(RPC, commitment="confirmed")


def _account_keys_from_tx_value(tx_value: Any) -> List[str]:
    """
    Extract account keys (base58 strings) from a jsonParsed transaction value.
    Handles both object-style and dict-style responses.
    """
    keys_out: List[str] = []
    if tx_value is None:
        return keys_out
    try:
        tx_obj = getattr(tx_value, "transaction", None) or (
            tx_value.get("transaction") if isinstance(tx_value, dict) else None
        )
        if tx_obj is None:
            return keys_out
        msg = getattr(tx_obj, "message", None) or (
            tx_obj.get("message") if isinstance(tx_obj, dict) else None
        )
        if msg is None:
            return keys_out
        keys = getattr(msg, "account_keys", None) or getattr(msg, "accountKeys", None)
        if keys is None and isinstance(msg, dict):
            keys = msg.get("account_keys") or msg.get("accountKeys")
        if keys is None:
            return keys_out
        for k in keys:
            if isinstance(k, str):
                if k:
                    keys_out.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if pk:
                    keys_out.append(str(pk))
    except Exception:
        # Best-effort; on parse errors just return what we have
        return keys_out
    return keys_out


def extract_features(wallet: str) -> Dict[str, Any]:
    """
    Extract wallet features using solana-py 0.36 + solders on devnet.

    Features:
      - total_tx: number of recent transactions (up to last 100 signatures)
      - account_age_days: age in days based on oldest signature's block_time
      - unique_counterparties: number of unique counterparties from last ~20 txs
      - extracted_at: ISO timestamp of extraction

    Prints basic debug info about the number of signatures fetched.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        print("Feature error: empty wallet")
        return {
            "total_tx": 0,
            "account_age_days": 0,
            "unique_counterparties": 0,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    try:
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        print("Feature error (invalid pubkey):", e)
        return {
            "total_tx": 0,
            "account_age_days": 0,
            "unique_counterparties": 0,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    try:
        resp = client.get_signatures_for_address(pubkey, limit=100)

        if resp.value is None:
            print("No signatures returned")
            return {
                "total_tx": 0,
                "account_age_days": 0,
                "unique_counterparties": 0,
                "extracted_at": datetime.utcnow().isoformat(),
            }

        sig_infos = list(resp.value)
        total_tx = len(sig_infos)
        print("DEBUG total_tx:", total_tx)

        if total_tx == 0:
            return {
                "total_tx": 0,
                "account_age_days": 0,
                "unique_counterparties": 0,
                "extracted_at": datetime.utcnow().isoformat(),
            }

        # Oldest entry is last when signatures are newest-first
        oldest_entry = sig_infos[-1]
        first_time = getattr(oldest_entry, "block_time", None)
        if first_time:
            age_days = (datetime.now(timezone.utc).timestamp() - first_time) / 86400
        else:
            age_days = 0

        # Compute unique counterparties from last ~20 transactions
        counterparties: set[str] = set()
        for info in sig_infos[:20]:
            sig_val = getattr(info, "signature", None)
            if sig_val is None:
                continue
            try:
                sig = sig_val if isinstance(sig_val, Signature) else Signature.from_string(str(sig_val))
            except Exception:
                continue

            try:
                tx_detail = client.get_transaction(
                    sig,
                    encoding="jsonParsed",
                    max_supported_transaction_version=0,
                )
            except Exception:
                continue

            if tx_detail.value is None:
                continue

            # Support both legacy and v0 transaction layouts
            try:
                # Legacy / common case
                message = tx_detail.value.transaction.message
                keys = message.account_keys
            except Exception:
                try:
                    # v0 or nested transaction
                    message = tx_detail.value.transaction.transaction.message
                    keys = message.account_keys
                except Exception:
                    continue

            for k in keys:
                addr = str(k).strip()
                if not addr or addr == wallet:
                    continue
                counterparties.add(addr)

        unique_counterparties = len(counterparties)
        print("DEBUG counterparties:", unique_counterparties)

        return {
            "total_tx": total_tx,
            "account_age_days": int(age_days),
            "unique_counterparties": unique_counterparties,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        print("Feature error:", e)
        return {
            "total_tx": 0,
            "account_age_days": 0,
            "unique_counterparties": 0,
            "extracted_at": datetime.utcnow().isoformat(),
        }
