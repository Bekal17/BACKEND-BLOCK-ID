"""
Wallet scanner: collect on-chain metrics for a Solana wallet via RPC.

Uses encoding="jsonParsed" for token accounts and get_transaction so RPC
returns parsed data. Fetches tx count (1000), wallet age from oldest block_time,
unique programs from first 20 transaction message.instructions (and meta inner),
token count from get_token_accounts_by_owner with TokenAccountOpts.
Advanced ML metrics: avg_tx_value, nft_count, dex_interactions, lp_interactions,
cluster_size, scam_cluster_flag (0; cluster risk comes from wallet_graph).
On parse failure metrics are set to None.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

SIGNATURES_LIMIT = 1000
MAX_TXS_FOR_PROGRAM_PARSING = 20
MAX_TXS_AVG_VALUE = 100
MAX_TXS_DEX_LP = 50
MAX_TXS_CLUSTER = 30
TOKEN_PROGRAM_ID_STR = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
DEFAULT_RPC = "https://api.devnet.solana.com"

# Known DEX / LP program IDs (mainnet; used for dex_interactions and lp_interactions)
RAYDIUM_PROGRAM_IDS = frozenset({
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # AMM
    "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",  # CLMM
})
ORCA_PROGRAM_IDS = frozenset({
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",  # Orca V1
    "whirLbMiicVdio4qvUfM5KQ6ebyKoEK6KqnioypnfdR",   # Whirlpool
})
JUPITER_PROGRAM_IDS = frozenset({
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "JUP5cHjnnCx2DppVsufsLrXs8EBZeEZz2JGWFAyJGbE",
})
DEX_PROGRAM_IDS = RAYDIUM_PROGRAM_IDS | ORCA_PROGRAM_IDS | JUPITER_PROGRAM_IDS
LP_PROGRAM_IDS = RAYDIUM_PROGRAM_IDS | ORCA_PROGRAM_IDS


def _rpc_url() -> str:
    return (os.getenv("SOLANA_RPC_URL") or "").strip() or DEFAULT_RPC


def _get_resp_value(resp: Any) -> Any:
    if resp is None:
        return None
    v = getattr(resp, "value", None)
    if v is not None:
        return v
    if hasattr(resp, "result"):
        return getattr(resp.result, "value", None)
    return None


def _block_time_from_sig(s: Any) -> int | None:
    bt = getattr(s, "block_time", None) or getattr(s, "blockTime", None)
    if bt is not None:
        try:
            return int(bt)
        except (TypeError, ValueError):
            pass
    if isinstance(s, dict):
        bt = s.get("block_time") or s.get("blockTime")
        if bt is not None:
            try:
                return int(bt)
            except (TypeError, ValueError):
                pass
    return None


def _signature_from_sig(s: Any) -> Any:
    sig = getattr(s, "signature", None)
    if sig is not None:
        return sig
    if isinstance(s, dict):
        return s.get("signature")
    return None


def _count_token_accounts_parsed(token_accounts: Any) -> int | None:
    """
    Count token accounts when RPC was called with encoding=jsonParsed.
    Primary: acct.account.data.parsed["info"], mint = info.get("mint"). Never use acct.mint.
    Fallback: dict-style acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("mint").
    Returns None on failure.
    """
    value = getattr(token_accounts, "value", token_accounts)
    if value is None:
        return None
    try:
        accounts = list(value)
    except TypeError:
        return None
    token_count = 0
    for acct in accounts:
        mint = None
        try:
            parsed = acct.account.data.parsed
            if isinstance(parsed, dict):
                info = parsed.get("info", {})
            else:
                info = getattr(parsed, "info", None)
                info = info if isinstance(info, dict) else {}
            mint = info.get("mint") if isinstance(info, dict) else None
        except (AttributeError, TypeError, KeyError):
            pass
        if not mint and isinstance(acct, dict):
            try:
                info = acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                mint = info.get("mint") if isinstance(info, dict) else None
            except (AttributeError, TypeError, KeyError):
                pass
        if mint:
            token_count += 1
    return token_count


def _parse_token_accounts_safe(token_accounts: Any, wallet_prefix: str) -> tuple[int | None, bool]:
    """Compatibility: count token accounts; returns (count or None, parse_failed)."""
    count = _count_token_accounts_parsed(token_accounts)
    return (count, count is None)


def _sol_lamports_change_for_wallet(tx_value: Any, wallet: str) -> float | None:
    """From tx.value (parsed), compute SOL balance change in lamports for the given wallet. Returns None on parse failure."""
    if tx_value is None:
        return None
    try:
        tx_obj = getattr(tx_value, "transaction", None) or (tx_value.get("transaction") if isinstance(tx_value, dict) else None)
        meta = getattr(tx_value, "meta", None) or (tx_value.get("meta") if isinstance(tx_value, dict) else None)
        if tx_obj is None or meta is None:
            return None
        msg = getattr(tx_obj, "message", None) or (tx_obj.get("message") if isinstance(tx_obj, dict) else None)
        if msg is None:
            return None
        keys = getattr(msg, "account_keys", None) or getattr(msg, "accountKeys", None)
        if keys is None and isinstance(msg, dict):
            keys = msg.get("account_keys") or msg.get("accountKeys")
        pre = getattr(meta, "pre_balances", None) or getattr(meta, "preBalances", None)
        if pre is None and isinstance(meta, dict):
            pre = meta.get("pre_balances") or meta.get("preBalances")
        post = getattr(meta, "post_balances", None) or getattr(meta, "postBalances", None)
        if post is None and isinstance(meta, dict):
            post = meta.get("post_balances") or meta.get("postBalances")
        if not keys or not pre or not post:
            return None
        key_list = list(keys) if not isinstance(keys, list) else keys
        pre_list = list(pre) if not isinstance(pre, list) else pre
        post_list = list(post) if not isinstance(post, list) else post
        idx = None
        for i, k in enumerate(key_list):
            pk = str(k) if not isinstance(k, dict) else str(k.get("pubkey", k))
            if pk == wallet:
                idx = i
                break
        if idx is None or idx >= len(pre_list) or idx >= len(post_list):
            return None
        pre_bal = int(pre_list[idx]) if pre_list[idx] is not None else 0
        post_bal = int(post_list[idx]) if post_list[idx] is not None else 0
        fee = int(getattr(meta, "fee", None) or (meta.get("fee") if isinstance(meta, dict) else 0) or 0)
        fee_payer_idx = getattr(msg, "account_keys", None) or (msg.get("account_keys") if isinstance(msg, dict) else None)
        if fee_payer_idx is not None:
            first_key = (key_list[0] if key_list else None)
            first_pk = str(first_key) if not isinstance(first_key, dict) else str(first_key.get("pubkey", first_key))
            if first_pk == wallet:
                post_bal -= fee
        return float(post_bal - pre_bal)
    except (AttributeError, TypeError, KeyError, IndexError, ValueError):
        return None


def _program_ids_from_tx_value(tx_value: Any) -> set[str]:
    """
    Extract program ids from tx.value.transaction.message.instructions
    and from tx.value.meta.inner_instructions when present. Handles object and dict responses.
    """
    programs: set[str] = set()
    if tx_value is None:
        return programs
    try:
        tx_obj = getattr(tx_value, "transaction", None) or (tx_value.get("transaction") if isinstance(tx_value, dict) else None)
        if tx_obj is None:
            return programs
        msg = getattr(tx_obj, "message", None) or (tx_obj.get("message") if isinstance(tx_obj, dict) else None)
        if msg is None:
            return programs
        instructions = getattr(msg, "instructions", None) or (msg.get("instructions", []) if isinstance(msg, dict) else [])
        if instructions is None:
            instructions = []
        for ix in instructions:
            pid = getattr(ix, "program_id", None) or getattr(ix, "programId", None)
            if pid is None and isinstance(ix, dict):
                pid = ix.get("program_id") or ix.get("programId")
            if pid is not None:
                programs.add(str(pid))
        meta = getattr(tx_value, "meta", None) or (tx_value.get("meta") if isinstance(tx_value, dict) else None)
        if meta is not None:
            inner = getattr(meta, "inner_instructions", None) or getattr(meta, "innerInstructions", None)
            if inner is None and isinstance(meta, dict):
                inner = meta.get("inner_instructions") or meta.get("innerInstructions")
            if inner is not None:
                for group in inner:
                    inxs = getattr(group, "instructions", None)
                    if inxs is None and isinstance(group, dict):
                        inxs = group.get("instructions", [])
                    for inner_ix in inxs or []:
                        pid = getattr(inner_ix, "program_id", None) or getattr(inner_ix, "programId", None)
                        if pid is None and isinstance(inner_ix, dict):
                            pid = inner_ix.get("program_id") or inner_ix.get("programId")
                        if pid is not None:
                            programs.add(str(pid))
    except (AttributeError, TypeError, KeyError):
        pass
    return programs


def scan_wallet(wallet: str) -> dict[str, Any]:
    """
    Collect on-chain metrics using Solana RPC with parsed encoding where applicable.
    Returns dict: wallet, tx_count, wallet_age_days, unique_programs, token_accounts.
    On RPC or parse failure a metric is set to None.
    """
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.rpc.types import TokenAccountOpts

    wallet = (wallet or "").strip()
    if not wallet:
        logger.warning("wallet_scanner_empty_wallet")
        return _empty_metrics(wallet)

    try:
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("wallet_scanner_invalid_wallet", wallet=wallet[:16] + "...", error=str(e))
        return _empty_metrics(wallet)

    rpc_url = _rpc_url()
    out: dict[str, Any] = {
        "wallet": wallet,
        "tx_count": None,
        "wallet_age_days": None,
        "unique_programs": None,
        "token_accounts": None,
    }

    try:
        client = Client(rpc_url)
    except Exception as e:
        logger.warning("wallet_scanner_client_failed", wallet=wallet[:16] + "...", error=str(e))
        return out

    wallet_short = wallet[:16] + "..."
    token_program_pubkey = Pubkey.from_string(TOKEN_PROGRAM_ID_STR)

    # --- Signatures: limit 1000, wallet age from oldest block_time (skip if missing) ---
    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=SIGNATURES_LIMIT)
        signatures_value = _get_resp_value(sigs_resp)
        if signatures_value is not None:
            try:
                sigs_list = list(signatures_value)
            except TypeError:
                sigs_list = []
            out["tx_count"] = len(sigs_list)
            block_times = []
            for s in sigs_list:
                bt = _block_time_from_sig(s)
                if bt is not None:
                    block_times.append(bt)
            if block_times:
                oldest = min(block_times)
                out["wallet_age_days"] = max(0, int((time.time() - oldest) / 86400))
                logger.info("wallet_age_computed", wallet=wallet_short, age_days=out["wallet_age_days"])
            else:
                out["wallet_age_days"] = None
        else:
            out["tx_count"] = None
            out["wallet_age_days"] = None
    except Exception as e:
        logger.warning("wallet_scanner_signatures_failed", wallet=wallet_short, error=str(e))
        out["tx_count"] = None
        out["wallet_age_days"] = None

    # --- Unique programs: get_transaction with encoding=jsonParsed, message.instructions + meta.inner ---
    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=SIGNATURES_LIMIT)
        signatures_value = _get_resp_value(sigs_resp)
        if signatures_value is None:
            out["unique_programs"] = None
        else:
            try:
                sigs_list = list(signatures_value)[:MAX_TXS_FOR_PROGRAM_PARSING]
            except TypeError:
                sigs_list = []
            programs: set[str] = set()
            from solders.signature import Signature
            first_tx_logged = False
            for sig in sigs_list:
                sig_val = _signature_from_sig(sig)
                if sig_val is None:
                    continue
                try:
                    sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
                except Exception:
                    continue
                tx = client.get_transaction(
                    sig_use,
                    encoding="jsonParsed",
                    max_supported_transaction_version=0,
                )
                if not tx or not tx.value:
                    logger.debug("wallet_scanner_tx_value_none", wallet=wallet_short, signature=str(sig_val)[:44])
                    continue
                if not first_tx_logged:
                    logger.debug("sample_tx", sample=str(tx.value)[:800])
                    first_tx_logged = True
                programs |= _program_ids_from_tx_value(tx.value)
            out["unique_programs"] = len(programs)
            logger.info("unique_programs_detected", wallet=wallet_short, count=out["unique_programs"])
    except Exception as e:
        logger.debug("wallet_scanner_programs_failed", wallet=wallet_short, error=str(e))
        out["unique_programs"] = None

    # --- Token accounts: get_token_accounts_by_owner with TokenAccountOpts + encoding=jsonParsed ---
    try:
        token_accounts = client.get_token_accounts_by_owner(
            pubkey,
            TokenAccountOpts(program_id=token_program_pubkey, encoding="jsonParsed"),
        )
        if token_accounts is not None and hasattr(token_accounts, "value"):
            logger.debug("sample_token_accounts", sample=str(token_accounts.value[:1]))
        count = _count_token_accounts_parsed(token_accounts)
        if count is not None:
            out["token_accounts"] = count
        else:
            out["token_accounts"] = None
    except Exception as e:
        logger.warning("wallet_scanner_token_accounts_failed", wallet=wallet_short, error=str(e))
        out["token_accounts"] = None

    # --- Advanced ML metrics: avg_tx_value, nft_count, dex_interactions, lp_interactions, cluster_size, scam_cluster_flag ---
    try:
        adv = _get_advanced_metrics_sync(wallet, client, pubkey, token_program_pubkey)
        out["avg_tx_value"] = adv.get("avg_tx_value")
        out["nft_count"] = adv.get("nft_count")
        out["dex_interactions"] = adv.get("dex_interactions")
        out["lp_interactions"] = adv.get("lp_interactions")
        out["cluster_size"] = adv.get("cluster_size")
        out["scam_cluster_flag"] = adv.get("scam_cluster_flag", 0)
    except Exception as e:
        logger.debug("wallet_scanner_advanced_metrics_failed", wallet=wallet_short, error=str(e))
        out["avg_tx_value"] = None
        out["nft_count"] = None
        out["dex_interactions"] = None
        out["lp_interactions"] = None
        out["cluster_size"] = None
        out["scam_cluster_flag"] = 0

    logger.info(
        "wallet_scanner_done",
        wallet=wallet_short,
        tx_count=out["tx_count"],
        wallet_age_days=out["wallet_age_days"],
        unique_programs=out["unique_programs"],
        token_accounts=out["token_accounts"],
        avg_tx_value=out.get("avg_tx_value"),
        nft_count=out.get("nft_count"),
        dex_interactions=out.get("dex_interactions"),
        lp_interactions=out.get("lp_interactions"),
        cluster_size=out.get("cluster_size"),
    )
    return out


def _empty_metrics(wallet: str) -> dict[str, Any]:
    return {
        "wallet": wallet or "",
        "tx_count": None,
        "wallet_age_days": None,
        "unique_programs": None,
        "token_accounts": None,
        "avg_tx_value": None,
        "nft_count": None,
        "dex_interactions": None,
        "lp_interactions": None,
        "cluster_size": None,
        "scam_cluster_flag": 0,
    }


# -----------------------------------------------------------------------------
# Advanced ML metrics (sync implementations; used by scan_wallet and async wrappers)
# -----------------------------------------------------------------------------
def _compute_avg_tx_value_sync(wallet: str, client: Any, pubkey: Any) -> float:
    """Fetch last MAX_TXS_AVG_VALUE tx signatures, sum SOL transferred (lamports), return avg in SOL (lamports/1e9)."""
    from solders.signature import Signature

    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_AVG_VALUE)
        sigs_value = _get_resp_value(sigs_resp)
        if not sigs_value:
            return 0.0
        sigs_list = list(sigs_value)[:MAX_TXS_AVG_VALUE]
    except Exception:
        return 0.0
    total_lamports = 0.0
    count = 0
    for sig_entry in sigs_list:
        sig_val = _signature_from_sig(sig_entry)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
        except Exception:
            continue
        if not tx or not tx.value:
            continue
        delta = _sol_lamports_change_for_wallet(tx.value, wallet)
        if delta is not None:
            total_lamports += abs(delta)
            count += 1
    if count == 0:
        return 0.0
    return (total_lamports / count) / 1e9


def _detect_dex_interactions_sync(wallet: str, client: Any, pubkey: Any) -> int:
    """Count transactions that interact with known DEX program IDs."""
    from solders.signature import Signature

    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_DEX_LP)
        sigs_value = _get_resp_value(sigs_resp)
        if not sigs_value:
            return 0
        sigs_list = list(sigs_value)[:MAX_TXS_DEX_LP]
    except Exception:
        return 0
    count = 0
    for sig_entry in sigs_list:
        sig_val = _signature_from_sig(sig_entry)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
        except Exception:
            continue
        if not tx or not tx.value:
            continue
        programs = _program_ids_from_tx_value(tx.value)
        if programs & DEX_PROGRAM_IDS:
            count += 1
    return count


def _detect_lp_interactions_sync(wallet: str, client: Any, pubkey: Any) -> int:
    """Count transactions that interact with known LP (liquidity) program IDs."""
    from solders.signature import Signature

    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_DEX_LP)
        sigs_value = _get_resp_value(sigs_resp)
        if not sigs_value:
            return 0
        sigs_list = list(sigs_value)[:MAX_TXS_DEX_LP]
    except Exception:
        return 0
    count = 0
    for sig_entry in sigs_list:
        sig_val = _signature_from_sig(sig_entry)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
        except Exception:
            continue
        if not tx or not tx.value:
            continue
        programs = _program_ids_from_tx_value(tx.value)
        if programs & LP_PROGRAM_IDS:
            count += 1
    return count


def _count_nft_accounts_sync(wallet: str, client: Any, pubkey: Any, token_program_pubkey: Any) -> int:
    """getTokenAccountsByOwner; count tokens with decimals == 0 and amount == 1 (NFT-like)."""
    from solana.rpc.types import TokenAccountOpts
    try:
        token_accounts = client.get_token_accounts_by_owner(
            pubkey,
            TokenAccountOpts(program_id=token_program_pubkey, encoding="jsonParsed"),
        )
    except Exception:
        return 0
    value = getattr(token_accounts, "value", token_accounts)
    if value is None:
        return 0
    try:
        accounts = list(value)
    except TypeError:
        return 0
    nft_count = 0
    for acct in accounts:
        info = None
        try:
            parsed = acct.account.data.parsed
            if isinstance(parsed, dict):
                info = parsed.get("info", {})
            else:
                info = getattr(parsed, "info", None)
                info = info if isinstance(info, dict) else {}
        except (AttributeError, TypeError, KeyError):
            if isinstance(acct, dict):
                info = acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
        if not isinstance(info, dict):
            continue
        token_amount = info.get("tokenAmount") or {}
        dec = token_amount.get("decimals")
        amt = token_amount.get("amount")
        if dec is not None and amt is not None:
            try:
                if int(dec) == 0 and (amt == "1" or int(amt) == 1):
                    nft_count += 1
            except (TypeError, ValueError):
                pass
    return nft_count


def _estimate_cluster_size_sync(wallet: str, client: Any, pubkey: Any) -> int:
    """Find first inbound tx sender wallets; count unique counterparties (account keys excluding wallet and programs)."""
    from solders.signature import Signature

    KNOWN = frozenset({
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "MetaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
    })
    try:
        sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_CLUSTER)
        sigs_value = _get_resp_value(sigs_resp)
        if not sigs_value:
            return 1
        sigs_list = list(sigs_value)[:MAX_TXS_CLUSTER]
    except Exception:
        return 1
    counterparties: set[str] = set()
    wallet_clean = wallet.strip()
    for sig_entry in sigs_list:
        sig_val = _signature_from_sig(sig_entry)
        if sig_val is None:
            continue
        try:
            sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
        except Exception:
            continue
        try:
            tx = client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)
        except Exception:
            continue
        if not tx or not tx.value:
            continue
        tx_obj = getattr(tx.value, "transaction", None) or (tx.value.get("transaction") if isinstance(tx.value, dict) else None)
        if tx_obj is None and isinstance(tx.value, dict):
            tx_obj = tx.value.get("transaction")
        if tx_obj is None:
            continue
        msg = getattr(tx_obj, "message", None) or (tx_obj.get("message") if isinstance(tx_obj, dict) else None)
        if msg is None:
            continue
        keys = getattr(msg, "account_keys", None) or getattr(msg, "accountKeys", None)
        if keys is None and isinstance(msg, dict):
            keys = msg.get("account_keys") or msg.get("accountKeys")
        if not keys:
            continue
        for k in keys:
            pk = str(k) if not isinstance(k, dict) else str(k.get("pubkey", k))
            if not pk or pk == wallet_clean or pk in KNOWN or len(pk) < 32:
                continue
            counterparties.add(pk)
    return max(1, len(counterparties))


def _get_advanced_metrics_sync(wallet: str, client: Any, pubkey: Any, token_program_pubkey: Any) -> dict[str, Any]:
    """Compute all advanced ML metrics; return dict to merge into scan_wallet output."""
    out: dict[str, Any] = {
        "avg_tx_value": None,
        "nft_count": None,
        "dex_interactions": None,
        "lp_interactions": None,
        "cluster_size": None,
        "scam_cluster_flag": 0,
    }
    try:
        out["avg_tx_value"] = _compute_avg_tx_value_sync(wallet, client, pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_avg_tx_value_failed", wallet=wallet[:16], error=str(e))
    try:
        out["nft_count"] = _count_nft_accounts_sync(wallet, client, pubkey, token_program_pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_nft_count_failed", wallet=wallet[:16], error=str(e))
    try:
        out["dex_interactions"] = _detect_dex_interactions_sync(wallet, client, pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_dex_failed", wallet=wallet[:16], error=str(e))
    try:
        out["lp_interactions"] = _detect_lp_interactions_sync(wallet, client, pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_lp_failed", wallet=wallet[:16], error=str(e))
    try:
        out["cluster_size"] = _estimate_cluster_size_sync(wallet, client, pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_cluster_size_failed", wallet=wallet[:16], error=str(e))
    return out


# -----------------------------------------------------------------------------
# Async wrappers (run sync code in executor)
# -----------------------------------------------------------------------------
async def compute_avg_tx_value(wallet: str) -> float:
    """Fetch last 100 tx signatures, sum SOL transferred, return avg value (SOL)."""
    loop = asyncio.get_event_loop()
    def _run() -> float:
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        try:
            pubkey = Pubkey.from_string(wallet)
            client = Client(_rpc_url())
            return _compute_avg_tx_value_sync(wallet, client, pubkey)
        except Exception:
            return 0.0
    return await loop.run_in_executor(None, _run)


async def detect_dex_interactions(wallet: str) -> int:
    """Count interactions with known DEX program IDs (Raydium, Orca, Jupiter)."""
    loop = asyncio.get_event_loop()
    def _run() -> int:
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        try:
            pubkey = Pubkey.from_string(wallet)
            client = Client(_rpc_url())
            return _detect_dex_interactions_sync(wallet, client, pubkey)
        except Exception:
            return 0
    return await loop.run_in_executor(None, _run)


async def detect_lp_interactions(wallet: str) -> int:
    """Count add/remove liquidity interactions (Raydium, Orca LP programs)."""
    loop = asyncio.get_event_loop()
    def _run() -> int:
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        try:
            pubkey = Pubkey.from_string(wallet)
            client = Client(_rpc_url())
            return _detect_lp_interactions_sync(wallet, client, pubkey)
        except Exception:
            return 0
    return await loop.run_in_executor(None, _run)


async def count_nft_accounts(wallet: str) -> int:
    """getTokenAccountsByOwner; count tokens with decimals == 0 and supply == 1."""
    loop = asyncio.get_event_loop()
    def _run() -> int:
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        from solana.rpc.types import TokenAccountOpts
        try:
            pubkey = Pubkey.from_string(wallet)
            client = Client(_rpc_url())
            token_program_pubkey = Pubkey.from_string(TOKEN_PROGRAM_ID_STR)
            return _count_nft_accounts_sync(wallet, client, pubkey, token_program_pubkey)
        except Exception:
            return 0
    return await loop.run_in_executor(None, _run)


async def estimate_cluster_size(wallet: str) -> int:
    """Find first inbound tx sender wallets; count unique senders (counterparties)."""
    loop = asyncio.get_event_loop()
    def _run() -> int:
        from solders.pubkey import Pubkey
        from solana.rpc.api import Client
        try:
            pubkey = Pubkey.from_string(wallet)
            client = Client(_rpc_url())
            return _estimate_cluster_size_sync(wallet, client, pubkey)
        except Exception:
            return 1
    return await loop.run_in_executor(None, _run)


def get_advanced_metrics(wallet: str) -> dict[str, Any]:
    """
    Compute advanced ML metrics (avg_tx_value, nft_count, dex_interactions,
    lp_interactions, cluster_size, scam_cluster_flag=0). Sync; used by scan_wallet.
    """
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.rpc.types import TokenAccountOpts

    try:
        pubkey = Pubkey.from_string(wallet)
        client = Client(_rpc_url())
        token_program_pubkey = Pubkey.from_string(TOKEN_PROGRAM_ID_STR)
        return _get_advanced_metrics_sync(wallet, client, pubkey, token_program_pubkey)
    except Exception as e:
        logger.debug("wallet_scanner_advanced_metrics_failed", wallet=wallet[:16], error=str(e))
        return {
            "avg_tx_value": None,
            "nft_count": None,
            "dex_interactions": None,
            "lp_interactions": None,
            "cluster_size": None,
            "scam_cluster_flag": 0,
        }
