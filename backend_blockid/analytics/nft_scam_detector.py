"""
NFT scam detection for BlockID analytics: victim vs scammer.

Scans NFT holdings, parses Metaplex metadata (creators, collection), checks
mint authority and outbound transfer count. Compares against scam collection
blacklist. Only scammer role is penalized in the trust engine.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

METADATA_PROGRAM_ID = "MetaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
MAX_NFT_MINTS_TO_CHECK = 50
MAX_TXS_FOR_DISTRIBUTION = 100
MASS_DISTRIBUTE_THRESHOLD = 10
DEFAULT_COLLECTIONS_PATH = Path(__file__).resolve().parent.parent / "oracle" / "scam_nft_collections.json"

ROLE_VICTIM = "victim"
ROLE_PARTICIPANT = "participant"
ROLE_SCAMMER = "scammer"
ROLE_NONE = "none"


def _load_scam_collections() -> set[str]:
    """Load scam collection mint addresses from JSON. Returns empty set on failure."""
    path_str = os.getenv("SCAM_NFT_COLLECTIONS_PATH", "").strip() or str(DEFAULT_COLLECTIONS_PATH)
    path = Path(path_str)
    if not path.is_file():
        logger.debug("nft_scam_detector_collections_missing", path=path_str)
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("nft_scam_detector_collections_load_failed", path=path_str, error=str(e))
        return set()
    if not isinstance(data, list):
        return set()
    return {str(p).strip() for p in data if p}


def _get_resp_value(resp: Any) -> Any:
    if resp is None:
        return None
    v = getattr(resp, "value", None)
    if v is not None:
        return v
    if hasattr(resp, "result"):
        return getattr(resp.result, "value", None)
    return None


def _pubkey_from_bytes(data: bytes) -> str:
    """Encode 32-byte pubkey to base58. Uses solders if available."""
    if len(data) < 32:
        return ""
    try:
        from solders.pubkey import Pubkey
        return str(Pubkey.from_bytes(data[:32]))
    except Exception:
        try:
            import base58
            return base58.b58encode(data[:32]).decode("ascii")
        except Exception:
            return ""


def _metadata_pda(mint_pubkey: Any) -> Any:
    """Return PDA for Metaplex metadata account: ['metadata', program_id, mint]."""
    from solders.pubkey import Pubkey
    program_id = Pubkey.from_string(METADATA_PROGRAM_ID)
    mint = mint_pubkey if hasattr(mint_pubkey, "value") else Pubkey.from_string(str(mint_pubkey))
    seeds = [b"metadata", bytes(program_id), bytes(mint)]
    return Pubkey.find_program_address(seeds, program_id)[0]


def _parse_metadata_creators_and_collection(data: bytes) -> tuple[list[str], str | None]:
    """
    Minimal parse of Metaplex Metadata account: creators (list of base58 addresses)
    and collection key (base58 or None). Returns ([], None) on any parse error.
    Layout: key(1) update_authority(32) mint(32) name(4+var) symbol(4+var) uri(4+var)
    seller_fee(2) creators(option) primary_sale(1) is_mutable(1) edition_nonce(opt) token_standard(opt) collection(opt).
    """
    creators: list[str] = []
    collection_key: str | None = None
    if len(data) < 1 + 32 + 32 + 4 + 2:
        return (creators, collection_key)
    try:
        pos = 1 + 32 + 32  # skip key, update_authority, mint
        for _ in range(3):  # name, symbol, uri
            if pos + 4 > len(data):
                return (creators, collection_key)
            (slen,) = struct.unpack_from("<I", data, pos)
            pos += 4 + min(slen, 500)
        if pos + 2 > len(data):
            return (creators, collection_key)
        pos += 2  # seller_fee_basis_points
        # creators option
        if pos >= len(data):
            return (creators, collection_key)
        has_creators = data[pos]
        pos += 1
        if has_creators and pos + 4 <= len(data):
            (n,) = struct.unpack_from("<I", data, pos)
            pos += 4
            for _ in range(min(n, 20)):
                if pos + 33 > len(data):
                    break
                addr = _pubkey_from_bytes(data[pos : pos + 32])
                if addr:
                    creators.append(addr)
                pos += 33
        # primary_sale_happened, is_mutable
        if pos + 2 > len(data):
            return (creators, collection_key)
        pos += 2
        # edition_nonce option
        if pos < len(data):
            pos += 1
        # token_standard option
        if pos < len(data):
            pos += 1
        # collection option
        if pos < len(data) and data[pos] == 1 and pos + 33 <= len(data):
            collection_key = _pubkey_from_bytes(data[pos + 1 : pos + 33])
    except (struct.error, IndexError):
        pass
    return (creators, collection_key)


def _get_nft_mints_from_token_accounts(token_accounts_resp: Any) -> list[str]:
    """From get_token_accounts_by_owner (jsonParsed) value, return list of mint addresses for NFTs (amount 1, decimals 0)."""
    mints: list[str] = []
    value = getattr(token_accounts_resp, "value", token_accounts_resp)
    if value is None:
        return mints
    try:
        accounts = list(value)
    except TypeError:
        return mints
    for acct in accounts[: MAX_NFT_MINTS_TO_CHECK * 2]:
        mint = None
        amount_str = None
        decimals = None
        try:
            parsed = getattr(acct, "account", None)
            if parsed is None and isinstance(acct, dict):
                parsed = acct.get("account")
            if parsed is None:
                continue
            data = getattr(parsed, "data", None)
            if data is None and isinstance(parsed, dict):
                data = parsed.get("data")
            if data is None:
                continue
            info = None
            if hasattr(data, "parsed"):
                p = data.parsed
                info = getattr(p, "info", None) if not isinstance(p, dict) else p.get("info")
            elif isinstance(data, dict):
                info = data.get("parsed", {}).get("info")
            if not isinstance(info, dict):
                continue
            mint = info.get("mint")
            ta = info.get("tokenAmount") or info.get("token_amount")
            if isinstance(ta, dict):
                amount_str = ta.get("amount")
                decimals = ta.get("decimals")
            if hasattr(ta, "amount"):
                amount_str = getattr(ta, "amount", None)
            if hasattr(ta, "decimals"):
                decimals = getattr(ta, "decimals", None)
        except (AttributeError, TypeError, KeyError):
            continue
        if not mint:
            continue
        # NFT: single token, 0 decimals
        try:
            amt = int(amount_str) if amount_str is not None else 0
        except (TypeError, ValueError):
            amt = 0
        dec = int(decimals) if decimals is not None else 0
        if amt == 1 and dec == 0:
            mints.append(str(mint))
    return mints[:MAX_NFT_MINTS_TO_CHECK]


def _is_mint_authority(mint_pubkey: str, wallet: str, client: Any) -> bool:
    """Return True if wallet is the mint authority of the SPL Token mint account."""
    try:
        from solders.pubkey import Pubkey
        mint = Pubkey.from_string(mint_pubkey)
        resp = client.get_account_info(mint)
        raw = _get_resp_value(resp)
        if raw is None:
            return False
        data = getattr(raw, "data", None)
        if data is None:
            return False
        if hasattr(data, "value"):
            b = data.value
        elif isinstance(data, (list, tuple)):
            b = bytes(data)
        else:
            b = base64.b64decode(data) if isinstance(data, str) else data
        if not isinstance(b, bytes) or len(b) < 46:
            return False
        # SPL Mint: mint_authority option (1) + pubkey(32), supply(8), decimals(1), is_initialized(1)
        if b[0] != 1:
            return False
        authority = _pubkey_from_bytes(b[1:33])
        return authority == wallet
    except Exception:
        return False


def _count_distributed_scam_nft(
    wallet: str,
    scam_mints: set[str],
    sigs_list: list[Any],
    get_tx: Any,
) -> int:
    """Count outbound transfers of scam NFTs from meta pre/post token balances. get_tx(sig_entry) returns tx response."""
    if not scam_mints:
        return 0
    distributed = 0
    for sig_entry in sigs_list:
        try:
            tx = get_tx(sig_entry)
            tx_value = _get_resp_value(tx) if tx else None
        except Exception:
            continue
        if tx_value is None:
            continue
        meta = getattr(tx_value, "meta", None) or (tx_value.get("meta") if isinstance(tx_value, dict) else None)
        if meta is None:
            continue
        pre = getattr(meta, "pre_token_balances", None) or getattr(meta, "preTokenBalances", None)
        if pre is None and isinstance(meta, dict):
            pre = meta.get("pre_token_balances") or meta.get("preTokenBalances")
        post = getattr(meta, "post_token_balances", None) or getattr(meta, "postTokenBalances", None)
        if post is None and isinstance(meta, dict):
            post = meta.get("post_token_balances") or meta.get("postTokenBalances")
        if not pre or not post:
            continue
        pre_list = list(pre) if not isinstance(pre, list) else pre
        post_list = list(post) if not isinstance(post, list) else post
        pre_by_idx: dict[int, Any] = {}
        for b in pre_list:
            idx = getattr(b, "account_index", None) or (b.get("accountIndex") if isinstance(b, dict) else None)
            if idx is not None:
                pre_by_idx[int(idx)] = b
        for b in post_list:
            idx = getattr(b, "account_index", None) or (b.get("accountIndex") if isinstance(b, dict) else None)
            if idx is None:
                continue
            idx = int(idx)
            pre_b = pre_by_idx.get(idx)
            if pre_b is None:
                continue
            mint = getattr(b, "mint", None) or (b.get("mint") if isinstance(b, dict) else None)
            if mint not in scam_mints:
                continue
            owner = getattr(b, "owner", None) or (b.get("owner") if isinstance(b, dict) else None)
            if owner != wallet:
                continue
            ui_amount = getattr(b, "ui_token_amount", None) or getattr(b, "uiTokenAmount", None)
            if isinstance(ui_amount, dict):
                post_amt = float(ui_amount.get("ui_amount") or ui_amount.get("uiAmount") or 0)
            else:
                post_amt = float(getattr(ui_amount, "ui_amount", 0) or getattr(ui_amount, "uiAmount", 0))
            pre_ui = getattr(pre_b, "ui_token_amount", None) or getattr(pre_b, "uiTokenAmount", None)
            if isinstance(pre_ui, dict):
                pre_amt = float(pre_ui.get("ui_amount") or pre_ui.get("uiAmount") or 0)
            else:
                pre_amt = float(getattr(pre_ui, "ui_amount", 0) or getattr(pre_ui, "uiAmount", 0))
            if pre_amt > post_amt:
                distributed += 1
                if distributed >= MASS_DISTRIBUTE_THRESHOLD * 2:
                    return distributed
    return distributed


def detect_nft_scam_role(wallet: str) -> dict[str, Any]:
    """
    Determine NFT scam role: victim (received only), participant, or scammer (minted/creator/mass distribute).

    Returns:
        received_scam_nft: count of scam NFTs currently held
        minted_scam_nft: count of those that we minted (we are mint authority)
        distributed_scam_nft: count of outbound transfers of scam NFTs
        is_creator: True if we appear in Metaplex creators for any scam NFT
        role: "victim" | "participant" | "scammer" | "none"
    """
    from solders.pubkey import Pubkey
    from solders.signature import Signature
    from solana.rpc.api import Client

    wallet = (wallet or "").strip()
    empty = {
        "received_scam_nft": 0,
        "minted_scam_nft": 0,
        "distributed_scam_nft": 0,
        "is_creator": False,
        "role": ROLE_NONE,
    }
    if not wallet:
        return empty

    collections_blacklist = _load_scam_collections()
    if not collections_blacklist:
        return empty

    rpc_url = (os.getenv("SOLANA_RPC_URL") or "").strip() or "https://api.devnet.solana.com"
    try:
        client = Client(rpc_url)
        pubkey = Pubkey.from_string(wallet)
    except Exception as e:
        logger.warning("nft_scam_detector_init_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    try:
        from solana.rpc.types import TokenAccountOpts
        token_program = Pubkey.from_string(TOKEN_PROGRAM_ID)
        token_resp = client.get_token_accounts_by_owner(
            pubkey,
            TokenAccountOpts(program_id=token_program, encoding="jsonParsed"),
        )
        token_value = _get_resp_value(token_resp)
    except Exception as e:
        logger.debug("nft_scam_detector_token_accounts_failed", wallet=wallet[:16] + "...", error=str(e))
        return empty

    nft_mints = _get_nft_mints_from_token_accounts(token_value)
    if not nft_mints:
        return empty

    received_scam_nft = 0
    minted_scam_nft = 0
    is_creator = False
    scam_mints_for_distribution: set[str] = set()

    for mint in nft_mints:
        try:
            mint_pubkey = Pubkey.from_string(mint)
            meta_pda = _metadata_pda(mint_pubkey)
            meta_resp = client.get_account_info(meta_pda, encoding="base64")
            raw = _get_resp_value(meta_resp)
            if raw is None:
                continue
            data = getattr(raw, "data", None)
            if data is None:
                continue
            b = None
            if hasattr(data, "value"):
                b = data.value
            elif isinstance(data, str):
                b = base64.b64decode(data)
            if not b or not isinstance(b, bytes):
                continue
            creators, collection_key = _parse_metadata_creators_and_collection(b)
            if collection_key not in collections_blacklist:
                continue
            scam_mints_for_distribution.add(mint)
            received_scam_nft += 1
            if wallet in creators:
                is_creator = True
            if _is_mint_authority(mint, wallet, client):
                minted_scam_nft += 1
        except Exception as e:
            logger.debug("nft_scam_detector_mint_check_failed", mint=mint[:16] + "...", error=str(e))
            continue

    distributed_scam_nft = 0
    if scam_mints_for_distribution:
        try:
            sigs_resp = client.get_signatures_for_address(pubkey, limit=MAX_TXS_FOR_DISTRIBUTION)
            sigs_value = _get_resp_value(sigs_resp)
            sigs_list = list(sigs_value)[:MAX_TXS_FOR_DISTRIBUTION] if sigs_value else []

            def get_tx_for_sig(sig_entry: Any) -> Any:
                sig_val = getattr(sig_entry, "signature", None) or (sig_entry.get("signature") if isinstance(sig_entry, dict) else None)
                if sig_val is None:
                    return None
                sig_use = sig_val if (hasattr(sig_val, "value") or type(sig_val).__name__ == "Signature") else Signature.from_string(str(sig_val))
                return client.get_transaction(sig_use, encoding="jsonParsed", max_supported_transaction_version=0)

            distributed_scam_nft = _count_distributed_scam_nft(
                wallet,
                scam_mints_for_distribution,
                sigs_list,
                get_tx_for_sig,
            )
        except Exception as e:
            logger.debug("nft_scam_detector_distribution_failed", wallet=wallet[:16] + "...", error=str(e))

    # Role: scammer > participant > victim > none
    if minted_scam_nft > 0 or is_creator or distributed_scam_nft >= MASS_DISTRIBUTE_THRESHOLD:
        role = ROLE_SCAMMER
    elif received_scam_nft > 0 and distributed_scam_nft > 0:
        role = ROLE_PARTICIPANT
    elif received_scam_nft > 0:
        role = ROLE_VICTIM
    else:
        role = ROLE_NONE

    result = {
        "received_scam_nft": received_scam_nft,
        "minted_scam_nft": minted_scam_nft,
        "distributed_scam_nft": distributed_scam_nft,
        "is_creator": is_creator,
        "role": role,
    }
    if role != ROLE_NONE:
        logger.info(
            "nft_scam_detector_result",
            wallet=wallet[:16] + "...",
            role=role,
            received=received_scam_nft,
            minted=minted_scam_nft,
            distributed=distributed_scam_nft,
            is_creator=is_creator,
        )
    return result
