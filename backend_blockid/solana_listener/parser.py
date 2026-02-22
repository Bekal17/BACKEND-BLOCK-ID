"""
Solana transaction parser — raw RPC payloads to structured features.

Parses getTransaction-style responses into sender, receiver, amount,
timestamp, and optional transaction frequency. Purely structural;
no scoring or risk logic. Supports native SOL transfers and
balance-delta fallback for other flows.
"""

from __future__ import annotations

import base64
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

try:
    import base58 as _base58
except ImportError:
    _base58 = None  # type: ignore[assignment]

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# System Program (native SOL transfers)
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
# Transfer instruction discriminator in System Program
SYSTEM_TRANSFER_DISCRIMINATOR = 2


@dataclass
class TransactionFrequency:
    """
    Aggregated transaction frequency for an address in a given context.

    Populated when parsing a batch; left None when parsing a single tx.
    All counts are over the parsed set (e.g. last N txs or time window).
    """

    tx_count: int
    """Number of transactions involving this address in the context."""
    as_sender_count: int = 0
    """Times this address appeared as sender."""
    as_receiver_count: int = 0
    """Times this address appeared as receiver."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_count": self.tx_count,
            "as_sender_count": self.as_sender_count,
            "as_receiver_count": self.as_receiver_count,
        }


@dataclass
class ParsedTransaction:
    """
    Structured features extracted from a raw Solana transaction.

    Schema is stable and scoring-agnostic. Use for downstream
    analysis, storage, or API responses.
    """

    sender: str
    """Base58 address of the primary sender (fee payer / first signer)."""
    receiver: str
    """Base58 address of the primary receiver (SOL recipient in transfer)."""
    amount: int
    """Transfer amount in lamports (1 SOL = 1_000_000_000 lamports)."""
    timestamp: int | None
    """Unix timestamp (seconds) from blockTime; None if unavailable."""
    transaction_frequency: TransactionFrequency | None = None
    """Optional; set when parsing a batch with frequency context."""
    signature: str | None = None
    """Transaction signature (base58); None if not provided."""
    slot: int | None = None
    """Slot of the block containing the transaction; None if not provided."""
    amount_sol: float = field(init=False)
    """Amount in SOL (derived from amount); for convenience."""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "amount_sol",
            round(self.amount / 1_000_000_000.0, 9),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict; scoring logic must not depend on field order."""
        out: dict[str, Any] = {
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
            "amount_sol": self.amount_sol,
            "timestamp": self.timestamp,
            "signature": self.signature,
            "slot": self.slot,
        }
        if self.transaction_frequency is not None:
            out["transaction_frequency"] = self.transaction_frequency.to_dict()
        return out


def _get_account_keys(
    message: dict[str, Any],
    meta: dict[str, Any] | None = None,
) -> list[str]:
    """
    Resolve accountKeys to a list of base58 strings (handles json vs jsonParsed).
    For versioned transactions, appends meta.loadedAddresses (writable + readonly).
    """
    keys = message.get("accountKeys")
    if not keys:
        return []
    if isinstance(keys[0], str):
        out = list(keys)
    else:
        out = [k.get("pubkey", "") for k in keys if isinstance(k, dict)]
    loaded = (meta or {}).get("loadedAddresses") or {}
    for role in ("writable", "readonly"):
        for addr in loaded.get(role) or []:
            out.append(addr if isinstance(addr, str) else getattr(addr, "pubkey", ""))
    return out


def _get_program_id(account_keys: list[str], instruction: dict[str, Any]) -> str | None:
    """Resolve program id for an instruction (programIdIndex -> account key)."""
    idx = instruction.get("programIdIndex")
    if idx is None or not (0 <= idx < len(account_keys)):
        return None
    return account_keys[idx]


def _decode_system_transfer_data(data_b58: str) -> int | None:
    """Decode System Program transfer instruction data; return lamports or None."""
    if not data_b58:
        return None
    try:
        raw = _b58decode(data_b58)
    except Exception:
        return None
    if len(raw) < 9:
        return None
    if raw[0] != SYSTEM_TRANSFER_DISCRIMINATOR:
        return None
    return int.from_bytes(raw[1:9], "little")


def _b58decode(s: str) -> bytes:
    """Decode base58 instruction data to bytes; fallback to base64 for RPC variance."""
    if _base58 is not None:
        try:
            return _base58.b58decode(s)
        except Exception:
            pass
    try:
        return base64.b64decode(s, validate=True)
    except Exception as e:
        raise ValueError(f"Could not decode instruction data (base58/base64): {e}") from e


def _extract_native_transfer(
    account_keys: list[str],
    instructions: list[dict[str, Any]],
) -> tuple[str | None, str | None, int | None]:
    """
    Find first System Program transfer; return (sender, receiver, lamports).
    """
    for ix in instructions:
        program_id = _get_program_id(account_keys, ix)
        if program_id != SYSTEM_PROGRAM_ID:
            continue
        accounts = ix.get("accounts") or []
        if len(accounts) < 2:
            continue
        from_idx, to_idx = int(accounts[0]), int(accounts[1])
        if not (0 <= from_idx < len(account_keys) and 0 <= to_idx < len(account_keys)):
            continue
        data = ix.get("data")
        if data is None:
            continue
        lamports = _decode_system_transfer_data(data)
        if lamports is None:
            continue
        return account_keys[from_idx], account_keys[to_idx], lamports
    return None, None, None


def _extract_from_balance_delta(
    account_keys: list[str],
    pre_balances: list[int],
    post_balances: list[int],
    num_required_signatures: int,
) -> tuple[str | None, str | None, int | None]:
    """
    Infer primary sender/receiver and amount from balance deltas.
    Sender = first signer; receiver = account with largest positive delta (excluding sender).
    """
    if not account_keys or len(pre_balances) != len(account_keys) or len(post_balances) != len(account_keys):
        return None, None, None
    sender = account_keys[0] if num_required_signatures else None
    deltas = [post_balances[i] - pre_balances[i] for i in range(len(account_keys))]
    # Receiver: largest positive delta that isn't the fee payer (often index 0 pays fee and has negative delta).
    best_idx = None
    best_delta = 0
    for i, d in enumerate(deltas):
        if d <= 0:
            continue
        if d > best_delta:
            best_delta = d
            best_idx = i
    if best_idx is None or best_delta == 0:
        return sender, None, None
    receiver = account_keys[best_idx]
    return sender, receiver, best_delta


def _get_message_and_meta(raw: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return (transaction.message, meta) from getTransaction-style result."""
    tx_obj = raw.get("transaction")
    if not tx_obj or not isinstance(tx_obj, dict):
        return None, None
    message = tx_obj.get("message")
    if not message or not isinstance(message, dict):
        return None, None
    meta = raw.get("meta")
    if not isinstance(meta, dict):
        meta = None
    return message, meta


def parse(raw: dict[str, Any]) -> ParsedTransaction | None:
    """
    Parse a single raw getTransaction-style result into structured features.

    Handles legacy and versioned transactions; uses native Transfer
    instruction when present, otherwise falls back to balance deltas.
    Returns None if the payload cannot be parsed (missing message, etc.).
    """
    message, meta = _get_message_and_meta(raw)
    if not message:
        return None

    account_keys = _get_account_keys(message, meta)
    if not account_keys:
        return None

    instructions = message.get("instructions") or []
    # Include inner instructions for transfer (e.g. CPI from another program)
    inner = (meta or {}).get("innerInstructions") or []
    for inner_block in inner:
        instructions.extend(inner_block.get("instructions") or [])

    sender, receiver, amount = _extract_native_transfer(account_keys, instructions)
    if sender is None or receiver is None or amount is None:
        pre = (meta or {}).get("preBalances") or []
        post = (meta or {}).get("postBalances") or []
        header = message.get("header") or {}
        num_sig = header.get("numRequiredSignatures", 1)
        sender, receiver, amount = _extract_from_balance_delta(
            account_keys, pre, post, num_sig
        )
        if sender is None:
            sender = account_keys[0]
        if receiver is None or amount is None:
            receiver = receiver or account_keys[1] if len(account_keys) > 1 else sender
            amount = amount or 0

    timestamp = raw.get("blockTime")
    if timestamp is not None and not isinstance(timestamp, int):
        try:
            timestamp = int(timestamp)
        except (TypeError, ValueError):
            timestamp = None

    sig_list = (raw.get("transaction") or {})
    if isinstance(sig_list, dict):
        sig_list = sig_list.get("signatures") or []
    signature = sig_list[0] if isinstance(sig_list, list) and sig_list else None
    slot = raw.get("slot")

    return ParsedTransaction(
        sender=sender,
        receiver=receiver,
        amount=amount,
        timestamp=timestamp,
        transaction_frequency=None,
        signature=signature,
        slot=int(slot) if slot is not None else None,
    )


def _compute_frequency(parsed_list: list[ParsedTransaction]) -> dict[str, TransactionFrequency]:
    """Build per-address frequency from a list of parsed transactions."""
    addr_tx_count: defaultdict[str, int] = defaultdict(int)
    addr_sender_count: defaultdict[str, int] = defaultdict(int)
    addr_receiver_count: defaultdict[str, int] = defaultdict(int)
    for p in parsed_list:
        addr_tx_count[p.sender] += 1
        addr_tx_count[p.receiver] += 1
        addr_sender_count[p.sender] += 1
        addr_receiver_count[p.receiver] += 1
    out: dict[str, TransactionFrequency] = {}
    for addr in set(addr_tx_count):
        out[addr] = TransactionFrequency(
            tx_count=addr_tx_count[addr],
            as_sender_count=addr_sender_count[addr],
            as_receiver_count=addr_receiver_count[addr],
        )
    return out


def parse_batch(
    raw_list: list[dict[str, Any]],
    *,
    include_frequency: bool = True,
) -> list[ParsedTransaction]:
    """
    Parse a list of raw getTransaction-style results and optionally attach
    transaction frequency per address (over this batch).

    Skips unparseable items; returned list may be shorter than input.
    """
    parsed_list: list[ParsedTransaction] = []
    for raw in raw_list:
        p = parse(raw)
        if p is not None:
            parsed_list.append(p)

    if not include_frequency or not parsed_list:
        return parsed_list

    freq_map = _compute_frequency(parsed_list)
    result: list[ParsedTransaction] = []
    for p in parsed_list:
        sender_freq = freq_map.get(p.sender)
        receiver_freq = freq_map.get(p.receiver)
        # Attach frequency for the primary sender (most useful for downstream).
        tx_freq = sender_freq or receiver_freq
        result.append(
            ParsedTransaction(
                sender=p.sender,
                receiver=p.receiver,
                amount=p.amount,
                timestamp=p.timestamp,
                transaction_frequency=tx_freq,
                signature=p.signature,
                slot=p.slot,
            )
        )
    return result
