"""
Data models for Solana listener output.

Responsibilities:
- Define dataclasses/Pydantic models for normalized transactions.
- Include fields such as signature, slot, block_time, accounts, instructions,
  token changes, and any metadata needed by the analysis engine.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SignatureInfo:
    """
    Normalized transaction signature info from getSignaturesForAddress.

    Mirrors Solana RPC response fields; used as the unit of work
    emitted by the listener to the analysis engine.
    """

    signature: str
    slot: int
    err: Any  # None if success; dict/object from RPC if failed
    block_time: int | None  # Unix timestamp; None if not available
    memo: str | None
    confirmation_status: str | None  # processed | confirmed | finalized

    @classmethod
    def from_rpc_item(cls, item: dict[str, Any]) -> "SignatureInfo":
        """Build from a single getSignaturesForAddress result item."""
        return cls(
            signature=item["signature"],
            slot=int(item["slot"]),
            err=item.get("err"),
            block_time=item.get("blockTime"),
            memo=item.get("memo"),
            confirmation_status=item.get("confirmationStatus"),
        )
