# Trust Oracle — Solana Program Interface Spec

Anchor-compatible interface for an on-chain trust oracle. **Spec and reference only; no deployment or build.**

## Contents

- **SPEC.md** — Account layout, instructions, PDA seeds, IDL snippet.
- **lib.rs** — Anchor-style interface (accounts, enums, instruction contexts). Use as the contract surface when implementing the program.

## Summary

| Item | Description |
|------|-------------|
| **Instructions** | `update_trust_score(trust_score, risk_level)` \| `get_trust_score()` |
| **Account** | `TrustScoreAccount`: wallet_pubkey, trust_score (u8), risk_level (u8 enum), last_updated (i64), oracle_pubkey |
| **PDA** | `["trust_score", oracle_pubkey, wallet_pubkey]` |
| **Authority** | Only the oracle pubkey used in the PDA may sign `update_trust_score`. |

To deploy later: add `Cargo.toml`, replace `declare_id!`, run `anchor build`.
