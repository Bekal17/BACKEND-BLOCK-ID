# Trust Oracle — Solana Program Interface Spec

Anchor-compatible interface for an on-chain trust oracle. No deployment; spec only.

---

## Program ID

- **Placeholder:** `TRUSTxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` (replace with keypair-derived ID at deploy time).

---

## Account Structure

### `TrustScoreAccount`

Single account per wallet; PDA derived from `["trust_score", oracle_pubkey, wallet_pubkey]`.

| Field           | Type   | Description                                      |
|----------------|--------|--------------------------------------------------|
| `wallet_pubkey`| `Pubkey` | Wallet whose trust score is stored.             |
| `trust_score`  | `u8`   | Score 0–100.                                     |
| `risk_level`   | `u8`   | Enum: `RiskLevel` (0=Low, 1=Medium, 2=High, 3=Critical). |
| `last_updated` | `i64`  | Unix timestamp (seconds) of last update.         |
| `oracle_pubkey`| `Pubkey` | Oracle authority that may update this account. |

- **Space:** 32 + 1 + 1 + 8 + 32 = **74** bytes (plus 8-byte discriminator if using Anchor `account`).
- **Mutability:** Only `oracle_pubkey` may sign for updates.

---

## Enums

### `RiskLevel` (u8)

| Variant   | Value | Meaning (score band) |
|----------|-------|----------------------|
| `Low`    | 0     | Score ≥ 70           |
| `Medium` | 1     | 50 ≤ score < 70      |
| `High`   | 2     | 30 ≤ score < 50      |
| `Critical` | 3   | Score < 30           |

---

## Instructions

### `update_trust_score`

Writes trust score and risk level for a wallet. Callable only by the oracle authority.

**Accounts:**

| Account           | Writable | Signer | Description                    |
|------------------|----------|--------|--------------------------------|
| `trust_score_account` | ✓   | No  | PDA `TrustScoreAccount` (create if missing). |
| `oracle`         | —        | ✓     | Must match `trust_score_account.oracle_pubkey`. |
| `wallet`         | —        | No    | Wallet pubkey (must match `trust_score_account.wallet_pubkey`). |
| `system_program` | —        | No    | For PDA creation.             |

**Args:**

| Name          | Type | Description        |
|---------------|------|--------------------|
| `trust_score` | `u8` | Value 0–100.       |
| `risk_level`  | `u8` | `RiskLevel` enum.  |

**Constraints:**

- `trust_score` ∈ [0, 100].
- `risk_level` ∈ [0, 3].
- `clock.unix_timestamp` used for `last_updated` (or passed as `i64` arg).

---

### `get_trust_score`

Read-only: no state change. Client passes PDA (or oracle + wallet for PDA derivation); program validates PDA and returns. No args.

**Accounts:**

| Account             | Writable | Signer | Description        |
|--------------------|----------|--------|--------------------|
| `trust_score_account` | No    | No     | PDA to read (seeds: `["trust_score", oracle, wallet]`). |
| `oracle`           | No       | No     | Oracle pubkey (for PDA seeds). |
| `wallet`           | No       | No     | Wallet pubkey (for PDA seeds). |

**Args:** None.

**Return / client behavior:**

- Client reads `trust_score_account` (via RPC or CPI) and decodes:
  - `trust_score: u8`
  - `risk_level: u8`
  - `last_updated: i64`
  - `wallet_pubkey: Pubkey`
  - `oracle_pubkey: Pubkey`

---

## PDA Seeds

```
trust_score_account = PDA(
  program_id,
  ["trust_score", oracle_pubkey, wallet_pubkey]
)
```

- Same `(oracle_pubkey, wallet_pubkey)` always resolves to one account per oracle per wallet.

---

## Anchor IDL Snippet (reference)

```json
{
  "version": "0.29.0",
  "name": "trust_oracle",
  "instructions": [
    {
      "name": "updateTrustScore",
      "accounts": [
        { "name": "trustScoreAccount", "writable": true },
        { "name": "oracle", "writable": false, "signer": true },
        { "name": "wallet", "writable": false },
        { "name": "systemProgram", "writable": false }
      ],
      "args": [
        { "name": "trustScore", "type": "u8" },
        { "name": "riskLevel", "type": "u8" }
      ]
    },
    {
      "name": "getTrustScore",
      "accounts": [
        { "name": "trustScoreAccount", "writable": false },
        { "name": "oracle", "writable": false },
        { "name": "wallet", "writable": false }
      ],
      "args": []
    }
  ],
  "accounts": [
    {
      "name": "TrustScoreAccount",
      "type": {
        "kind": "struct",
        "fields": [
          { "name": "walletPubkey", "type": "pubkey" },
          { "name": "trustScore", "type": "u8" },
          { "name": "riskLevel", "type": "u8" },
          { "name": "lastUpdated", "type": "i64" },
          { "name": "oraclePubkey", "type": "pubkey" }
        ]
      }
    }
  ],
  "types": [
    {
      "name": "RiskLevel",
      "type": {
        "kind": "enum",
        "variants": [
          { "name": "Low" },
          { "name": "Medium" },
          { "name": "High" },
          { "name": "Critical" }
        ]
      }
    }
  ]
}
```

---

## Security Notes (spec-level)

- Only the account’s `oracle_pubkey` may sign `update_trust_score`.
- `get_trust_score` is read-only; no signer required.
- PDA derivation prevents spoofing of account address; clients must use correct `(program_id, oracle, wallet)` seeds.
