# BlockID API Reference

## Overview

The BlockID API provides trust scores for Solana wallets and manages wallet tracking. It serves read-only data from the local database (populated by the publish pipeline and trust-score sync worker) and exposes endpoints for wallet registration, CSV import, analytics reports, and debug status. No API keys or authentication are required.

---

## Endpoints

### GET /api/trust-score/{wallet}

**Description:** Returns trust score for a wallet. Reads from local database (DB first, no RPC in hot path). Returns 404 if the wallet has no cached score.

**Path parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| wallet | string | Solana public key (base58) |

**Response:**
```json
{
  "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "score": 72,
  "risk": 0,
  "reason_codes": ["NEW_WALLET", "LOW_ACTIVITY"],
  "updated_at": "2026-02-19T12:00:00Z",
  "oracle_pubkey": "...",
  "pda": "..."
}
```

**Errors:**
- 400 — wallet must be non-empty, or invalid wallet pubkey
- 404 — Trust score not found for this wallet
- 503 — Server misconfiguration (ORACLE_PRIVATE_KEY or ORACLE_PROGRAM_ID required)

---

### POST /api/trust-score/list

**Description:** Batch fetch trust scores from local database. One batch query; missing wallets return `status="not_scored"`. Optimized for fast response (< 50ms for 100 wallets).

**Request body:**
```json
{
  "wallets": ["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", "..."]
}
```

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| wallets | array of string | Yes | 1–100 wallet pubkeys |

**Response:**
```json
[
  {
    "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    "score": 72,
    "risk": 0,
    "reason_codes": ["NEW_WALLET"],
    "updated_at": "2026-02-19T12:00:00Z",
    "oracle_pubkey": "...",
    "pda": "..."
  },
  {
    "wallet": "UnknownWallet...",
    "status": "not_scored"
  }
]
```

**Errors:**
- 503 — Server misconfiguration (ORACLE_PRIVATE_KEY or ORACLE_PROGRAM_ID required)

---

### GET /wallet/{address}

**Description:** Returns the latest trust score and anomaly flags for a wallet. Reads from main database only; does not compute scores. Merges reason_codes from trust_scores metadata and db_wallet_tracking when available.

**Path parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| address | string | Solana wallet address |

**Response:**
```json
{
  "address": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "trust_score": 72.5,
  "computed_at": 1708336800,
  "flags": [],
  "reason_codes": ["NEW_WALLET", "LOW_ACTIVITY"]
}
```

**Errors:**
- 400 — address must be non-empty
- 404 — No trust score found for wallet

---

### GET /health

**Description:** Liveness probe. Returns status to confirm API is up.

**Response:**
```json
{
  "status": "ok"
}
```

---

### POST /track-wallet

**Description:** Register a wallet for monitoring. Inserts into tracked_wallets (main agent scheduler). Returns 201 when newly added, 200 when already tracked.

**Request body:**
```json
{
  "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
}
```

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| wallet | string | Yes | 8–64 chars, valid Solana address |

**Response:**
```json
{
  "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "registered": true
}
```

**Errors:**
- 400 — wallet must be non-empty, or invalid Solana wallet address

---

### POST /track_wallet

**Description:** Add a wallet to Step 2 tracking (db_wallet_tracking). Validates wallet with Solana PublicKey. Returns 201 when newly added, 200 when already tracked.

**Request body:**
```json
{
  "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "label": "optional label"
}
```

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| wallet | string | Yes | 8–64 chars, valid Solana address |
| label | string | No | Max 256 chars |

**Response:**
```json
{
  "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
  "label": "optional label",
  "registered": true
}
```

**Errors:**
- 400 — Invalid Solana wallet (from ValueError)
- 500 — Failed to add wallet

---

### GET /tracked_wallets

**Description:** Return all wallets in Step 2 tracking. Each item has id, wallet, label, last_score, last_risk, last_checked, is_active.

**Response:**
```json
[
  {
    "id": 1,
    "wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
    "label": "my wallet",
    "last_score": 75,
    "last_risk": "low",
    "last_checked": 1708336800,
    "is_active": true
  }
]
```

**Errors:**
- 500 — Failed to list wallets

---

### POST /import_wallets_csv

**Description:** Import wallets from CSV. Expected columns: wallet, label (label optional). Invalid wallets are rejected; duplicates are skipped.

**Request:** multipart/form-data with `file` (CSV upload)

**Response:**
```json
{
  "imported": 10,
  "duplicates": 2,
  "invalid": ["bad_address_xyz"]
}
```

**Errors:**
- 400 — CSV must have a 'wallet' column
- 500 — Failed to import CSV

---

### GET /wallet_report/{wallet}

**Description:** Run full analytics pipeline for a wallet (scan → risk → trust). Returns metrics, risk, score, risk_label. Does not publish to the oracle.

**Path parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| wallet | string | Solana wallet address |

**Response:** Raw analytics dict (structure varies by pipeline output).

**Errors:**
- 400 — wallet must be non-empty, or invalid Solana wallet address
- 500 — Analytics failed

---

### GET /debug/wallet_status/{wallet}

**Description:** Debug endpoint. Check if wallet is in tracked_wallets (Step 2) and if its trust score PDA exists on-chain. Uses db_wallet_tracking and Solana RPC (ORACLE_PROGRAM_ID, ORACLE_PRIVATE_KEY, SOLANA_RPC_URL).

**Path parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| wallet | string | Solana wallet address |

**Response:**
```json
{
  "in_database": true,
  "onchain_pda_exists": true,
  "last_score": 75
}
```

**Errors:**
- 400 — wallet must be non-empty, or invalid Solana wallet address

---

## Authentication

No authentication is used. The API does not require API keys, Bearer tokens, or JWT. All endpoints are unauthenticated. Middleware supports optional API key or JWT validation in the future but it is not currently implemented.

---

## Rate Limits

No rate limiting is implemented. The middleware module documents optional rate limiting per client or per endpoint, but no throttling logic is present in the codebase.

---

## Example curl Commands

```bash
# Health check
curl -s http://localhost:8000/health

# Get trust score (API prefix)
curl -s http://localhost:8000/api/trust-score/7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU

# Batch trust scores
curl -s -X POST http://localhost:8000/api/trust-score/list \
  -H "Content-Type: application/json" \
  -d '{"wallets": ["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"]}'

# Get wallet (legacy endpoint)
curl -s http://localhost:8000/wallet/7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU

# Track wallet (Step 2)
curl -s -X POST http://localhost:8000/track_wallet \
  -H "Content-Type: application/json" \
  -d '{"wallet": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", "label": "test"}'

# List tracked wallets
curl -s http://localhost:8000/tracked_wallets

# Import CSV
curl -s -X POST http://localhost:8000/import_wallets_csv \
  -F "file=@wallets.csv"

# Wallet report (analytics)
curl -s http://localhost:8000/wallet_report/7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU

# Debug wallet status
curl -s http://localhost:8000/debug/wallet_status/7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU
```

---

## Example Python Client

```python
import requests

BASE_URL = "http://localhost:8000"

def get_trust_score(wallet: str) -> dict:
    r = requests.get(f"{BASE_URL}/api/trust-score/{wallet}")
    r.raise_for_status()
    return r.json()

def list_trust_scores(wallets: list[str]) -> list[dict]:
    r = requests.post(f"{BASE_URL}/api/trust-score/list", json={"wallets": wallets})
    r.raise_for_status()
    return r.json()

def track_wallet(wallet: str, label: str | None = None) -> dict:
    r = requests.post(f"{BASE_URL}/track_wallet", json={"wallet": wallet, "label": label})
    r.raise_for_status()
    return r.json()

def get_tracked_wallets() -> list[dict]:
    r = requests.get(f"{BASE_URL}/tracked_wallets")
    r.raise_for_status()
    return r.json()

# Usage
score = get_trust_score("7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU")
print(score["score"], score["reason_codes"])

batch = list_trust_scores(["addr1", "addr2"])
track_wallet("addr3", label="my_wallet")
wallets = get_tracked_wallets()
```

---

## Error Codes

| Status | Meaning |
|--------|---------|
| 200 | OK |
| 201 | Created (wallet newly registered) |
| 400 | Bad request — invalid or missing parameters, invalid wallet address |
| 404 | Not found — no trust score for wallet |
| 500 | Internal server error — DB failure, analytics failure, etc. |
| 503 | Service unavailable — server misconfiguration (oracle/program ID required) |

All error responses use JSON:
```json
{
  "detail": "Error message"
}
```

---

## Data Flow

```
┌─────────────┐     GET /api/trust-score/{wallet}     ┌──────────────────┐
│   Client    │ ───────────────────────────────────▶ │  Trust Score API  │
└─────────────┘                                      └────────┬─────────┘
                                                              │
                                                              │ get_trust_score_timeline()
                                                              ▼
┌─────────────┐     POST /track_wallet               ┌──────────────────┐
│   Client    │ ───────────────────────────────────▶ │  Wallet Tracking │
└─────────────┘                                      │  (db_wallet_     │
                                                     │   tracking)      │
                                                     └────────┬─────────┘
                                                              │
                         ┌────────────────────────────────────┼────────────────────────────────────┐
                         │                                    │                                    │
                         ▼                                    ▼                                    ▼
              ┌──────────────────┐               ┌──────────────────┐               ┌──────────────────┐
              │ Main DB          │               │ publish_scores   │               │ trust_score_sync │
              │ (blockid.db)     │               │ publish_one_     │               │ (5 min loop)     │
              │ trust_scores     │               │ wallet           │               │                  │
              │ wallet_profiles  │               │                  │               │ getMultipleAccts │
              │ transactions     │               │ update_trust_    │               │ on-chain PDA     │
              └──────────────────┘               │ score → Solana   │               └────────┬─────────┘
                                                 └────────┬─────────┘                        │
                                                          │                                  │
                                                          ▼                                  ▼
                                                 ┌──────────────────────────────────────────────┐
                                                 │           Solana Anchor PDA                  │
                                                 │           (TrustScoreAccount)                │
                                                 └──────────────────────────────────────────────┘
                                                          ▲
                                                          │ ML scoring
                                                 ┌────────┴─────────┐
                                                 │ publish_scores   │
                                                 │ train_blockid_   │
                                                 │ model → predict  │
                                                 └──────────────────┘
```

- **Wallet Tracking DB:** `track_wallet`, `tracked_wallets`, `import_wallets_csv` read/write `db_wallet_tracking` (tracked_wallets, score_history). Used by batch publish to decide which wallets to publish.
- **Main DB:** `trust_scores` table stores cached scores. API reads from here for `/wallet/{address}` and `/api/trust-score/*`. Populated by `publish_one_wallet` (insert after publish) and `trust_score_sync` (on-chain PDA → DB).
- **Oracle PDA:** Scores are published to Solana via `update_trust_score` instruction. `trust_score_sync` polls on-chain PDAs every 5 minutes and inserts/updates `trust_scores`.
- **ML scoring engine:** `publish_scores.py` loads `blockid_model.joblib`, predicts scam probability, converts to trust score, publishes to PDA. Not called directly by the API; runs as a batch or scheduled job.
