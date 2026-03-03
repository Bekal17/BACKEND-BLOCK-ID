# BlockID Exchange Integration API Spec

**Purpose:** Allow exchanges and marketplaces to check wallet trust scores before deposit, withdraw, or trade.

**Stack:** FastAPI · Tables: `trust_scores`, `wallet_reasons`, `wallet_history`

---

## SECTION 1 — Base URL & Authentication

### Base URL

```
https://api.blockidscore.fun/v1
```

### Authentication

All requests require an API key in the header:

```
Authorization: Bearer YOUR_API_KEY
```

| Header        | Required | Description                    |
|---------------|----------|--------------------------------|
| `Authorization` | Yes      | `Bearer <API_KEY>`             |
| `Content-Type`  | For POST | `application/json`             |

**Obtaining API keys:** Contact BlockID to register as an exchange partner. Each partner receives a unique API key with configurable rate limits and quotas. See [B2B API Pricing](b2b_api_pricing.md) for tiers (Starter, Growth, Enterprise).

---

## SECTION 2 — Check Wallet Trust

**Endpoint:** `GET /wallet/{wallet}`

**Use case:** Exchange checks wallet before accepting deposit.

### Request

```
GET /v1/wallet/{wallet}
Authorization: Bearer YOUR_API_KEY
```

| Path Parameter | Type   | Description        |
|----------------|--------|--------------------|
| `wallet`       | string | Solana wallet address (base58) |

### Response

| Field        | Type   | Description                          |
|--------------|--------|--------------------------------------|
| `wallet`     | string | Wallet address                       |
| `score`      | number | Trust score 0–100 (higher = safer)   |
| `risk_level` | number | 0=Low, 1=Medium, 2=High, 3=Critical  |
| `badge`      | string | Risk badge (e.g. `SCAM_SUSPECTED`)   |
| `confidence` | number | Model confidence 0–1                 |
| `updated_at` | string | ISO 8601 timestamp of last update    |

### Example

```json
{
  "wallet": "ABC123...",
  "score": 20,
  "risk_level": 3,
  "badge": "SCAM_SUSPECTED",
  "confidence": 0.85,
  "updated_at": "2025-02-26T12:00:00Z"
}
```

---

## SECTION 3 — Reason Details

**Endpoint:** `GET /wallet/{wallet}/reasons`

**Use case:** Exchange can audit risk for compliance and dispute resolution.

### Request

```
GET /v1/wallet/{wallet}/reasons
Authorization: Bearer YOUR_API_KEY
```

### Response

| Field          | Type   | Description                    |
|----------------|--------|--------------------------------|
| `wallet`       | string | Wallet address                 |
| `reason_codes` | array  | List of reason codes           |
| `weights`      | object | Reason code → weight mapping   |
| `tx_hash`      | array  | Transaction signatures (evidence) |
| `evidence_links` | array | Solscan/explorer URLs          |

### Example

```json
{
  "wallet": "ABC123...",
  "reason_codes": ["SCAM_CLUSTER_MEMBER", "DRAINER_INTERACTION"],
  "weights": {
    "SCAM_CLUSTER_MEMBER": 0.72,
    "DRAINER_INTERACTION": 0.65
  },
  "tx_hash": ["5abc...", "7def..."],
  "evidence_links": [
    "https://solscan.io/tx/5abc...",
    "https://solscan.io/tx/7def..."
  ]
}
```

---

## SECTION 4 — Batch Check

**Endpoint:** `POST /wallet/batch_check`

**Use case:** Exchange deposit queue — check many wallets in one request.

### Request

```
POST /v1/wallet/batch_check
Authorization: Bearer YOUR_API_KEY
Content-Type: application/json

["wallet1", "wallet2", "wallet3", ...]
```

| Body   | Type  | Description                  |
|--------|-------|------------------------------|
| wallets | array | List of wallet addresses (max 100 per request) |

### Response

```json
{
  "results": [
    {
      "wallet": "wallet1",
      "score": 75,
      "risk_level": 1,
      "badge": "LOW_RISK",
      "confidence": 0.92,
      "updated_at": "2025-02-26T12:00:00Z"
    },
    {
      "wallet": "wallet2",
      "score": 15,
      "risk_level": 3,
      "badge": "SCAM_SUSPECTED",
      "confidence": 0.88,
      "updated_at": "2025-02-26T11:55:00Z"
    }
  ]
}
```

Wallets not found return `score: null`, `risk_level: null`, `badge: "UNKNOWN"`.

---

## SECTION 5 — Real-time Alert Webhook

**Endpoint:** `POST /webhook/register`

**Use case:** BlockID notifies the exchange when a tracked wallet’s risk increases.

### Register Webhook

```
POST /v1/webhook/register
Authorization: Bearer YOUR_API_KEY
Content-Type: application/json

{
  "url": "https://your-exchange.com/blockid/webhook",
  "events": ["risk_increase"],
  "wallets": ["wallet1", "wallet2"]
}
```

| Field   | Type   | Description                                 |
|---------|--------|---------------------------------------------|
| `url`   | string | HTTPS endpoint to receive events            |
| `events` | array | `["risk_increase"]` (optional filters)      |
| `wallets` | array | Optional wallet allowlist (empty = all tracked) |

### Webhook Event Payload

When risk increases, BlockID sends a POST to your URL:

```json
{
  "wallet": "ABC123...",
  "old_score": 80,
  "new_score": 15,
  "reason": "SCAM_CLUSTER_MEMBER",
  "timestamp": "2025-02-26T12:05:00Z"
}
```

**Verification:** Webhook requests include `X-BlockID-Signature` header (HMAC-SHA256 of body). Verify signature before processing.

---

## SECTION 6 — Investigation Report

**Endpoint:** `GET /wallet/{wallet}/report`

**Use case:** Compliance team review. Returns a PDF report link.

### Request

```
GET /v1/wallet/{wallet}/report
Authorization: Bearer YOUR_API_KEY
```

### Response

```json
{
  "wallet": "ABC123...",
  "report_url": "https://api.blockidscore.fun/v1/wallet/ABC123.../report/download?token=xxx",
  "expires_at": "2025-02-26T13:00:00Z"
}
```

Or direct PDF binary: `Content-Type: application/pdf` with `Content-Disposition: attachment`.

---

## SECTION 7 — SLA & Rate Limits

| Tier       | Requests/min | Batch max |
|------------|--------------|-----------|
| Standard   | 1,000        | 100       |
| Enterprise | Custom       | Custom    |

**Recommendations:**
- Use `POST /wallet/batch_check` for deposit queues instead of many single `GET /wallet/{wallet}` calls.
- Cache responses; trust scores typically update on the order of minutes.
- Implement exponential backoff on 429 responses.

---

## SECTION 8 — Error Codes

| Code | Meaning            | Description                          |
|------|--------------------|--------------------------------------|
| `401` | Invalid key        | Missing or invalid `Authorization`   |
| `404` | Wallet not found   | No trust score for this wallet       |
| `429` | Rate limit         | Too many requests; retry after delay |

### Error Response Format

```json
{
  "error": "wallet_not_found",
  "message": "No trust score found for wallet ABC123...",
  "code": 404
}
```

---

## SECTION 9 — Security

| Item           | Status | Notes                                      |
|----------------|--------|--------------------------------------------|
| API key per partner | ✔ | Unique key per exchange                    |
| Request signing | ✔ | Optional HMAC for webhook verification     |
| Audit logs     | ✔ | All API calls logged (partner, endpoint, wallet) |

**Best practices:**
- Store API keys in secrets manager, never in code.
- Use HTTPS only.
- Rotate keys periodically.

---

## SECTION 10 — Future Upgrades

- Cross-chain support (EVM, etc.)
- KYT (Know Your Transaction) integration
- Exchange-specific risk thresholds
- Risk scoring customization per partner

---

## Appendix — Badge Reference

| Badge               | Risk Level | Description                    |
|---------------------|------------|--------------------------------|
| `SAFE`              | 0          | No significant risk detected   |
| `LOW_RISK`          | 1          | Minor flags                    |
| `MEDIUM_RISK`       | 2          | Multiple risk indicators       |
| `SCAM_SUSPECTED`    | 3          | High confidence scam link      |
| `UNKNOWN`           | —          | No data for wallet             |
