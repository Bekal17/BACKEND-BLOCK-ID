# BlockID UI API Spec (Stable)

**Purpose:** Allow the UI repo (app.blockidscore.fun) to connect safely without frequent backend changes.

**Stack:** FastAPI · DB: `trust_scores`, `wallet_reasons`, `wallet_history`, `wallet_clusters`, `transactions`

---

## Base URL

| Environment | Base URL |
|-------------|----------|
| Local      | `http://localhost:8000/api/v1` |
| Production | `https://api.blockidscore.fun/api/v1` |

---

## 1️⃣ Wallet Profile

**Endpoint:** `GET /wallet/{wallet}`

**Use case:** UI main wallet page.

### Response

| Field        | Type   | Description                    |
|--------------|--------|--------------------------------|
| `wallet`     | string | Wallet address                 |
| `score`      | number | Trust score 0–100              |
| `risk`       | number | 0=Low, 1=Medium, 2=High, 3=Critical |
| `badge`      | string | Risk badge label               |
| `confidence` | number | Model confidence 0–1           |
| `updated_at` | string | ISO date or timestamp          |

### Example

```json
{
  "wallet": "8X35rQ...",
  "score": 10,
  "risk": 3,
  "badge": "SCAM_SUSPECTED",
  "confidence": 0.82,
  "updated_at": "2026-02-25"
}
```

---

## 2️⃣ Reason Codes

**Endpoint:** `GET /wallet/{wallet}/reasons`

**Use case:** "Why flagged?" section.

### Response

Array of objects:

| Field       | Type   | Description                  |
|-------------|--------|------------------------------|
| `code`      | string | Reason code (e.g. DRAINER_INTERACTION) |
| `weight`    | number | Weight/contribution          |
| `confidence`| number | Confidence for this reason   |
| `tx_hash`   | string | Transaction signature (evidence) |
| `solscan`   | string | Solscan URL for tx           |

### Example

```json
[
  {
    "code": "SCAM_CLUSTER_MEMBER",
    "weight": 0.72,
    "confidence": 0.85,
    "tx_hash": "5abc...",
    "solscan": "https://solscan.io/tx/5abc..."
  },
  {
    "code": "DRAINER_INTERACTION",
    "weight": 0.65,
    "confidence": 0.78,
    "tx_hash": "7def...",
    "solscan": "https://solscan.io/tx/7def..."
  }
]
```

---

## 3️⃣ Badge Timeline

**Endpoint:** `GET /wallet/{wallet}/badge_timeline`

**Use case:** Reputation timeline chart.

### Response

Array of objects:

| Field  | Type   | Description         |
|--------|--------|---------------------|
| `date` | string | ISO date            |
| `badge`| string | Badge at that date  |
| `score`| number | Score at that date  |

### Example

```json
[
  {"date": "2026-02-20", "badge": "LOW_RISK", "score": 75},
  {"date": "2026-02-23", "badge": "MEDIUM_RISK", "score": 45},
  {"date": "2026-02-25", "badge": "SCAM_SUSPECTED", "score": 10}
]
```

---

## 4️⃣ Investigation Badge Panel

**Endpoint:** `GET /wallet/{wallet}/investigation_badge`

**Use case:** Investigator UI card.

### Response

| Field                | Type   | Description                |
|----------------------|--------|----------------------------|
| `score`              | number | Current trust score        |
| `badge`              | string | Current badge              |
| `top_reason_codes`   | array  | Top reason codes           |
| `cluster_size`       | number | Wallet cluster size        |
| `scam_ratio`         | number | Scam ratio in cluster      |
| `suspicious_tx_count`| number | Suspicious tx count        |
| `score_change_30d`   | number | Score delta over 30 days   |

### Example

```json
{
  "score": 15,
  "badge": "SCAM_SUSPECTED",
  "top_reason_codes": ["SCAM_CLUSTER_MEMBER", "DRAINER_INTERACTION"],
  "cluster_size": 42,
  "scam_ratio": 0.85,
  "suspicious_tx_count": 12,
  "score_change_30d": -65
}
```

---

## 5️⃣ Graph Panel

**Endpoint:** `GET /wallet/{wallet}/graph`

**Use case:** Network visualization.

### Query Parameters

| Param  | Type | Default | Description     |
|--------|------|---------|-----------------|
| `depth`| int  | 2       | Graph depth 1–5 |
| `mode` | string | `all` | `all` or `scam_only` |
| `max_nodes` | int | 200 | Max nodes to return |

### Response

| Field  | Type  | Description                    |
|--------|-------|--------------------------------|
| `nodes`| array | Graph nodes                    |
| `edges`| array | Graph edges                    |

**Node fields:** `id`, `badge`, `risk`, `cluster_id`

**Edge fields:** `source`, `target`, `amount`

### Example

```json
{
  "nodes": [
    {"id": "8X35rQ...", "badge": "SCAM_SUSPECTED", "risk": 3, "cluster_id": "c1"},
    {"id": "7abc...", "badge": "LOW_RISK", "risk": 0, "cluster_id": "c1"}
  ],
  "edges": [
    {"source": "8X35rQ...", "target": "7abc...", "amount": 1.5}
  ]
}
```

---

## 6️⃣ Wallet Report

**Endpoint:** `GET /wallet/{wallet}/report`

**Use case:** "Download Investigation Report" button.

### Response

- **Option A:** Redirect or `Content-Type: application/pdf` (binary PDF)
- **Option B:** JSON with URL:

```json
{
  "pdf_url": "https://api.blockidscore.fun/api/v1/wallet/8X35rQ.../report/download"
}
```

---

## 7️⃣ Batch Wallet Check

**Endpoint:** `POST /wallet/batch_check`

**Use case:** Exchange UI or bulk wallet check.

### Request

```json
["wallet1", "wallet2", "wallet3"]
```

### Response

```json
[
  {"wallet": "wallet1", "score": 75, "badge": "LOW_RISK"},
  {"wallet": "wallet2", "score": 15, "badge": "SCAM_SUSPECTED"}
]
```

Wallets not found: `{"wallet": "...", "score": null, "badge": "UNKNOWN"}`

---

## 8️⃣ Monitoring (Admin UI)

**Base path:** `GET /monitor/*`

**Use case:** Admin dashboard.

| Endpoint | Description |
|----------|-------------|
| `GET /monitor/health` | Health check (DB, Helius, pipeline) |
| `GET /monitor/pipeline_status` | Last pipeline run, success/failure |
| `GET /monitor/trust_stats` | Trust score stats (counts, distribution) |
| `GET /monitor/helius_usage` | Helius API usage and cost |
| `GET /monitor/review_queue` | Pending review queue size |
| `GET /monitor/alerts` | Active alerts |

---

## 9️⃣ Error Format

All errors return JSON:

```json
{
  "error": "wallet_not_found",
  "detail": "No trust score found for wallet 8X35rQ..."
}
```

| Code | `error` examples     | Description         |
|------|----------------------|---------------------|
| 400  | `invalid_wallet`     | Bad request         |
| 404  | `wallet_not_found`   | Wallet not in DB    |
| 429  | `rate_limit`         | Too many requests   |
| 500  | `internal_error`     | Server error        |

---

## 🔟 Versioning

Always use path prefix:

```
/api/v1/
```

Future breaking changes → new version:

```
/api/v2/
```

**Stability:** Fields added to existing responses are backward-compatible. Fields removed or renamed require a new version.

---

## Quick Reference

| Endpoint | Method | Use Case |
|----------|--------|----------|
| `/wallet/{wallet}` | GET | Main wallet page |
| `/wallet/{wallet}/reasons` | GET | Why flagged? |
| `/wallet/{wallet}/badge_timeline` | GET | Timeline chart |
| `/wallet/{wallet}/investigation_badge` | GET | Investigator card |
| `/wallet/{wallet}/graph` | GET | Network viz |
| `/wallet/{wallet}/report` | GET | PDF report |
| `/wallet/batch_check` | POST | Bulk check |
| `/monitor/health` | GET | Admin health |
| `/monitor/pipeline_status` | GET | Admin pipeline |
| `/monitor/trust_stats` | GET | Admin stats |
| `POST /transaction/check` | POST | Transaction risk (Phantom plugin) |
