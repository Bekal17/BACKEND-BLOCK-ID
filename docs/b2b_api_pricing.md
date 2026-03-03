# BlockID B2B API Pricing Model

**Purpose:** Generate sustainable revenue while encouraging adoption for exchanges, marketplaces, and Web3 platforms.

**Endpoints:** Wallet check, batch check, webhook alerts, investigation report

---

## SECTION 1 — Pricing Philosophy

BlockID pricing must be:

- **Cheap enough for startups** — low barrier to entry
- **Scalable for big exchanges** — volume-based growth
- **Predictable monthly cost** — no surprise bills

**Model:** Charge based on **API usage** + **features**.

---

## SECTION 2 — Pricing Tiers

### Starter — Free

| Item | Limit |
|------|-------|
| Wallet checks | 10,000/month |
| Batch endpoint | No |
| Webhook alerts | No |
| Support | Community (Discord/GitHub) |

**Use case:** Small projects, MVPs, dev testing

---

### Growth — $99/month

| Item | Limit |
|------|-------|
| Wallet checks | 500,000/month |
| Batch check | Enabled |
| Webhook alerts | Basic |
| Investigation report | Limited (e.g. 100/month) |
| Support | Email |

**Use case:** Marketplaces, DEXs, mid-size platforms

---

### Enterprise — Custom

| Item | Limit |
|------|-------|
| Wallet checks | Unlimited |
| Batch check | Unlimited |
| Webhook alerts | Real-time, high throughput |
| Dedicated RPC node | Optional |
| Investigation report API | Full access |
| SLA | 99.9% uptime |
| Support | Priority, dedicated CSM |

**Use case:** Exchanges, large Web3 platforms, compliance teams

---

## SECTION 3 — Pay-as-you-go Option

- **$0.0005 per wallet check** (over quota)
- Good for burst usage
- Applied when monthly quota exceeded
- Capped at Growth-tier max (e.g. $500/month overage cap) or uncapped for Enterprise

---

## SECTION 4 — Extra Revenue Features

| Feature | Price | Notes |
|---------|-------|-------|
| PDF investigation reports | $0.05/report or bundled | Compliance teams |
| Real-time webhook alerts | +$29/mo or per alert | Low-latency notifications |
| Cross-chain checks | +$49/mo or per chain | EVM, etc. |
| Custom risk scoring | Enterprise quote | Thresholds, rules, ML tweaks |

---

## SECTION 5 — Internal Cost Consideration

Costs include:

- Helius API
- RPC nodes
- Server infra (K8s, DB)
- Monitoring (Grafana, Prometheus)

**Target:** Pricing must cover **5× infra cost** for sustainable margin.

---

## SECTION 6 — Abuse Protection

| Measure | Implementation |
|---------|----------------|
| Rate limit | Per API key: req/min, req/day |
| Scraping | Block bulk sequential checks; require batch endpoint |
| Key rotation | Support revocation, multiple keys per org |
| Quota enforcement | Soft limit (throttle) then hard limit |

---

## SECTION 7 — Billing Integration

**Payment:** Stripe subscription API

**Usage tracking (DB):**

```sql
CREATE TABLE api_usage (
  id INTEGER PRIMARY KEY,
  api_key TEXT NOT NULL,
  period_start TEXT NOT NULL,   -- e.g. 2025-02-01
  period_end TEXT NOT NULL,
  wallet_checks INTEGER DEFAULT 0,
  batch_checks INTEGER DEFAULT 0,
  reports_generated INTEGER DEFAULT 0,
  webhook_events INTEGER DEFAULT 0,
  overage_charges REAL DEFAULT 0,
  created_at TEXT,
  updated_at TEXT
);
```

**Flow:**
1. On each API call, increment counters (or use Redis for real-time, sync to DB periodically)
2. Stripe metered billing or invoice at month-end
3. Dashboard: usage vs quota

---

## SECTION 8 — Future Pricing Ideas

| Idea | Description |
|------|-------------|
| Trust Score NFT badge | One-time or subscription for verified badge NFT |
| Compliance subscription | Quarterly compliance report, audit trail export |
| Risk analytics dashboard | Premium Grafana-style dashboards, custom metrics |
| Insurance partnership | Revenue share for wallet risk–based insurance products |

---

## Quick Reference

| Tier | Price | Checks/mo | Batch | Webhook | Report |
|------|-------|-----------|-------|---------|--------|
| Starter | Free | 10K | No | No | No |
| Growth | $99/mo | 500K | Yes | Basic | Limited |
| Enterprise | Custom | ∞ | Yes | Real-time | Full |
| Pay-as-you-go | $0.0005/check | Over quota | — | — | — |
