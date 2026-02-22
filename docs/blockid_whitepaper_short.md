# BlockID Whitepaper — Executive Summary

*Max 5 pages | Problem · Solution · Market · Revenue*

---

## Problem

### Trust Vacuum in Web3

Solana and other L1s suffer from a fundamental trust gap: **wallet addresses are opaque**. A new address carries no reputation. Scammers exploit this by creating fresh wallets for each campaign—phishing, token rug pulls, NFT scams, drainer attacks—making traditional blocklists ineffective.

### Scale of Abuse

- **Scam wallets** — Thousands of addresses execute coordinated fraud; victims lose funds because counterparties cannot be vetted.
- **Anonymity abuse** — One entity controls dozens of wallets, funded from a common source, appearing as unrelated users.
- **Sybil attacks** — Fake identities inflate holder counts, game airdrops, or pose as trusted counterparties.
- **Fake identity** — NFT creators, DeFi LPs, and bots present single addresses while operating many wallets for obfuscation, wash trading, or front-running.

### Why Now

Solana’s high throughput and low fees accelerate scam velocity. Drainer kits, malicious token mints, and approval-based theft are common. Applications lack shared trust infrastructure—each builds (or ignores) reputation in isolation.

---

## Solution

### BlockID: Trust Infrastructure for Web3

BlockID is a **behavioral trust oracle** that scores Solana wallets based on on-chain behavior. Scores are stored on-chain in Program Derived Addresses (PDAs), so any application can read them without centralized APIs.

### Core Insight: Wallet ≠ Identity

A wallet is not an identity. BlockID groups wallets into **identity clusters** using behavioral signals: bidirectional transfers, shared funding sources, burst timing, and flow structure. Trust is computed at the cluster level, then propagated to wallets—resisting Sybil attacks and fake identity.

### How It Works

1. **Data** — Transaction graphs and flow data from Helius RPC (public on-chain).
2. **Features** — Graph clustering (distance to known scams), flow analysis (rapid tx, unique destinations), drainer heuristics (multi-victim patterns, approval-like interactions).
3. **Scoring** — ML model predicts scam probability; trust score = `(1 − prob) × 100` (0–100).
4. **On-chain** — Scores published to Solana PDAs; transparent, composable, censorship-resistant.

### Differentiators

- **On-chain scores** — No single server can revoke access; DeFi and NFT contracts can gate on trust.
- **Explainable** — Reason codes (e.g. NEAR_SCAM_CLUSTER, HIGH_RAPID_TX) for each score.
- **Anti-Sybil** — Cluster signals detect coordinated wallet creation and shared funding.
- **Privacy-preserving** — No KYC; only public blockchain data.

---

## Market

### Target Segments

| Segment | Use Case | Pain Point |
|---------|----------|------------|
| **Wallet apps** | Trust badge before sending; warnings on low-score counterparties | Users send to scam addresses; no reputation layer |
| **NFT marketplaces** | Filter or flag listings from high-risk creators | Fake collections, rug pulls, phishing mints |
| **DeFi platforms** | Restrict or warn on risky counterparties | Sybil farming, wash trading, malicious LPs |
| **Agent / bot reputation** | Score AI-controlled wallets for trading, lending | No standard for agent trust; black boxes |
| **Enterprises** | Custom dashboards, risk reports, bulk exports | Need audit trail and compliance visibility |

### Market Size (Directional)

- **Solana** — Millions of active wallets; growing DeFi TVL, NFT volume, and agent activity.
- **Trust tooling** — Incumbent solutions are off-chain, centralized, or project-specific. No shared on-chain oracle for behavioral trust.
- **Regulatory tailwind** — Scam reduction and transparency align with consumer protection and compliance trends.

### Competitive Moat

- **First-mover on-chain** — PDA-stored trust scores are composable by any Solana program.
- **Behavioral + cluster model** — Distinguishes from simple blocklists and single-wallet scoring.
- **Explainability** — Reason codes and propagation logic support compliance and dispute resolution.

---

## Revenue

### Revenue Streams

| Stream | Model | Target |
|--------|-------|--------|
| **API subscription** | Tiered: free low-volume; paid for high-volume or batch | Developers, wallets, marketplaces |
| **Enterprise analytics** | Custom dashboards, risk reports, bulk exports, SLA | Exchanges, funds, compliance teams |
| **Marketplace integration** | Revenue share or per-integration fee | NFT platforms, DeFi protocols |
| **Risk scoring API** | Per-call or per-wallet pricing | High-frequency traders, agent systems |

### Unit Economics (Conceptual)

- **Cost** — RPC (Helius), compute, oracle key management, support.
- **Value** — Fraud reduction, user trust, compliance, reduced support and chargebacks.
- **Pricing levers** — Volume tiers, latency guarantees, reason-code depth, historical access.

### Path to Revenue

1. **Free tier** — Drive adoption; establish BlockID as default trust layer.
2. **Paid API** — Beyond threshold; batch and real-time endpoints.
3. **Enterprise** — Custom contracts, SLAs, dedicated support.
4. **Protocol integration** — Revenue share with protocols that gate on BlockID scores.

---

## Conclusion

BlockID provides **trust infrastructure** for Web3: on-chain, behavioral, explainable scores that reduce scams and Sybil risk without centralizing identity. The problem is acute, the solution is differentiated, the market spans wallets to enterprises, and revenue can scale through API, enterprise, and protocol partnerships.

---

*For full technical detail, see `blockid_whitepaper.md`.*
